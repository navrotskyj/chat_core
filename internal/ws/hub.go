package ws

import (
	"chat_core/internal/broker"
	"chat_core/internal/presence"
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Client struct {
	Hub      *Hub
	Conn     *websocket.Conn
	UserID   uuid.UUID
	DeviceID uuid.UUID
	Send     chan []byte
}

type Hub struct {
	// Registered clients: UserID -> DeviceID -> Client
	clients map[uuid.UUID]map[uuid.UUID]*Client

	Register   chan *Client
	Unregister chan *Client

	presenceRepo presence.Repository
	broker       *broker.RabbitMQClient
	nodeID       string

	// Map to store cancel functions for consumers: UserID -> CancelFunc
	consumers map[uuid.UUID]func()

	mu sync.RWMutex
}

func NewHub(presenceRepo presence.Repository, broker *broker.RabbitMQClient, nodeID string) *Hub {
	return &Hub{
		clients:      make(map[uuid.UUID]map[uuid.UUID]*Client),
		Register:     make(chan *Client),
		Unregister:   make(chan *Client),
		presenceRepo: presenceRepo,
		broker:       broker,
		nodeID:       nodeID,
		consumers:    make(map[uuid.UUID]func()),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register:
			h.mu.Lock()
			if _, ok := h.clients[client.UserID]; !ok {
				h.clients[client.UserID] = make(map[uuid.UUID]*Client)

				// Start consuming user queue if first device
				msgs, cancel, err := h.broker.ConsumeUserQueue(client.UserID.String())
				if err != nil {
					log.Printf("Failed to consume user queue: %v", err)
				} else {
					h.consumers[client.UserID] = cancel
					go h.handleUserMessages(client.UserID, msgs)
				}
			}
			h.clients[client.UserID][client.DeviceID] = client
			h.mu.Unlock()

			// Track session in DB (Still useful for other things, but not strictly needed for push anymore)
			go func() {
				if err := h.presenceRepo.AddSession(context.Background(), client.UserID, client.DeviceID, h.nodeID); err != nil {
					log.Printf("Failed to add session: %v", err)
				}
			}()

			log.Printf("Client registered: User=%s Device=%s", client.UserID, client.DeviceID)

		case client := <-h.Unregister:
			h.mu.Lock()
			if userClients, ok := h.clients[client.UserID]; ok {
				if _, ok := userClients[client.DeviceID]; ok {
					delete(userClients, client.DeviceID)
					close(client.Send)
					if len(userClients) == 0 {
						delete(h.clients, client.UserID)

						// Stop consumer
						if cancel, ok := h.consumers[client.UserID]; ok {
							cancel()
							delete(h.consumers, client.UserID)
						}
					}
				}
			}
			h.mu.Unlock()

			// Remove session from DB
			go func() {
				if err := h.presenceRepo.RemoveSession(context.Background(), client.UserID, client.DeviceID); err != nil {
					log.Printf("Failed to remove session: %v", err)
				}
			}()

			log.Printf("Client unregistered: User=%s Device=%s", client.UserID, client.DeviceID)
		}
	}
}

func (h *Hub) handleUserMessages(userID uuid.UUID, msgs <-chan amqp.Delivery) {
	for d := range msgs {
		var event struct {
			Type    string          `json:"type"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(d.Body, &event); err != nil {
			log.Printf("Failed to unmarshal event: %v", err)
			continue
		}

		if event.Type == "MESSAGE_CREATED" { // Hardcoded for now or import domain
			// Forward to all devices of this user
			h.BroadcastToUser(userID, event.Payload) // Payload is the message
		}
	}
}

func (h *Hub) BroadcastToUser(userID uuid.UUID, message interface{}) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	userClients, ok := h.clients[userID]
	if !ok {
		return
	}

	data, err := json.Marshal(message)
	if err != nil {
		log.Printf("Failed to marshal message: %v", err)
		return
	}

	for _, client := range userClients {
		select {
		case client.Send <- data:
		default:
			close(client.Send)
			delete(userClients, client.DeviceID)
		}
	}
}

func (h *Hub) BroadcastToChat(members []uuid.UUID, message interface{}) {
	for _, userID := range members {
		h.BroadcastToUser(userID, message)
	}
}
