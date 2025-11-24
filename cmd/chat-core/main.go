package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"chat_core/internal/broker"
	"chat_core/internal/domain"
	"chat_core/internal/outbox"
	"chat_core/internal/presence"
	"chat_core/internal/push"
	"chat_core/internal/repository"
	"chat_core/internal/ws"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for demo
	},
}

func main() {
	// 1. Configuration
	dbConnStr := os.Getenv("DB_CONN_STR")
	if dbConnStr == "" {
		dbConnStr = "postgres://postgres:postgres@localhost:5432/chat_core?sslmode=disable"
	}
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@localhost:5672/"
	}

	// 2. Database
	db, err := sql.Open("postgres", dbConnStr)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer db.Close()

	// 3. Repositories
	// 3. RabbitMQ
	mqClient, err := broker.NewRabbitMQClient(amqpURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer mqClient.Close()

	// 3. Declare Stream
	streamName := "chat_events"
	if err := mqClient.DeclareStream(streamName); err != nil {
		log.Fatalf("Failed to declare stream: %v", err)
	}

	// 4. Repositories
	// We use RabbitMQ Stream as Outbox
	outboxRepo, err := repository.NewRabbitMQStreamOutboxRepository(mqClient, streamName)
	if err != nil {
		log.Fatalf("Failed to create outbox repo: %v", err)
	}
	// We still need PostgresOutboxRepository for manual event creation if needed?
	// Or we just use the stream one everywhere.
	// But wait, ChatRepository needs it.

	chatRepo := repository.NewChatRepository(db, outboxRepo)
	presenceRepo := presence.NewPostgresRepository(db)

	// 5. Stream Consumer (Replaces Outbox Worker)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamConsumer := outbox.NewStreamConsumer(mqClient, chatRepo, presenceRepo, streamName)
	go func() {
		if err := streamConsumer.Start(ctx); err != nil {
			log.Printf("Stream consumer failed: %v", err)
		}
	}()

	// 6. WebSocket Hub
	nodeID := uuid.New().String()
	hub := ws.NewHub(presenceRepo, mqClient, nodeID)
	go hub.Run()

	// 7. Push Worker
	pushWorker := push.NewWorker(mqClient)
	go pushWorker.Start(ctx)

	// 8. Cache Invalidation Consumer
	invalidationMsgs, err := mqClient.ConsumeBroadcast("cache.invalidate")
	if err != nil {
		log.Fatalf("Failed to start invalidation consumer: %v", err)
	}
	go func() {
		for d := range invalidationMsgs {
			var payload map[string]string
			if err := json.Unmarshal(d.Body, &payload); err != nil {
				log.Printf("Failed to unmarshal invalidation payload: %v", err)
				continue
			}
			if chatIDStr, ok := payload["chat_id"]; ok {
				if chatID, err := uuid.Parse(chatIDStr); err == nil {
					chatRepo.Invalidate(chatID)
					log.Printf("Cache invalidated for chat %s", chatID)
				}
			}
		}
	}()

	// 9. HTTP Handlers
	// 8. HTTP Handlers
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		// WebSocket handshake doesn't strictly need CORS headers if CheckOrigin is true,
		// but it's good practice to be consistent if we were doing auth headers etc.
		// However, for WS, we just proceed.
		userIDStr := r.URL.Query().Get("user_id")
		deviceIDStr := r.URL.Query().Get("device_id")
		if userIDStr == "" || deviceIDStr == "" {
			http.Error(w, "Missing user_id or device_id", http.StatusBadRequest)
			return
		}

		userID, _ := uuid.Parse(userIDStr)
		deviceID, _ := uuid.Parse(deviceIDStr)

		// Auto-create user and device
		if err := chatRepo.EnsureUser(r.Context(), userID); err != nil {
			log.Printf("Failed to ensure user: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		if err := chatRepo.EnsureDevice(r.Context(), deviceID, userID); err != nil {
			log.Printf("Failed to ensure device: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("Failed to upgrade WS: %v", err)
			return
		}

		client := &ws.Client{
			Hub:      hub,
			Conn:     conn,
			UserID:   userID,
			DeviceID: deviceID,
			Send:     make(chan []byte, 256),
		}
		hub.Register <- client

		go client.WritePump()
		go client.ReadPump()
	})

	http.HandleFunc("/messages/sync", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		chatIDStr := r.URL.Query().Get("chat_id")
		afterIDStr := r.URL.Query().Get("after_id")

		if chatIDStr == "" {
			http.Error(w, "Missing chat_id", http.StatusBadRequest)
			return
		}

		chatID, err := uuid.Parse(chatIDStr)
		if err != nil {
			http.Error(w, "Invalid chat_id", http.StatusBadRequest)
			return
		}

		var afterID uuid.UUID
		if afterIDStr != "" {
			afterID, _ = uuid.Parse(afterIDStr)
		}

		messages, err := chatRepo.GetMessagesAfter(r.Context(), chatID, afterID, 50)
		if err != nil {
			log.Printf("Failed to sync messages: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		if messages == nil {
			messages = []*domain.Message{}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(messages)
	}))

	http.HandleFunc("/chats/members", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		chatIDStr := r.URL.Query().Get("chat_id")
		if chatIDStr == "" {
			http.Error(w, "Missing chat_id", http.StatusBadRequest)
			return
		}

		chatID, err := uuid.Parse(chatIDStr)
		if err != nil {
			http.Error(w, "Invalid chat_id", http.StatusBadRequest)
			return
		}

		members, err := chatRepo.GetChatMembersDetails(r.Context(), chatID)
		if err != nil {
			log.Printf("Failed to get members: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(members)
	}))

	http.HandleFunc("/messages/read", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			ChatID        uuid.UUID `json:"chat_id"`
			UserID        uuid.UUID `json:"user_id"`
			LastMessageID uuid.UUID `json:"last_message_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		if err := chatRepo.MarkMessagesRead(r.Context(), req.ChatID, req.UserID, req.LastMessageID); err != nil {
			log.Printf("Failed to mark messages read: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}))

	http.HandleFunc("/messages/delivered", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			ChatID        uuid.UUID `json:"chat_id"`
			UserID        uuid.UUID `json:"user_id"`
			LastMessageID uuid.UUID `json:"last_message_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		if err := chatRepo.MarkMessagesDelivered(r.Context(), req.ChatID, req.UserID, req.LastMessageID); err != nil {
			log.Printf("Failed to mark messages delivered: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}))

	http.HandleFunc("/messages", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var msg domain.Message
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			http.Error(w, "Invalid body", http.StatusBadRequest)
			return
		}

		// Assign IDs if missing
		if msg.ID == uuid.Nil {
			msg.ID = uuid.New()
		}
		if msg.CreatedAt.IsZero() {
			msg.CreatedAt = time.Now()
		}

		// Auto-create chat and sender
		if err := chatRepo.EnsureUser(r.Context(), msg.SenderID); err != nil {
			log.Printf("Failed to ensure sender: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		if err := chatRepo.EnsureChat(r.Context(), msg.ChatID); err != nil {
			log.Printf("Failed to ensure chat: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		added, err := chatRepo.EnsureChatMember(r.Context(), msg.ChatID, msg.SenderID)
		if err != nil {
			log.Printf("Failed to ensure member: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// If user was newly added, publish USER_JOINED event
		if added {
			// Invalidate cache first
			go func() {
				invalidationPayload := map[string]string{"chat_id": msg.ChatID.String()}
				if err := mqClient.Publish(context.Background(), "cache.invalidate", invalidationPayload); err != nil {
					log.Printf("Failed to publish cache invalidation: %v", err)
				}
			}()

			// Publish USER_JOINED
			go func() {
				// We reuse the Outbox logic? Or direct publish?
				// Ideally via Outbox for reliability.
				// But we don't have a handy "CreateEvent" method exposed in main.
				// Let's just create an outbox event manually using outboxRepo.
				// Wait, outboxRepo is available here.

				payload, _ := json.Marshal(map[string]interface{}{
					"chat_id": msg.ChatID,
					"user_id": msg.SenderID,
				})

				event := &domain.OutboxEvent{

					ID:        uuid.New(),
					EventType: domain.EventTypeUserJoined,
					Payload:   payload,
					CreatedAt: time.Now(),
				}

				// We need a transaction? Ideally yes, but for now separate is okay-ish.
				// Actually, we can't easily use the same tx as EnsureChatMember because that method manages its own tx (implicit).
				// So we just save it.
				if err := outboxRepo.Save(context.Background(), nil, event); err != nil {
					log.Printf("Failed to save USER_JOINED event: %v", err)
				}
			}()
		}

		if err := chatRepo.CreateMessage(r.Context(), &msg); err != nil {
			log.Printf("Failed to create message: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(msg)
	}))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Server starting on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

func enableCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next(w, r)
	}
}
