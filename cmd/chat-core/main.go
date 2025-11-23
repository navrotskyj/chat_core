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
	outboxRepo := repository.NewPostgresOutboxRepository(db)
	chatRepo := repository.NewChatRepository(db)
	presenceRepo := presence.NewPostgresRepository(db)

	// 4. RabbitMQ
	mqClient, err := broker.NewRabbitMQClient(amqpURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer mqClient.Close()

	// 5. Outbox Worker
	worker := outbox.NewWorker(outboxRepo, chatRepo, mqClient)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go worker.Start(ctx, 500*time.Millisecond)

	// 6. WebSocket Hub
	nodeID := uuid.New().String()
	hub := ws.NewHub(presenceRepo, mqClient, nodeID)
	go hub.Run()

	// 7. Push Worker
	pushWorker := push.NewWorker(mqClient)
	go pushWorker.Start(ctx)

	// 8. Cluster Broadcast (Consumer) - REMOVED
	// The Hub now consumes user queues directly via mqClient.ConsumeUserQueue

	// 8. HTTP Handlers
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
		if err := chatRepo.EnsureChatMember(r.Context(), msg.ChatID, msg.SenderID); err != nil {
			log.Printf("Failed to ensure member: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
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
