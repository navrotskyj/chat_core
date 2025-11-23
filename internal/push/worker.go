package push

import (
	"context"
	"encoding/json"
	"log"

	"chat_core/internal/broker"
	"chat_core/internal/domain"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Worker struct {
	broker *broker.RabbitMQClient
}

func NewWorker(broker *broker.RabbitMQClient) *Worker {
	return &Worker{
		broker: broker,
	}
}

func (w *Worker) Start(ctx context.Context) {
	msgs, err := w.broker.ConsumePushQueue()
	if err != nil {
		log.Printf("Failed to start push consumer: %v", err)
		return
	}

	go func() {
		for d := range msgs {
			// This message came from DLX, meaning it expired in the user queue.
			// The user is offline!

			var event struct {
				Type    string          `json:"type"`
				Payload json.RawMessage `json:"payload"`
			}
			if err := json.Unmarshal(d.Body, &event); err != nil {
				log.Printf("Failed to unmarshal event: %v", err)
				d.Ack(false)
				continue
			}

			if event.Type == domain.EventTypeMessageCreated {
				var msg domain.Message
				if err := json.Unmarshal(event.Payload, &msg); err != nil {
					log.Printf("Failed to unmarshal message payload: %v", err)
					d.Ack(false)
					continue
				}

				// The routing key of the dead-lettered message tells us the user!
				// We check the "x-death" header which contains detailed info about the dead-lettering.
				// Although d.RoutingKey usually preserves the original key, x-death is the source of truth.

				targetUser := d.RoutingKey

				if headers, ok := d.Headers["x-death"].([]interface{}); ok && len(headers) > 0 {
					if table, ok := headers[0].(amqp.Table); ok {
						if rk, ok := table["routing-keys"].([]interface{}); ok && len(rk) > 0 {
							if key, ok := rk[0].(string); ok {
								targetUser = key
							}
						}
					}
				}

				log.Printf("[PUSH] Sending push to %s: %s (Expired in queue)", targetUser, msg.Content)
			}
			d.Ack(false)
		}
	}()

	<-ctx.Done()
}
