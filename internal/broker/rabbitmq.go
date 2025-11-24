package broker

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ExchangeTopic = "chat.topic"
	ExchangePush  = "chat.push"
)

type RabbitMQClient struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewRabbitMQClient(url string) (*RabbitMQClient, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// 1. Declare Topic Exchange for Chat Events
	err = ch.ExchangeDeclare(
		ExchangeTopic, // name
		"topic",       // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare topic exchange: %w", err)
	}

	// 2. Declare Push Exchange (DLX)
	err = ch.ExchangeDeclare(
		ExchangePush, // name
		"fanout",     // type (or direct if we want to partition push workers)
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare push exchange: %w", err)
	}

	return &RabbitMQClient{
		conn:    conn,
		channel: ch,
	}, nil
}

func (c *RabbitMQClient) Publish(ctx context.Context, routingKey string, body interface{}) error {
	return c.PublishToExchange(ctx, ExchangeTopic, routingKey, body)
}

func (c *RabbitMQClient) PublishToExchange(ctx context.Context, exchange, routingKey string, body interface{}) error {
	bytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal body: %w", err)
	}

	return c.channel.PublishWithContext(ctx,
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        bytes,
		},
	)
}

func (c *RabbitMQClient) Close() {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}

// ConsumeUserQueue creates a queue for the user with TTL/DLX and consumes it.
// Returns a channel of messages and a cancel function to stop consuming (delete queue).
func (c *RabbitMQClient) ConsumeUserQueue(userID string) (<-chan amqp.Delivery, func(), error) {
	queueName := fmt.Sprintf("user.%s", userID)

	args := amqp.Table{
		"x-message-ttl":          int32(5000),  // 5 seconds TTL
		"x-dead-letter-exchange": ExchangePush, // Send to Push Exchange on expiry
		//"x-dead-letter-routing-key": "",             // Optional: keep original routing key
		"x-expires": int32(60000), // Delete queue if unused for 60s (cleanup)
	}

	q, err := c.channel.QueueDeclare(
		queueName, // name
		false,     // durable (transient for active connection)
		false,     // delete when unused (we use x-expires instead to allow brief disconnects)
		false,     // exclusive (no, because we might have multiple tabs? actually yes, one connection per tab?
		// If multiple tabs, we want competing consumers or fanout?
		// For this architecture, let's assume one main queue per user.
		// If multiple tabs, they compete. Only one gets it.
		// To support multiple tabs, we'd need user.device queues.
		// Let's stick to user queue for simplicity as per plan.
		// If exclusive=true, only one connection can consume.
		// Let's use exclusive=false.)
		false, // no-wait
		args,  // arguments
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to declare user queue: %w", err)
	}

	err = c.channel.QueueBind(
		q.Name,                         // queue name
		fmt.Sprintf("user.%s", userID), // routing key
		ExchangeTopic,                  // exchange
		false,
		nil,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to bind user queue: %w", err)
	}

	consumerTag := fmt.Sprintf("consumer-%s", userID)
	msgs, err := c.channel.Consume(
		q.Name,      // queue
		consumerTag, // consumer tag (empty = auto-generated)
		true,        // auto-ack (we ack immediately as we forward to WS)
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to register consumer: %w", err)
	}

	cancel := func() {
		// We could delete the queue or just cancel consumer.
		// With x-expires, it will clean itself up.
		// We just need to stop consuming?
		// Actually, if we stop consuming, the messages will sit there until TTL and then DLX.
		// That's exactly what we want!
		// But we need the consumer tag to cancel.
		// The amqp library doesn't return the tag if we pass empty.
		// We should probably pass a tag or just close the channel (bad).
		// For now, we rely on the channel closing or connection dropping?
		// No, we need to cancel explicitly if user disconnects but server stays up.
		// Refactor to return consumer tag or use a specific one.
		c.channel.Cancel(consumerTag, false)
	}

	return msgs, cancel, nil
}

// ConsumePushQueue consumes from the DLX exchange
func (c *RabbitMQClient) ConsumePushQueue() (<-chan amqp.Delivery, error) {
	q, err := c.channel.QueueDeclare(
		"push_notifications_dlx", // name
		true,                     // durable
		false,                    // delete when unused
		false,                    // exclusive
		false,                    // no-wait
		nil,                      // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare push queue: %w", err)
	}

	err = c.channel.QueueBind(
		q.Name,       // queue name
		"#",          // routing key (catch all from fanout/topic)
		ExchangePush, // exchange
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to bind push queue: %w", err)
	}

	return c.channel.Consume(
		q.Name, "", false, false, false, false, nil,
	)
}

// ConsumeBroadcast creates a temporary exclusive queue bound to the exchange with a specific routing key.
// This is used for broadcasting events to ALL nodes (e.g. cache invalidation).
func (c *RabbitMQClient) ConsumeBroadcast(routingKey string) (<-chan amqp.Delivery, error) {
	q, err := c.channel.QueueDeclare(
		"",    // name (empty = random auto-generated)
		false, // durable
		true,  // delete when unused
		true,  // exclusive (only this connection can read)
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare broadcast queue: %w", err)
	}

	err = c.channel.QueueBind(
		q.Name,        // queue name
		routingKey,    // routing key
		ExchangeTopic, // exchange
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to bind broadcast queue: %w", err)
	}

	return c.channel.Consume(
		q.Name, "", true, false, false, false, nil,
	)
}
