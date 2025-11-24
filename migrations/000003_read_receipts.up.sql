ALTER TABLE messages ADD COLUMN status VARCHAR(20) DEFAULT 'sent';

CREATE TABLE message_receipts (
    user_id UUID NOT NULL,
    message_id UUID NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'delivered', 'read'
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (user_id, message_id)
);
