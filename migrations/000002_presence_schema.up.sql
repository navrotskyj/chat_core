CREATE TABLE active_sessions (
    user_id UUID NOT NULL,
    device_id UUID NOT NULL,
    node_id VARCHAR(255) NOT NULL,
    connected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (user_id, device_id)
);

CREATE INDEX idx_active_sessions_user_id ON active_sessions(user_id);
