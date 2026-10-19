CREATE TABLE IF NOT EXISTS processed_messages (
  message_id TEXT NOT NULL,
  consumer_group TEXT NOT NULL,
  topic TEXT NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT pk_processed_messages PRIMARY KEY (message_id, consumer_group)
);

CREATE INDEX IF NOT EXISTS idx_processed_messages_topic ON processed_messages (topic);
