CREATE TABLE IF NOT EXISTS saga_log (
  id TEXT PRIMARY KEY,
  booking_id TEXT NOT NULL REFERENCES bookings(id) ON DELETE CASCADE,
  state TEXT NOT NULL,
  last_event TEXT NOT NULL,
  payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_saga_log_booking_id
  ON saga_log (booking_id, created_at ASC);
