CREATE TABLE IF NOT EXISTS bookings (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  seat_nos JSONB NOT NULL
    CHECK (jsonb_typeof(seat_nos) = 'array' AND jsonb_array_length(seat_nos) > 0),
  status TEXT NOT NULL DEFAULT 'PENDING'
    CHECK (status IN ('PENDING', 'SEATS_HELD', 'PAID', 'CONFIRMED', 'CANCELLED', 'EXPIRED')),
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT unique_booking_user_idempotency UNIQUE (user_id, idempotency_key)
);
