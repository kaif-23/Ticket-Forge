
CREATE TABLE IF NOT EXISTS seats (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id TEXT NOT NULL,
  seat_no TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'AVAILABLE'
    CHECK (status IN ('AVAILABLE', 'HELD', 'SOLD')),
  held_by_booking TEXT,
  hold_expires_at TIMESTAMPTZ,
  version INTEGER NOT NULL DEFAULT 0,

  CONSTRAINT unique_event_seat UNIQUE (event_id, seat_no)
);
