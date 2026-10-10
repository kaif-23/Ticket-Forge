CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  starts_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Preserve the existing local seed before enforcing the relationship.
INSERT INTO events (id, name)
VALUES ('event-1', 'Event 1')
ON CONFLICT (id) DO NOTHING;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'seats_event_id_fkey'
      AND conrelid = 'public.seats'::regclass
  ) THEN
    ALTER TABLE seats
      ADD CONSTRAINT seats_event_id_fkey
      FOREIGN KEY (event_id) REFERENCES events(id);
  END IF;
END $$;
