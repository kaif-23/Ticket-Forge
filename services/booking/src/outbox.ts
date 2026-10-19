import type { Pool, PoolClient } from "pg";

export interface OutboxRecord {
    id: string;
    bookingId: string;
    topic: string;
    type: string;
    occurredAt: string;
    payload: Record<string, unknown>;
    createdAt: Date;
    publishedAt: Date | null;
}

export interface CreateOutboxRecordInput {
    id: string;
    bookingId: string;
    topic: string;
    type: string;
    occurredAt: string;
    payload: Record<string, unknown>;
}

function rowToOutboxRecord(row: Record<string, unknown>): OutboxRecord {
    const occurredAt =
        row.occurred_at instanceof Date
            ? row.occurred_at.toISOString()
            : new Date(row.occurred_at as string | number).toISOString();

    return {
        id: row.id as string,
        bookingId: row.booking_id as string,
        topic: row.topic as string,
        type: row.type as string,
        occurredAt,
        payload: row.payload as OutboxRecord["payload"],
        createdAt: row.created_at as Date,
        publishedAt: (row.published_at as Date | null) ?? null,
    };
}

export async function insertOutboxRecord(
    client: PoolClient | Pool,
    input: CreateOutboxRecordInput,
): Promise<OutboxRecord> {
    const result = await client.query(
        `INSERT INTO outbox (
            id, booking_id, topic, type, occurred_at, payload
         )
         VALUES ($1, $2, $3, $4, $5, $6::jsonb)
         RETURNING id, booking_id, topic, type, occurred_at, payload, created_at, published_at`,
        [
            input.id,
            input.bookingId,
            input.topic,
            input.type,
            input.occurredAt,
            JSON.stringify(input.payload),
        ],
    );

    return rowToOutboxRecord(result.rows[0]);
}

export async function getOutboxRecordById(
    pool: Pool,
    id: string,
): Promise<OutboxRecord | null> {
    const result = await pool.query(
        `SELECT id, booking_id, topic, type, occurred_at, payload, created_at, published_at
         FROM outbox
         WHERE id = $1`,
        [id],
    );

    return result.rows.length === 0 ? null : rowToOutboxRecord(result.rows[0]);
}

export async function getOutboxRecordsByBookingId(
    pool: Pool,
    bookingId: string,
): Promise<OutboxRecord[]> {
    const result = await pool.query(
        `SELECT id, booking_id, topic, type, occurred_at, payload, created_at, published_at
         FROM outbox
         WHERE booking_id = $1
         ORDER BY created_at ASC`,
        [bookingId],
    );

    return result.rows.map(rowToOutboxRecord);
}
