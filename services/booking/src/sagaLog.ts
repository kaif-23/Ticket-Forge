import { randomUUID } from "node:crypto";
import type { Pool, PoolClient } from "pg";

export interface SagaLogRecord {
    id: string;
    bookingId: string;
    state: string;
    lastEvent: string;
    payload: Record<string, unknown> | null;
    createdAt: Date;
}

function rowToSagaLogRecord(row: Record<string, unknown>): SagaLogRecord {
    return {
        id: row.id as string,
        bookingId: row.booking_id as string,
        state: row.state as string,
        lastEvent: row.last_event as string,
        payload: (row.payload as Record<string, unknown> | null) ?? null,
        createdAt: row.created_at as Date,
    };
}

export async function recordSagaStep(
    clientOrPool: PoolClient | Pool,
    step: {
        bookingId: string;
        state: string;
        lastEvent: string;
        payload?: Record<string, unknown> | null;
    },
): Promise<SagaLogRecord> {
    const id = randomUUID();
    const result = await clientOrPool.query(
        `INSERT INTO saga_log (id, booking_id, state, last_event, payload)
         VALUES ($1, $2, $3, $4, $5::jsonb)
         RETURNING id, booking_id, state, last_event, payload, created_at`,
        [
            id,
            step.bookingId,
            step.state,
            step.lastEvent,
            step.payload ? JSON.stringify(step.payload) : null,
        ],
    );

    return rowToSagaLogRecord(result.rows[0]);
}

export async function getSagaLog(
    pool: Pool,
    bookingId: string,
): Promise<SagaLogRecord[]> {
    const result = await pool.query(
        `SELECT id, booking_id, state, last_event, payload, created_at
         FROM saga_log
         WHERE booking_id = $1
         ORDER BY created_at ASC`,
        [bookingId],
    );

    return result.rows.map(rowToSagaLogRecord);
}
