import type { PoolClient } from "pg";
import type { Redis } from "ioredis";
import { pool } from "./db.js";
import { acquireSeatLocks, releaseSeatLocks } from "./seatLock.js";

export type ReserveSeatsResult =
    | { success: true }
    | { success: false; reason: "SEAT_UNAVAILABLE" };

export interface ReserveSeatsOptions {
    skipRedisLock?: boolean;
    lockTtlMs?: number;
    redisClient?: Redis;
    releaseLockOnSuccess?: boolean;
}

export async function reserveSeats(
    bookingId: string,
    eventId: string,
    seatNos: string[],
    client?: PoolClient,
    options?: ReserveSeatsOptions,
): Promise<ReserveSeatsResult> {
    const uniqueSeatNos = [...new Set(seatNos)];

    if (uniqueSeatNos.length === 0 || uniqueSeatNos.length !== seatNos.length) {
        return { success: false, reason: "SEAT_UNAVAILABLE" };
    }

    let lockAcquired = false;
    if (!options?.skipRedisLock) {
        const lockResult = await acquireSeatLocks(
            eventId,
            uniqueSeatNos,
            bookingId,
            options?.lockTtlMs,
            options?.redisClient,
        );

        if (!lockResult.acquired && !lockResult.redisBypassed) {
            // Fast-path rejection: Diverts competing traffic without touching PostgreSQL
            return { success: false, reason: "SEAT_UNAVAILABLE" };
        }
        lockAcquired = lockResult.acquired && !lockResult.redisBypassed;
    }

    const localClient = client ?? (await pool.connect());
    let transactionActive = false;

    try {
        if (!client) {
            await localClient.query("BEGIN");
            transactionActive = true;
        }

        const result = await localClient.query<{ seat_no: string }>(
            `UPDATE seats
       SET status = 'HELD',
           held_by_booking = $1,
           hold_expires_at = NOW() + INTERVAL '5 minutes',
           version = version + 1
       WHERE event_id = $2
         AND seat_no = ANY($3::text[])
         AND status = 'AVAILABLE'
       RETURNING seat_no`,
            [bookingId, eventId, uniqueSeatNos],
        );

        if (result.rowCount !== uniqueSeatNos.length) {
            if (!client && transactionActive) {
                await localClient.query("ROLLBACK");
                transactionActive = false;
            }
            if (lockAcquired) {
                await releaseSeatLocks(eventId, uniqueSeatNos, bookingId, options?.redisClient);
            }
            return { success: false, reason: "SEAT_UNAVAILABLE" };
        }

        if (!client) {
            await localClient.query("COMMIT");
            transactionActive = false;
        }

        if (lockAcquired && (options?.releaseLockOnSuccess ?? true)) {
            await releaseSeatLocks(eventId, uniqueSeatNos, bookingId, options?.redisClient);
        }

        return { success: true };
    } catch (error) {
        if (!client && transactionActive) {
            try {
                await localClient.query("ROLLBACK");
            } catch {
                // Preserve the original database error.
            }
        }
        if (lockAcquired) {
            await releaseSeatLocks(eventId, uniqueSeatNos, bookingId, options?.redisClient);
        }

        throw error;
    } finally {
        if (!client) {
            localClient.release();
        }
    }
}
