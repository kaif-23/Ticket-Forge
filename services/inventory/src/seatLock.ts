import type { Redis } from "ioredis";
import { getRedisClient } from "./redis.js";

export const DEFAULT_LOCK_TTL_MS = 10000; // 10 seconds short-lived mutex

export const RELEASE_LOCK_LUA = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
`;

export interface AcquireLocksResult {
    acquired: boolean;
    failedSeat?: string;
    redisBypassed?: boolean;
}

/**
 * Acquire short-lived Redis mutexes for the requested seats using SET ... NX PX.
 * If any seat lock cannot be acquired, previously acquired locks in this attempt
 * are atomically rolled back via compare-and-delete.
 * If Redis is unavailable, it gracefully bypasses Redis and falls back to PostgreSQL authority.
 */
export async function acquireSeatLocks(
    eventId: string,
    seatNos: string[],
    bookingId: string,
    ttlMs = DEFAULT_LOCK_TTL_MS,
    client: Redis = getRedisClient(),
): Promise<AcquireLocksResult> {
    try {
        const acquiredKeys: string[] = [];

        for (const seatNo of seatNos) {
            const key = `seat:${eventId}:${seatNo}`;
            const result = await client.set(key, bookingId, "PX", ttlMs, "NX");

            if (result === "OK") {
                acquiredKeys.push(key);
            } else {
                // Rollback locks already acquired during this attempt
                for (const acquiredKey of acquiredKeys) {
                    try {
                        await client.eval(RELEASE_LOCK_LUA, 1, acquiredKey, bookingId);
                    } catch {
                        // ignore cleanup errors during rollback
                    }
                }
                return { acquired: false, failedSeat: seatNo };
            }
        }

        return { acquired: true, redisBypassed: false };
    } catch (error) {
        // Fallback to PostgreSQL authority on Redis error/unavailability
        return { acquired: true, redisBypassed: true };
    }
}

/**
 * Safe lock release using atomic Lua compare-and-delete.
 * Guarantees that callers only delete keys that they currently own.
 */
export async function releaseSeatLocks(
    eventId: string,
    seatNos: string[],
    bookingId: string,
    client: Redis = getRedisClient(),
): Promise<void> {
    try {
        for (const seatNo of seatNos) {
            const key = `seat:${eventId}:${seatNo}`;
            await client.eval(RELEASE_LOCK_LUA, 1, key, bookingId);
        }
    } catch {
        // Redis release errors are non-fatal as the short-lived lock will expire via PX
    }
}

/**
 * Inspect the current lock owner for a seat.
 */
export async function getSeatLockOwner(
    eventId: string,
    seatNo: string,
    client: Redis = getRedisClient(),
): Promise<string | null> {
    try {
        const key = `seat:${eventId}:${seatNo}`;
        return await client.get(key);
    } catch {
        return null;
    }
}

/**
 * Get remaining TTL of seat lock in milliseconds.
 */
export async function getSeatLockTtlMs(
    eventId: string,
    seatNo: string,
    client: Redis = getRedisClient(),
): Promise<number> {
    try {
        const key = `seat:${eventId}:${seatNo}`;
        return await client.pttl(key);
    } catch {
        return -1;
    }
}
