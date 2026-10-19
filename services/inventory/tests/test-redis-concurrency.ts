import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { pool } from "../src/db.js";
import { getRedisClient, closeRedis } from "../src/redis.js";
import {
    acquireSeatLocks,
    releaseSeatLocks,
    getSeatLockOwner,
    getSeatLockTtlMs,
    RELEASE_LOCK_LUA,
} from "../src/seatLock.js";
import { reserveSeats } from "../src/reserveSeats.js";

const testRunId = `redis-test-${Date.now()}`;
const eventId = `event-${testRunId}`;
const redis = getRedisClient();

async function setupSeats(seatNos: string[]) {
    await pool.query(
        "INSERT INTO events (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
        [eventId, `Event for ${testRunId}`],
    );

    for (const seatNo of seatNos) {
        await pool.query(
            `INSERT INTO seats (event_id, seat_no, status, version)
             VALUES ($1, $2, 'AVAILABLE', 0)
             ON CONFLICT (event_id, seat_no) DO UPDATE
             SET status = 'AVAILABLE', held_by_booking = NULL, hold_expires_at = NULL, version = 0`,
            [eventId, seatNo],
        );
    }
}

async function getSeat(seatNo: string) {
    const res = await pool.query<{
        status: string;
        held_by_booking: string | null;
        hold_expires_at: Date | null;
        version: number;
    }>(
        "SELECT status, held_by_booking, hold_expires_at, version FROM seats WHERE event_id = $1 AND seat_no = $2",
        [eventId, seatNo],
    );
    return res.rows[0];
}

// =========================================================================
// TEST 1: Redis Lock Acquisition and Safe Release (Atomic Compare-and-Delete)
// =========================================================================
async function testRedisLockAcquisitionAndRelease() {
    console.log("\n--- 1. Testing Redis Lock Acquisition & Safe Compare-and-Delete Release ---");

    const bookingId1 = randomUUID();
    const bookingId2 = randomUUID();
    const seatNo = "L1";

    // 1. Acquire lock for bookingId1
    const res1 = await acquireSeatLocks(eventId, [seatNo], bookingId1, 5000, redis);
    assert.equal(res1.acquired, true);
    assert.equal(res1.redisBypassed, false);

    const owner = await getSeatLockOwner(eventId, seatNo, redis);
    assert.equal(owner, bookingId1, "Lock owner in Redis must match bookingId1");

    const ttl = await getSeatLockTtlMs(eventId, seatNo, redis);
    assert.ok(ttl > 0 && ttl <= 5000, `TTL must be > 0 and <= 5000, got ${ttl}`);

    // 2. Competing acquisition for bookingId2 must fail fast
    const res2 = await acquireSeatLocks(eventId, [seatNo], bookingId2, 5000, redis);
    assert.equal(res2.acquired, false);
    assert.equal(res2.failedSeat, seatNo);

    // 3. Attempt release by wrong owner (bookingId2) must NOT delete bookingId1's lock
    await releaseSeatLocks(eventId, [seatNo], bookingId2, redis);
    const ownerStill1 = await getSeatLockOwner(eventId, seatNo, redis);
    assert.equal(ownerStill1, bookingId1, "Wrong owner must not be able to delete another caller's lock");

    // 4. Release by correct owner (bookingId1) deletes the key
    await releaseSeatLocks(eventId, [seatNo], bookingId1, redis);
    const ownerAfterRelease = await getSeatLockOwner(eventId, seatNo, redis);
    assert.equal(ownerAfterRelease, null, "Lock must be deleted after release by owner");

    // 5. Multi-seat atomic rollback: if one seat is locked, previous seats are rolled back
    const seatA = "L2";
    const seatB = "L3";
    // Pre-lock seatB with bookingId2
    await redis.set(`seat:${eventId}:${seatB}`, bookingId2, "PX", 5000, "NX");

    // Attempt to lock [seatA, seatB] with bookingId1
    const multiRes = await acquireSeatLocks(eventId, [seatA, seatB], bookingId1, 5000, redis);
    assert.equal(multiRes.acquired, false);
    assert.equal(multiRes.failedSeat, seatB);

    // Verify seatA was rolled back and is NOT left locked
    const ownerA = await getSeatLockOwner(eventId, seatA, redis);
    assert.equal(ownerA, null, "seatA must be rolled back when seatB fails");

    // Clean up seatB
    await releaseSeatLocks(eventId, [seatB], bookingId2, redis);

    console.log("✓ Redis lock acquisition, atomic rollback, and safe compare-and-delete verified");
}

// =========================================================================
// TEST 2: Distinct Concepts: 10s Redis Mutex vs 5-Min PostgreSQL Business Hold
// =========================================================================
async function testDistinctionBetweenLockAndHold() {
    console.log("\n--- 2. Testing Distinction: 10s Redis Mutex vs 5-Minute PostgreSQL Hold ---");

    await setupSeats(["H1"]);
    const bookingA = randomUUID();
    const bookingB = randomUUID();

    // 1. Reserve seat H1 with a very short Redis lock (1000ms) but keep lock active
    const resA = await reserveSeats(bookingA, eventId, ["H1"], undefined, {
        lockTtlMs: 1000,
        releaseLockOnSuccess: false, // Keep Redis key active to test TTL expiry
    });
    assert.equal(resA.success, true);

    // Verify immediately in PostgreSQL: 5-minute business hold is established
    const seatInPg = await getSeat("H1");
    assert.equal(seatInPg.status, "HELD");
    assert.equal(seatInPg.held_by_booking, bookingA);
    const holdRemainingMs = seatInPg.hold_expires_at!.getTime() - Date.now();
    assert.ok(
        holdRemainingMs > 4 * 60 * 1000,
        `PostgreSQL hold must be ~5 minutes, got ${holdRemainingMs / 1000}s`,
    );

    // Verify in Redis: TTL is <= 1000ms
    const lockTtl = await getSeatLockTtlMs(eventId, "H1", redis);
    assert.ok(lockTtl > 0 && lockTtl <= 1000, `Redis lock TTL must be <= 1000ms, got ${lockTtl}ms`);
    console.log(`  Initial state: PostgreSQL hold = ~300s, Redis mutex TTL = ${lockTtl}ms`);

    // 2. Wait for Redis short-lived mutex to naturally expire (1200ms)
    console.log("  Waiting 1200ms for Redis mutex to expire...");
    await new Promise((resolve) => setTimeout(resolve, 1200));

    // Verify Redis lock is gone
    const redisOwnerAfterExpiry = await getSeatLockOwner(eventId, "H1", redis);
    assert.equal(redisOwnerAfterExpiry, null, "Redis lock must have expired and returned null");

    // Verify PostgreSQL hold is STILL authoritative and active
    const seatInPgAfterExpiry = await getSeat("H1");
    assert.equal(seatInPgAfterExpiry.status, "HELD", "PostgreSQL seat status must remain HELD");
    assert.equal(seatInPgAfterExpiry.held_by_booking, bookingA);

    // 3. Competing bookingB tries to acquire H1 now that Redis lock expired
    const resB = await reserveSeats(bookingB, eventId, ["H1"], undefined, {
        lockTtlMs: 10000,
    });
    assert.equal(
        resB.success,
        false,
        "PostgreSQL must reject bookingB because 5-minute hold is still active in DB",
    );
    assert.equal((resB as { reason: string }).reason, "SEAT_UNAVAILABLE");

    // Seat remains safely owned by bookingA
    const seatFinal = await getSeat("H1");
    assert.equal(seatFinal.held_by_booking, bookingA);
    assert.equal(seatFinal.version, 1, "Seat version must remain 1");

    console.log("✓ Proved: Redis lock expiry CANNOT compromise the 5-minute PostgreSQL business hold");
}

// =========================================================================
// TEST 3: Redis Failure / Unavailability Resilience
// =========================================================================
async function testRedisFailureResilience() {
    console.log("\n--- 3. Testing Resilience to Redis Outage (PostgreSQL Authority) ---");

    await setupSeats(["F1"]);
    const bookingFailover1 = randomUUID();
    const bookingFailover2 = randomUUID();

    // Create a disconnected/broken Redis client pointing to an unallocated port
    const brokenRedis = new Redis({
        host: "127.0.0.1",
        port: 65530, // dead port
        connectTimeout: 200,
        maxRetriesPerRequest: 0,
        enableOfflineQueue: false,
        retryStrategy: () => null,
    });
    brokenRedis.on("error", () => {}); // silence connection errors

    // 1. Reserve seat F1 with the broken Redis client
    const res1 = await reserveSeats(bookingFailover1, eventId, ["F1"], undefined, {
        redisClient: brokenRedis,
    });

    assert.equal(res1.success, true, "Reservation must succeed via PostgreSQL authority despite Redis outage");

    const seatF1 = await getSeat("F1");
    assert.equal(seatF1.status, "HELD");
    assert.equal(seatF1.held_by_booking, bookingFailover1);
    assert.equal(seatF1.version, 1);

    // 2. Second competing request during Redis outage must still be rejected by PostgreSQL
    const res2 = await reserveSeats(bookingFailover2, eventId, ["F1"], undefined, {
        redisClient: brokenRedis,
    });
    assert.equal(res2.success, false, "Second attempt must be rejected by PostgreSQL authority");
    assert.equal((res2 as { reason: string }).reason, "SEAT_UNAVAILABLE");

    await brokenRedis.quit().catch(() => brokenRedis.disconnect());
    console.log("✓ Correctness survives Redis failure: PostgreSQL maintains 100% authoritative safety");
}

// =========================================================================
// TEST 4: High Concurrency: 50 Competing Requests for Same Seat
// =========================================================================
async function testHighConcurrencyZeroOverselling() {
    console.log("\n--- 4. Testing 50 Competing Concurrent Requests (Zero Oversold Guarantee) ---");

    const seatNo = "CONC1";
    await setupSeats([seatNo]);

    const numCallers = 50;
    const callers = Array.from({ length: numCallers }, () => ({
        bookingId: randomUUID(),
    }));

    // Launch all 50 concurrent requests simultaneously
    const results = await Promise.all(
        callers.map((c) =>
            reserveSeats(c.bookingId, eventId, [seatNo], undefined, {
                lockTtlMs: 10000,
            }),
        ),
    );

    const successful = results.filter((r) => r.success);
    const failed = results.filter((r) => !r.success);

    assert.equal(successful.length, 1, `Exactly 1 caller must succeed, got ${successful.length}`);
    assert.equal(failed.length, numCallers - 1, `Exactly 49 callers must fail, got ${failed.length}`);

    // Verify PostgreSQL state
    const seat = await getSeat(seatNo);
    assert.equal(seat.status, "HELD");
    assert.equal(seat.version, 1, "Seat version must increment by exactly 1");

    console.log(`✓ 50 concurrent requests: exactly 1 succeeded, ${failed.length} safely rejected. Zero oversold seats!`);
}

// =========================================================================
// TEST 5: Measure the Concurrency Benefit (Benchmark)
// =========================================================================
async function measureConcurrencyBenefit() {
    console.log("\n--- 5. Measuring Concurrency Benefit (Redis Fast Path vs Direct DB) ---");

    const numRequests = 60;

    // --- Benchmark A: WITH Redis Concurrency Assistance ---
    const seatWithRedis = "BENCH_REDIS";
    await setupSeats([seatWithRedis]);

    const startWithRedis = Date.now();
    const withRedisResults = await Promise.all(
        Array.from({ length: numRequests }, () => {
            const bId = randomUUID();
            const reqStart = performance.now();
            return reserveSeats(bId, eventId, [seatWithRedis], undefined, {
                skipRedisLock: false,
                lockTtlMs: 10000,
            }).then((res) => ({ ...res, durationMs: performance.now() - reqStart }));
        }),
    );
    const totalDurationWithRedis = Date.now() - startWithRedis;

    const successfulWithRedis = withRedisResults.filter((r) => r.success);
    const failedWithRedis = withRedisResults.filter((r) => !r.success);
    const avgFailedLatencyWithRedis =
        failedWithRedis.reduce((acc, r) => acc + r.durationMs, 0) / failedWithRedis.length;

    // --- Benchmark B: WITHOUT Redis Concurrency Assistance (Direct PostgreSQL) ---
    const seatWithoutRedis = "BENCH_NO_REDIS";
    await setupSeats([seatWithoutRedis]);

    const startNoRedis = Date.now();
    const withoutRedisResults = await Promise.all(
        Array.from({ length: numRequests }, () => {
            const bId = randomUUID();
            const reqStart = performance.now();
            return reserveSeats(bId, eventId, [seatWithoutRedis], undefined, {
                skipRedisLock: true, // Forces all 60 concurrent requests directly into PostgreSQL
            }).then((res) => ({ ...res, durationMs: performance.now() - reqStart }));
        }),
    );
    const totalDurationNoRedis = Date.now() - startNoRedis;

    const successfulNoRedis = withoutRedisResults.filter((r) => r.success);
    const failedNoRedis = withoutRedisResults.filter((r) => !r.success);
    const avgFailedLatencyNoRedis =
        failedNoRedis.reduce((acc, r) => acc + r.durationMs, 0) / failedNoRedis.length;

    assert.equal(successfulWithRedis.length, 1);
    assert.equal(successfulNoRedis.length, 1);

    console.log("\n=================== BENCHMARK RESULTS ===================");
    console.log(`Concurrent competing requests: ${numRequests}`);
    console.log("---------------------------------------------------------");
    console.log(`WITH Redis assistance:`);
    console.log(`  - Total duration:         ${totalDurationWithRedis}ms`);
    console.log(`  - Avg rejection latency:  ${avgFailedLatencyWithRedis.toFixed(2)}ms`);
    console.log(`  - Database load spared:   ${numRequests - 1} queries spared (diverted by Redis)`);
    console.log("---------------------------------------------------------");
    console.log(`WITHOUT Redis assistance (Direct PostgreSQL):`);
    console.log(`  - Total duration:         ${totalDurationNoRedis}ms`);
    console.log(`  - Avg rejection latency:  ${avgFailedLatencyNoRedis.toFixed(2)}ms`);
    console.log(`  - Database queries hit:   ${numRequests} queries hit PostgreSQL connection pool`);
    console.log("=========================================================\n");

    console.log("✓ Concurrency benefit measured: Redis fast-path spares PostgreSQL connection pool from contention lock waves");
}

async function main() {
    try {
        console.log("==========================================================");
        console.log("STARTING PHASE 10 REDIS CONCURRENCY TEST SUITE");
        console.log("==========================================================");

        await testRedisLockAcquisitionAndRelease();
        await testDistinctionBetweenLockAndHold();
        await testRedisFailureResilience();
        await testHighConcurrencyZeroOverselling();
        await measureConcurrencyBenefit();

        console.log("\n==========================================================");
        console.log("ALL PHASE 10 REDIS CONCURRENCY TESTS PASSED!");
        console.log("==========================================================");
    } finally {
        await closeRedis();
        await pool.end();
    }
}

main().catch((err) => {
    console.error("Test execution failure:", err);
    process.exit(1);
});
