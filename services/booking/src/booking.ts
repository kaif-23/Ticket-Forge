import { createHash, randomUUID } from "node:crypto";
import type { PoolClient } from "pg";
import { pool } from "./db.js";
import { insertOutboxRecord } from "./outbox.js";
import { recordSagaStep } from "./sagaLog.js";
import type {
    ProcessPaymentCommand,
    ReserveSeatsCommand,
} from "../../../packages/common/src/messages.js";

export type BookingStatus =
    | "PENDING"
    | "SEATS_HELD"
    | "PAID"
    | "CONFIRMED"
    | "CANCELLED"
    | "EXPIRED";

export interface BookingRequest {
    userId: string;
    eventId: string;
    seatNos: string[];
}

export interface BookingRecord {
    id: string;
    userId: string;
    eventId: string;
    seatNos: string[];
    status: BookingStatus;
    idempotencyKey: string;
    requestHash: string;
    createdAt: Date;
    updatedAt: Date;
}

export class InvalidBookingRequestError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "InvalidBookingRequestError";
    }
}

export class IdempotencyConflictError extends Error {
    constructor() {
        super("Idempotency-Key was already used with a different request");
        this.name = "IdempotencyConflictError";
    }
}

export class InvalidBookingIdError extends Error {
    constructor() {
        super("Booking ID must be a valid UUID");
        this.name = "InvalidBookingIdError";
    }
}

export function normalizeBookingRequest(input: unknown): BookingRequest {
    if (!input || typeof input !== "object") {
        throw new InvalidBookingRequestError("Request body must be an object");
    }

    const body = input as Record<string, unknown>;
    const userId = typeof body.userId === "string" ? body.userId.trim() : "";
    const eventId = typeof body.eventId === "string" ? body.eventId.trim() : "";
    const rawSeatNos = body.seatNos;

    if (!userId) {
        throw new InvalidBookingRequestError("userId must be a non-empty string");
    }
    if (!eventId) {
        throw new InvalidBookingRequestError("eventId must be a non-empty string");
    }
    if (!Array.isArray(rawSeatNos) || rawSeatNos.length === 0) {
        throw new InvalidBookingRequestError("seatNos must be a non-empty array");
    }

    const seatNos = rawSeatNos.map((seatNo) =>
        typeof seatNo === "string" ? seatNo.trim() : "",
    );

    if (seatNos.some((seatNo) => !seatNo)) {
        throw new InvalidBookingRequestError("seatNos must contain non-empty strings");
    }
    if (new Set(seatNos).size !== seatNos.length) {
        throw new InvalidBookingRequestError("seatNos must not contain duplicates");
    }

    return {
        userId,
        eventId,
        seatNos: [...seatNos].sort(),
    };
}

export function calculateRequestHash(request: BookingRequest): string {
    return createHash("sha256")
        .update(JSON.stringify(request))
        .digest("hex");
}

function rowToBooking(row: Record<string, unknown>): BookingRecord {
    return {
        id: row.id as string,
        userId: row.user_id as string,
        eventId: row.event_id as string,
        seatNos: row.seat_nos as string[],
        status: row.status as BookingStatus,
        idempotencyKey: row.idempotency_key as string,
        requestHash: row.request_hash as string,
        createdAt: row.created_at as Date,
        updatedAt: row.updated_at as Date,
    };
}

export function isValidBookingId(bookingId: string): boolean {
    return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(bookingId);
}

export async function getBookingById(bookingId: string): Promise<BookingRecord | null> {
    const result = await pool.query(
        `SELECT id, user_id, event_id, seat_nos, status,
                idempotency_key, request_hash, created_at, updated_at
         FROM bookings
         WHERE id = $1`,
        [bookingId],
    );

    return result.rows.length === 0 ? null : rowToBooking(result.rows[0]);
}

export interface BookingCreationResult {
    booking: BookingRecord;
    created: boolean;
    outboxRecord?: {
        id: string;
        bookingId: string;
        topic: string;
        type: string;
        occurredAt: string;
        payload: {
            eventId: string;
            seatNos: string[];
        };
    };
}

export interface CreateBookingOptions {
    outboxInserter?: (
        client: PoolClient,
        booking: BookingRecord,
        command: ReserveSeatsCommand,
    ) => Promise<void>;
}

export async function createOrGetBooking(
    request: BookingRequest,
    idempotencyKey: string,
    options?: CreateBookingOptions,
): Promise<BookingCreationResult> {
    const client = await pool.connect();
    const requestHash = calculateRequestHash(request);

    try {
        await client.query("BEGIN");

        const insertResult = await client.query(
            `INSERT INTO bookings (
                id, user_id, event_id, seat_nos, status,
                idempotency_key, request_hash
             )
             VALUES ($1, $2, $3, $4::jsonb, 'PENDING', $5, $6)
             ON CONFLICT (user_id, idempotency_key) DO NOTHING
             RETURNING id, user_id, event_id, seat_nos, status,
                       idempotency_key, request_hash, created_at, updated_at`,
            [
                randomUUID(),
                request.userId,
                request.eventId,
                JSON.stringify(request.seatNos),
                idempotencyKey,
                requestHash,
            ],
        );

        if (insertResult.rows.length > 0) {
            const booking = rowToBooking(insertResult.rows[0]);
            const messageId = randomUUID();
            const occurredAt = new Date().toISOString();
            const command: ReserveSeatsCommand = {
                messageId,
                bookingId: booking.id,
                type: "ReserveSeats",
                occurredAt,
                payload: {
                    eventId: booking.eventId,
                    seatNos: booking.seatNos,
                },
            };

            if (options?.outboxInserter) {
                await options.outboxInserter(client, booking, command);
            } else {
                await insertOutboxRecord(client, {
                    id: command.messageId,
                    bookingId: command.bookingId,
                    topic: "inventory.commands",
                    type: command.type,
                    occurredAt: command.occurredAt,
                    payload: command.payload,
                });
            }

            await recordSagaStep(client, {
                bookingId: booking.id,
                state: "PENDING",
                lastEvent: "BookingCreated",
                payload: {
                    eventId: booking.eventId,
                    seatNos: booking.seatNos,
                },
            });

            await client.query("COMMIT");
            return {
                booking,
                created: true,
                outboxRecord: {
                    id: command.messageId,
                    bookingId: command.bookingId,
                    topic: "inventory.commands",
                    type: command.type,
                    occurredAt: command.occurredAt,
                    payload: command.payload,
                },
            };
        }

        const existingResult = await client.query(
            `SELECT id, user_id, event_id, seat_nos, status,
                    idempotency_key, request_hash, created_at, updated_at
             FROM bookings
             WHERE user_id = $1 AND idempotency_key = $2
             FOR UPDATE`,
            [request.userId, idempotencyKey],
        );

        if (existingResult.rows.length !== 1) {
            throw new Error("Booking idempotency conflict could not be resolved");
        }

        const existingBooking = rowToBooking(existingResult.rows[0]);
        if (existingBooking.requestHash !== requestHash) {
            await client.query("ROLLBACK");
            throw new IdempotencyConflictError();
        }

        await client.query("COMMIT");
        return { booking: existingBooking, created: false };
    } catch (error) {
        try {
            await client.query("ROLLBACK");
        } catch {
            // Preserve the original database or idempotency error.
        }
        throw error;
    } finally {
        client.release();
    }
}

export class BookingNotFoundError extends Error {
    constructor() {
        super("Booking not found");
        this.name = "BookingNotFoundError";
    }
}

export class InvalidBookingStateError extends Error {
    constructor(currentState: string, expectedState: string) {
        super(`Cannot perform payment on booking in state ${currentState}; expected ${expectedState}`);
        this.name = "InvalidBookingStateError";
    }
}

export interface PayBookingResult {
    bookingId: string;
    status: BookingStatus;
    duplicate?: boolean;
    outboxRecord?: {
        id: string;
        bookingId: string;
        topic: string;
        type: string;
        occurredAt: string;
        payload: { amount: number; currency?: string };
    };
}

export async function payBooking(
    bookingId: string,
    options?: { amount?: number; currency?: string },
): Promise<PayBookingResult> {
    if (!bookingId || !isValidBookingId(bookingId)) {
        throw new InvalidBookingIdError();
    }

    const client = await pool.connect();
    try {
        await client.query("BEGIN");

        const result = await client.query(
            `SELECT id, user_id, event_id, seat_nos, status, idempotency_key, request_hash, created_at, updated_at
             FROM bookings
             WHERE id = $1
             FOR UPDATE`,
            [bookingId],
        );

        if (result.rows.length === 0) {
            await client.query("ROLLBACK");
            throw new BookingNotFoundError();
        }

        const booking = rowToBooking(result.rows[0]);
        if (booking.status === "PAID" || booking.status === "CONFIRMED") {
            await client.query("COMMIT");
            return {
                bookingId,
                status: booking.status,
                duplicate: true,
            };
        }

        if (booking.status !== "SEATS_HELD") {
            await client.query("ROLLBACK");
            throw new InvalidBookingStateError(booking.status, "SEATS_HELD");
        }

        const existingOutbox = await client.query(
            `SELECT id, topic, type, occurred_at, payload
             FROM outbox
             WHERE booking_id = $1 AND type = 'ProcessPayment'
             ORDER BY created_at DESC
             LIMIT 1`,
            [bookingId],
        );

        if (existingOutbox.rows.length > 0) {
            await client.query("COMMIT");
            const existing = existingOutbox.rows[0];
            return {
                bookingId,
                status: "SEATS_HELD",
                duplicate: true,
                outboxRecord: {
                    id: existing.id,
                    bookingId,
                    topic: existing.topic,
                    type: existing.type,
                    occurredAt:
                        existing.occurred_at instanceof Date
                            ? existing.occurred_at.toISOString()
                            : String(existing.occurred_at),
                    payload: existing.payload,
                },
            };
        }

        const messageId = randomUUID();
        const occurredAt = new Date().toISOString();
        const amount = options?.amount ?? 100;
        const payload: { amount: number; currency?: string } = {
            amount,
            ...(options?.currency ? { currency: options.currency } : {}),
        };

        const command: ProcessPaymentCommand = {
            messageId,
            bookingId,
            type: "ProcessPayment",
            occurredAt,
            payload,
        };

        await insertOutboxRecord(client, {
            id: command.messageId,
            bookingId: command.bookingId,
            topic: "payment.commands",
            type: command.type,
            occurredAt: command.occurredAt,
            payload: command.payload,
        });

        await recordSagaStep(client, {
            bookingId,
            state: "SEATS_HELD",
            lastEvent: "ProcessPayment",
            payload: command.payload,
        });

        await client.query("COMMIT");

        return {
            bookingId,
            status: "SEATS_HELD",
            outboxRecord: {
                id: command.messageId,
                bookingId: command.bookingId,
                topic: "payment.commands",
                type: command.type,
                occurredAt: command.occurredAt,
                payload,
            },
        };
    } catch (error) {
        try {
            await client.query("ROLLBACK");
        } catch {
            // Preserve original error
        }
        throw error;
    } finally {
        client.release();
    }
}

