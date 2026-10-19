import { randomUUID } from "node:crypto";
import type {
    BookingCancelledEvent,
    BookingConfirmedEvent,
    ConfirmSeatsCommand,
    HoldExpiredEvent,
    PaymentFailedEvent,
    PaymentSucceededEvent,
    ReleaseSeatsCommand,
    SeatsConfirmedEvent,
    SeatsReservationFailedEvent,
    SeatsReservedEvent,
} from "../../../packages/common/src/messages.js";
import type { BookingStatus } from "./booking.js";
import { pool } from "./db.js";
import { insertOutboxRecord } from "./outbox.js";
import { recordSagaStep } from "./sagaLog.js";
import { recordProcessedMessage } from "./idempotency.js";

export type InventoryOutcomeEvent = SeatsReservedEvent | SeatsReservationFailedEvent;
export type PaymentOutcomeEvent = PaymentSucceededEvent | PaymentFailedEvent;

export type SagaTransitionResult =
    | { kind: "updated"; bookingId: string; status: BookingStatus; commandEmitted?: string }
    | { kind: "unknown"; bookingId: string }
    | { kind: "stale"; bookingId: string; currentStatus: string }
    | { kind: "duplicate"; bookingId: string; messageId: string };

export class InvalidInventoryEventError extends Error {
    constructor(message = "Invalid Inventory outcome event") {
        super(message);
        this.name = "InvalidInventoryEventError";
    }
}

export class InvalidPaymentEventError extends Error {
    constructor(message = "Invalid Payment event") {
        super(message);
        this.name = "InvalidPaymentEventError";
    }
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null;
}

export function parseInventoryOutcomeEvent(value: unknown): InventoryOutcomeEvent {
    if (!isRecord(value)) {
        throw new InvalidInventoryEventError();
    }

    const type = value.type;
    const bookingId = value.bookingId;
    const messageId = value.messageId;
    const occurredAt = value.occurredAt;
    const payload = value.payload;

    if (!isRecord(payload)) {
        throw new InvalidInventoryEventError();
    }

    const eventId = payload.eventId;
    const seatNos = payload.seatNos;

    if (
        (type !== "SeatsReserved" && type !== "SeatsReservationFailed") ||
        typeof messageId !== "string" ||
        messageId.trim().length === 0 ||
        typeof occurredAt !== "string" ||
        occurredAt.trim().length === 0 ||
        typeof bookingId !== "string" ||
        bookingId.trim().length === 0 ||
        typeof eventId !== "string" ||
        eventId.trim().length === 0 ||
        !Array.isArray(seatNos) ||
        seatNos.length === 0 ||
        seatNos.some((seatNo) => typeof seatNo !== "string" || seatNo.trim().length === 0)
    ) {
        throw new InvalidInventoryEventError();
    }

    const normalizedSeatNos = seatNos.map((seatNo) => seatNo.trim()).sort();
    if (new Set(normalizedSeatNos).size !== normalizedSeatNos.length) {
        throw new InvalidInventoryEventError();
    }

    if (type === "SeatsReservationFailed" && payload.reason !== "SEAT_UNAVAILABLE") {
        throw new InvalidInventoryEventError();
    }

    return type === "SeatsReserved"
        ? { messageId, bookingId, type, occurredAt, payload: { eventId, seatNos: normalizedSeatNos } }
        : { messageId, bookingId, type, occurredAt, payload: { eventId, seatNos: normalizedSeatNos, reason: "SEAT_UNAVAILABLE" } };
}

export function parsePaymentOutcomeEvent(value: unknown): PaymentOutcomeEvent {
    if (!isRecord(value)) {
        throw new InvalidPaymentEventError();
    }

    const type = value.type;
    const bookingId = value.bookingId;
    const messageId = value.messageId;
    const occurredAt = value.occurredAt;
    const payload = value.payload;

    if (
        (type !== "PaymentSucceeded" && type !== "PaymentFailed") ||
        typeof messageId !== "string" ||
        messageId.trim().length === 0 ||
        typeof occurredAt !== "string" ||
        occurredAt.trim().length === 0 ||
        typeof bookingId !== "string" ||
        bookingId.trim().length === 0 ||
        !isRecord(payload)
    ) {
        throw new InvalidPaymentEventError();
    }

    if (type === "PaymentSucceeded") {
        if (typeof payload.paymentId !== "string" || typeof payload.amount !== "number") {
            throw new InvalidPaymentEventError("Invalid PaymentSucceeded payload");
        }
        return {
            messageId,
            bookingId,
            type: "PaymentSucceeded",
            occurredAt,
            payload: {
                paymentId: payload.paymentId,
                amount: payload.amount,
            },
        };
    }

    if (typeof payload.reason !== "string" || payload.reason.trim().length === 0) {
        throw new InvalidPaymentEventError("Invalid PaymentFailed payload");
    }

    return {
        messageId,
        bookingId,
        type: "PaymentFailed",
        occurredAt,
        payload: {
            reason: payload.reason,
        },
    };
}

export function parseSeatsConfirmedEvent(value: unknown): SeatsConfirmedEvent {
    if (!isRecord(value)) {
        throw new InvalidInventoryEventError("Invalid SeatsConfirmed event");
    }

    const type = value.type;
    const bookingId = value.bookingId;
    const messageId = value.messageId;
    const occurredAt = value.occurredAt;
    const payload = value.payload;

    if (
        type !== "SeatsConfirmed" ||
        typeof messageId !== "string" ||
        messageId.trim().length === 0 ||
        typeof occurredAt !== "string" ||
        occurredAt.trim().length === 0 ||
        typeof bookingId !== "string" ||
        bookingId.trim().length === 0 ||
        !isRecord(payload) ||
        typeof payload.eventId !== "string" ||
        !Array.isArray(payload.seatNos) ||
        payload.seatNos.length === 0
    ) {
        throw new InvalidInventoryEventError("Invalid SeatsConfirmed event structure");
    }

    return {
        messageId,
        bookingId,
        type: "SeatsConfirmed",
        occurredAt,
        payload: {
            eventId: payload.eventId as string,
            seatNos: payload.seatNos as string[],
        },
    };
}

export async function applyInventoryOutcome(
    event: unknown,
    consumerGroup = "booking-service-group",
    topic = "inventory.events",
): Promise<SagaTransitionResult> {
    const parsedEvent = parseInventoryOutcomeEvent(event);
    const nextStatus = parsedEvent.type === "SeatsReserved" ? "SEATS_HELD" : "CANCELLED";

    const client = await pool.connect();
    try {
        await client.query("BEGIN");

        const { isDuplicate } = await recordProcessedMessage(
            client,
            parsedEvent.messageId,
            consumerGroup,
            topic,
        );

        if (isDuplicate) {
            await client.query("ROLLBACK");
            return {
                kind: "duplicate",
                bookingId: parsedEvent.bookingId,
                messageId: parsedEvent.messageId,
            };
        }

        const updateResult = await client.query(
            `UPDATE bookings
             SET status = $1, updated_at = NOW()
             WHERE id = $2
               AND status = 'PENDING'
               AND event_id = $3
               AND seat_nos = $4::jsonb`,
            [nextStatus, parsedEvent.bookingId, parsedEvent.payload.eventId, JSON.stringify(parsedEvent.payload.seatNos)],
        );

        if (updateResult.rowCount === 1) {
            await recordSagaStep(client, {
                bookingId: parsedEvent.bookingId,
                state: nextStatus,
                lastEvent: parsedEvent.type,
                payload: parsedEvent.payload,
            });

            if (nextStatus === "CANCELLED") {
                const cancelEvent: BookingCancelledEvent = {
                    messageId: randomUUID(),
                    bookingId: parsedEvent.bookingId,
                    type: "BookingCancelled",
                    occurredAt: new Date().toISOString(),
                    payload: { reason: "SEATS_RESERVATION_FAILED" },
                };
                await insertOutboxRecord(client, {
                    id: cancelEvent.messageId,
                    bookingId: cancelEvent.bookingId,
                    topic: "booking.events",
                    type: cancelEvent.type,
                    occurredAt: cancelEvent.occurredAt,
                    payload: cancelEvent.payload,
                });
            }

            await client.query("COMMIT");
            return { kind: "updated", bookingId: parsedEvent.bookingId, status: nextStatus };
        }

        await client.query("ROLLBACK");

        const existingResult = await pool.query<{ status: string }>(
            "SELECT status FROM bookings WHERE id = $1",
            [parsedEvent.bookingId],
        );

        if (existingResult.rows.length === 0) {
            return { kind: "unknown", bookingId: parsedEvent.bookingId };
        }

        return {
            kind: "stale",
            bookingId: parsedEvent.bookingId,
            currentStatus: existingResult.rows[0].status,
        };
    } catch (error) {
        try {
            await client.query("ROLLBACK");
        } catch {
            // Ignore rollback error
        }
        throw error;
    } finally {
        client.release();
    }
}

export async function applyPaymentOutcome(
    event: unknown,
    consumerGroup = "booking-service-group",
    topic = "payment.events",
): Promise<SagaTransitionResult> {
    const parsedEvent = parsePaymentOutcomeEvent(event);

    const client = await pool.connect();
    try {
        await client.query("BEGIN");

        const { isDuplicate } = await recordProcessedMessage(
            client,
            parsedEvent.messageId,
            consumerGroup,
            topic,
        );

        if (isDuplicate) {
            await client.query("ROLLBACK");
            return {
                kind: "duplicate",
                bookingId: parsedEvent.bookingId,
                messageId: parsedEvent.messageId,
            };
        }

        if (parsedEvent.type === "PaymentSucceeded") {
            const updateResult = await client.query<{ id: string; event_id: string; seat_nos: string[] }>(
                `UPDATE bookings
                 SET status = 'PAID', updated_at = NOW()
                 WHERE id = $1 AND status = 'SEATS_HELD'
                 RETURNING id, event_id, seat_nos`,
                [parsedEvent.bookingId],
            );

            if (updateResult.rowCount === 1) {
                const booking = updateResult.rows[0];
                const command: ConfirmSeatsCommand = {
                    messageId: randomUUID(),
                    bookingId: booking.id,
                    type: "ConfirmSeats",
                    occurredAt: new Date().toISOString(),
                    payload: {
                        eventId: booking.event_id,
                        seatNos: booking.seat_nos,
                    },
                };

                await insertOutboxRecord(client, {
                    id: command.messageId,
                    bookingId: command.bookingId,
                    topic: "inventory.commands",
                    type: command.type,
                    occurredAt: command.occurredAt,
                    payload: command.payload,
                });

                await recordSagaStep(client, {
                    bookingId: booking.id,
                    state: "PAID",
                    lastEvent: "PaymentSucceeded",
                    payload: parsedEvent.payload,
                });

                await client.query("COMMIT");
                return {
                    kind: "updated",
                    bookingId: booking.id,
                    status: "PAID",
                    commandEmitted: "ConfirmSeats",
                };
            }
        } else {
            // PaymentFailed -> Compensation path
            const updateResult = await client.query<{ id: string; event_id: string; seat_nos: string[] }>(
                `UPDATE bookings
                 SET status = 'CANCELLED', updated_at = NOW()
                 WHERE id = $1 AND status = 'SEATS_HELD'
                 RETURNING id, event_id, seat_nos`,
                [parsedEvent.bookingId],
            );

            if (updateResult.rowCount === 1) {
                const booking = updateResult.rows[0];
                const command: ReleaseSeatsCommand = {
                    messageId: randomUUID(),
                    bookingId: booking.id,
                    type: "ReleaseSeats",
                    occurredAt: new Date().toISOString(),
                    payload: {
                        eventId: booking.event_id,
                        seatNos: booking.seat_nos,
                    },
                };

                // Compensation outbox message
                await insertOutboxRecord(client, {
                    id: command.messageId,
                    bookingId: command.bookingId,
                    topic: "inventory.commands",
                    type: command.type,
                    occurredAt: command.occurredAt,
                    payload: command.payload,
                });

                const cancelEvent: BookingCancelledEvent = {
                    messageId: randomUUID(),
                    bookingId: booking.id,
                    type: "BookingCancelled",
                    occurredAt: new Date().toISOString(),
                    payload: { reason: parsedEvent.payload.reason },
                };

                await insertOutboxRecord(client, {
                    id: cancelEvent.messageId,
                    bookingId: cancelEvent.bookingId,
                    topic: "booking.events",
                    type: cancelEvent.type,
                    occurredAt: cancelEvent.occurredAt,
                    payload: cancelEvent.payload,
                });

                await recordSagaStep(client, {
                    bookingId: booking.id,
                    state: "CANCELLED",
                    lastEvent: "PaymentFailed",
                    payload: parsedEvent.payload,
                });

                await client.query("COMMIT");
                return {
                    kind: "updated",
                    bookingId: booking.id,
                    status: "CANCELLED",
                    commandEmitted: "ReleaseSeats",
                };
            }
        }

        await client.query("ROLLBACK");

        const existingResult = await pool.query<{ status: string }>(
            "SELECT status FROM bookings WHERE id = $1",
            [parsedEvent.bookingId],
        );

        if (existingResult.rows.length === 0) {
            return { kind: "unknown", bookingId: parsedEvent.bookingId };
        }

        return {
            kind: "stale",
            bookingId: parsedEvent.bookingId,
            currentStatus: existingResult.rows[0].status,
        };
    } catch (error) {
        try {
            await client.query("ROLLBACK");
        } catch {
            // Ignore rollback error
        }
        throw error;
    } finally {
        client.release();
    }
}

export async function applySeatsConfirmed(
    event: unknown,
    consumerGroup = "booking-service-group",
    topic = "inventory.events",
): Promise<SagaTransitionResult> {
    const parsedEvent = parseSeatsConfirmedEvent(event);

    const client = await pool.connect();
    try {
        await client.query("BEGIN");

        const { isDuplicate } = await recordProcessedMessage(
            client,
            parsedEvent.messageId,
            consumerGroup,
            topic,
        );

        if (isDuplicate) {
            await client.query("ROLLBACK");
            return {
                kind: "duplicate",
                bookingId: parsedEvent.bookingId,
                messageId: parsedEvent.messageId,
            };
        }

        const updateResult = await client.query<{ id: string; user_id: string; event_id: string; seat_nos: string[] }>(
            `UPDATE bookings
             SET status = 'CONFIRMED', updated_at = NOW()
             WHERE id = $1 AND status = 'PAID'
             RETURNING id, user_id, event_id, seat_nos`,
            [parsedEvent.bookingId],
        );

        if (updateResult.rowCount === 1) {
            const booking = updateResult.rows[0];
            const event: BookingConfirmedEvent = {
                messageId: randomUUID(),
                bookingId: booking.id,
                type: "BookingConfirmed",
                occurredAt: new Date().toISOString(),
                payload: {
                    userId: booking.user_id,
                    eventId: booking.event_id,
                    seatNos: booking.seat_nos,
                },
            };

            await insertOutboxRecord(client, {
                id: event.messageId,
                bookingId: event.bookingId,
                topic: "booking.events",
                type: event.type,
                occurredAt: event.occurredAt,
                payload: event.payload,
            });

            await recordSagaStep(client, {
                bookingId: booking.id,
                state: "CONFIRMED",
                lastEvent: "SeatsConfirmed",
                payload: parsedEvent.payload,
            });

            await client.query("COMMIT");
            return {
                kind: "updated",
                bookingId: booking.id,
                status: "CONFIRMED",
            };
        }

        await client.query("ROLLBACK");

        const existingResult = await pool.query<{ status: string }>(
            "SELECT status FROM bookings WHERE id = $1",
            [parsedEvent.bookingId],
        );

        if (existingResult.rows.length === 0) {
            return { kind: "unknown", bookingId: parsedEvent.bookingId };
        }

        return {
            kind: "stale",
            bookingId: parsedEvent.bookingId,
            currentStatus: existingResult.rows[0].status,
        };
    } catch (error) {
        try {
            await client.query("ROLLBACK");
        } catch {
            // Ignore rollback error
        }
        throw error;
    } finally {
        client.release();
    }
}

export function parseHoldExpiredEvent(value: unknown): HoldExpiredEvent {
    if (!isRecord(value)) {
        throw new InvalidInventoryEventError("Invalid HoldExpired event");
    }

    const type = value.type;
    const bookingId = value.bookingId;
    const messageId = value.messageId;
    const occurredAt = value.occurredAt;
    const payload = value.payload;

    if (
        type !== "HoldExpired" ||
        typeof messageId !== "string" ||
        messageId.trim().length === 0 ||
        typeof occurredAt !== "string" ||
        occurredAt.trim().length === 0 ||
        typeof bookingId !== "string" ||
        bookingId.trim().length === 0 ||
        !isRecord(payload) ||
        typeof payload.eventId !== "string" ||
        !Array.isArray(payload.seatNos) ||
        payload.seatNos.length === 0
    ) {
        throw new InvalidInventoryEventError("Invalid HoldExpired event structure");
    }

    return {
        messageId,
        bookingId,
        type: "HoldExpired",
        occurredAt,
        payload: {
            eventId: payload.eventId as string,
            seatNos: payload.seatNos as string[],
        },
    };
}

export async function applyHoldExpired(
    event: unknown,
    consumerGroup = "booking-service-group",
    topic = "inventory.events",
): Promise<SagaTransitionResult> {
    const parsedEvent = parseHoldExpiredEvent(event);

    const client = await pool.connect();
    try {
        await client.query("BEGIN");

        const { isDuplicate } = await recordProcessedMessage(
            client,
            parsedEvent.messageId,
            consumerGroup,
            topic,
        );

        if (isDuplicate) {
            await client.query("ROLLBACK");
            return {
                kind: "duplicate",
                bookingId: parsedEvent.bookingId,
                messageId: parsedEvent.messageId,
            };
        }

        const updateResult = await client.query<{ id: string; event_id: string; seat_nos: string[] }>(
            `UPDATE bookings
             SET status = 'EXPIRED', updated_at = NOW()
             WHERE id = $1 AND status = 'SEATS_HELD'
             RETURNING id, event_id, seat_nos`,
            [parsedEvent.bookingId],
        );

        if (updateResult.rowCount === 1) {
            await recordSagaStep(client, {
                bookingId: parsedEvent.bookingId,
                state: "EXPIRED",
                lastEvent: "HoldExpired",
                payload: parsedEvent.payload,
            });

            await client.query("COMMIT");
            return {
                kind: "updated",
                bookingId: parsedEvent.bookingId,
                status: "EXPIRED",
            };
        }

        await client.query("ROLLBACK");

        const existingResult = await pool.query<{ status: string }>(
            "SELECT status FROM bookings WHERE id = $1",
            [parsedEvent.bookingId],
        );

        if (existingResult.rows.length === 0) {
            return { kind: "unknown", bookingId: parsedEvent.bookingId };
        }

        return {
            kind: "stale",
            bookingId: parsedEvent.bookingId,
            currentStatus: existingResult.rows[0].status,
        };
    } catch (error) {
        try {
            await client.query("ROLLBACK");
        } catch {
            // Ignore rollback error
        }
        throw error;
    } finally {
        client.release();
    }
}

