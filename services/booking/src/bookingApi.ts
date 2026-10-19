import express from "express";
import {
    createOrGetBooking,
    getBookingById,
    IdempotencyConflictError,
    InvalidBookingIdError,
    InvalidBookingRequestError,
    InvalidBookingStateError,
    BookingNotFoundError,
    isValidBookingId,
    normalizeBookingRequest,
    payBooking,
} from "./booking.js";
import { pool } from "./db.js";
import { getSagaLog } from "./sagaLog.js";

export type BookingCreatedCallback = (bookingId: string) => Promise<void> | void;

export function createBookingApp(onBookingCreated?: BookingCreatedCallback) {
    const app = express();
    app.use(express.json());

    app.get("/health", (_request, response) => {
        response.status(200).json({
            service: "booking",
            status: "ok",
        });
    });

    app.get("/bookings/:bookingId", async (request, response) => {
        const { bookingId } = request.params;

        if (!bookingId || !isValidBookingId(bookingId)) {
            response.status(400).json({ error: new InvalidBookingIdError().message });
            return;
        }

        try {
            const booking = await getBookingById(bookingId);

            if (!booking) {
                response.status(404).json({ error: "Booking not found" });
                return;
            }

            response.status(200).json({
                id: booking.id,
                userId: booking.userId,
                eventId: booking.eventId,
                seatNos: booking.seatNos,
                status: booking.status,
                createdAt: booking.createdAt,
                updatedAt: booking.updatedAt,
            });
        } catch (error) {
            console.error("Failed to retrieve booking:", { bookingId, error });
            response.status(500).json({ error: "Failed to retrieve booking" });
        }
    });

    app.get("/bookings/:bookingId/saga", async (request, response) => {
        const { bookingId } = request.params;

        if (!bookingId || !isValidBookingId(bookingId)) {
            response.status(400).json({ error: new InvalidBookingIdError().message });
            return;
        }

        try {
            const booking = await getBookingById(bookingId);
            if (!booking) {
                response.status(404).json({ error: "Booking not found" });
                return;
            }

            const history = await getSagaLog(pool, bookingId);
            response.status(200).json({
                bookingId,
                status: booking.status,
                history,
            });
        } catch (error) {
            console.error("Failed to retrieve saga log:", { bookingId, error });
            response.status(500).json({ error: "Failed to retrieve saga log" });
        }
    });

    app.post("/bookings/:bookingId/pay", async (request, response) => {
        const { bookingId } = request.params;

        if (!bookingId || !isValidBookingId(bookingId)) {
            response.status(400).json({ error: new InvalidBookingIdError().message });
            return;
        }

        try {
            const body = request.body as { amount?: number; currency?: string } | undefined;
            const amount = typeof body?.amount === "number" ? body.amount : undefined;
            const currency = typeof body?.currency === "string" ? body.currency : undefined;

            const result = await payBooking(bookingId, {
                ...(amount !== undefined ? { amount } : {}),
                ...(currency !== undefined ? { currency } : {}),
            });

            if (result.duplicate) {
                response.status(200).json({
                    bookingId: result.bookingId,
                    status: result.status,
                    duplicate: true,
                });
                return;
            }

            if (onBookingCreated) {
                try {
                    await onBookingCreated(result.bookingId);
                } catch (error) {
                    console.warn("Outbox relay trigger notification failed:", error);
                }
            }

            response.status(202).json({
                bookingId: result.bookingId,
                status: result.status,
            });
        } catch (error) {
            if (error instanceof BookingNotFoundError) {
                response.status(404).json({ error: error.message });
                return;
            }

            if (error instanceof InvalidBookingStateError) {
                response.status(409).json({ error: error.message });
                return;
            }

            console.error("Failed to pay booking:", { bookingId, error });
            response.status(500).json({ error: "Failed to process payment request" });
        }
    });

    app.post("/bookings", async (request, response) => {
        const idempotencyKey = request.header("Idempotency-Key")?.trim();

        if (!idempotencyKey) {
            response.status(400).json({ error: "Idempotency-Key header is required" });
            return;
        }

        let bookingRequest;
        try {
            bookingRequest = normalizeBookingRequest(request.body);
        } catch (error) {
            if (error instanceof InvalidBookingRequestError) {
                response.status(400).json({ error: error.message });
                return;
            }
            throw error;
        }

        try {
            const result = await createOrGetBooking(bookingRequest, idempotencyKey);

            if (!result.created) {
                response.status(200).json({
                    bookingId: result.booking.id,
                    status: result.booking.status,
                });
                return;
            }

            if (onBookingCreated) {
                try {
                    await onBookingCreated(result.booking.id);
                } catch (error) {
                    console.warn("Outbox relay trigger notification failed:", error);
                }
            }

            response.status(202).json({
                bookingId: result.booking.id,
                status: result.booking.status,
            });
        } catch (error) {
            if (error instanceof IdempotencyConflictError) {
                response.status(409).json({ error: error.message });
                return;
            }

            console.error("Failed to create booking:", error);
            response.status(500).json({ error: "Failed to create booking" });
        }
    });

    return app;
}
