import express from "express";
import type { ReserveSeatsCommand } from "../../../packages/common/src/messages.js";
import {
    createOrGetBooking,
    getBookingById,
    IdempotencyConflictError,
    InvalidBookingIdError,
    InvalidBookingRequestError,
    isValidBookingId,
    normalizeBookingRequest,
} from "./booking.js";

export type ReserveSeatsPublisher = (command: ReserveSeatsCommand) => Promise<void>;

export function createBookingApp(publishReserveSeats: ReserveSeatsPublisher) {
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

            const command: ReserveSeatsCommand = {
                type: "ReserveSeats",
                bookingId: result.booking.id,
                eventId: result.booking.eventId,
                seatNos: result.booking.seatNos,
            };

            try {
                await publishReserveSeats(command);
            } catch (error) {
                console.error("Booking persisted but ReserveSeats publication failed:", {
                    bookingId: result.booking.id,
                    error,
                });
                response.status(503).json({
                    error: "Booking persisted but seat reservation command publication failed",
                    bookingId: result.booking.id,
                    status: result.booking.status,
                });
                return;
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
