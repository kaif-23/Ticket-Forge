import type { Server } from "node:http";
import { Kafka } from "kafkajs";
import {
    applyHoldExpired,
    applyInventoryOutcome,
    applyPaymentOutcome,
    applySeatsConfirmed,
    InvalidInventoryEventError,
    InvalidPaymentEventError,
} from "./bookingEvents.js";
import { createBookingApp } from "./bookingApi.js";
import { pool } from "./db.js";
import { OutboxRelay } from "./outboxRelay.js";

const PORT = Number(process.env.BOOKING_HTTP_PORT ?? 3001);

const kafka = new Kafka({
    clientId: "ticketforge-booking",
    brokers: ["localhost:9092"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({
    groupId: "booking-service-group",
});

const relay = new OutboxRelay(pool, producer, {
    pollIntervalMs: Number(process.env.OUTBOX_POLL_INTERVAL_MS ?? 1000),
});

const app = createBookingApp(async () => {
    relay.trigger();
});

let server: Server | null = null;
let isShuttingDown = false;

async function shutdown(signal: string) {
    if (isShuttingDown) return;
    isShuttingDown = true;
    console.log(`Received ${signal}, shutting down Booking service gracefully...`);

    try {
        await relay.stop();
        if (server) {
            await new Promise<void>((resolve, reject) => {
                server!.close((err) => (err ? reject(err) : resolve()));
            });
        }
        await consumer.disconnect();
        await producer.disconnect();
        await pool.end();
        console.log("Booking service shut down cleanly.");
        process.exit(0);
    } catch (error) {
        console.error("Error during graceful shutdown:", error);
        process.exit(1);
    }
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

async function main() {
    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({
        topics: ["inventory.events", "payment.events"],
        fromBeginning: true,
    });

    relay.start();

    server = app.listen(PORT, () => {
        console.log(`Booking Service running on port ${PORT}`);
    });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const value = message.value?.toString();

            if (!value) return;

            let event: Record<string, unknown>;
            try {
                event = JSON.parse(value);
            } catch {
                console.error("Invalid event JSON; skipping message", {
                    topic,
                    partition,
                    offset: message.offset,
                });
                return;
            }

            try {
                if (topic === "payment.events") {
                    const outcome = await applyPaymentOutcome(event, "booking-service-group", topic);
                    if (outcome.kind === "updated") {
                        console.log("Persisted Payment outcome:", outcome);
                        relay.trigger();
                    } else if (outcome.kind === "duplicate") {
                        console.log("Ignored duplicate Payment outcome message:", outcome);
                    } else if (outcome.kind === "unknown") {
                        console.warn("Payment event references unknown booking:", outcome);
                    } else {
                        console.warn("Ignored stale Payment outcome:", outcome);
                    }
                    return;
                }

                if (topic === "inventory.events") {
                    if (event.type === "SeatsConfirmed") {
                        const outcome = await applySeatsConfirmed(event, "booking-service-group", topic);
                        if (outcome.kind === "updated") {
                            console.log("Persisted SeatsConfirmed outcome:", outcome);
                            relay.trigger();
                        } else if (outcome.kind === "duplicate") {
                            console.log("Ignored duplicate SeatsConfirmed message:", outcome);
                        } else if (outcome.kind === "unknown") {
                            console.warn("SeatsConfirmed event references unknown booking:", outcome);
                        } else {
                            console.warn("Ignored stale SeatsConfirmed outcome:", outcome);
                        }
                    } else if (
                        event.type === "SeatsReserved" ||
                        event.type === "SeatsReservationFailed"
                    ) {
                        const outcome = await applyInventoryOutcome(event, "booking-service-group", topic);
                        if (outcome.kind === "updated") {
                            console.log("Persisted Inventory outcome:", outcome);
                            relay.trigger();
                        } else if (outcome.kind === "duplicate") {
                            console.log("Ignored duplicate Inventory outcome message:", outcome);
                        } else if (outcome.kind === "unknown") {
                            console.warn("Inventory event references unknown booking:", outcome);
                        } else {
                            console.warn("Ignored stale Inventory outcome:", outcome);
                        }
                    } else if (event.type === "HoldExpired") {
                        const outcome = await applyHoldExpired(event, "booking-service-group", topic);
                        if (outcome.kind === "updated") {
                            console.log("Persisted HoldExpired outcome:", outcome);
                        } else if (outcome.kind === "duplicate") {
                            console.log("Ignored duplicate HoldExpired message:", outcome);
                        } else if (outcome.kind === "unknown") {
                            console.warn("HoldExpired event references unknown booking:", outcome);
                        } else {
                            console.warn("Ignored stale HoldExpired outcome:", outcome);
                        }
                    } else if (event.type === "SeatsReleased") {
                        console.log("Acknowledged SeatsReleased compensation event for booking:", event.bookingId);
                    } else {
                        console.warn("Ignoring unrecognized inventory event:", event.type);
                    }
                }
            } catch (error) {
                if (
                    error instanceof InvalidInventoryEventError ||
                    error instanceof InvalidPaymentEventError
                ) {
                    console.error("Invalid domain event; skipping message", {
                        topic,
                        partition,
                        offset: message.offset,
                        error: error.message,
                    });
                    return;
                }

                console.error("Failed to persist domain event outcome:", {
                    topic,
                    partition,
                    offset: message.offset,
                    error,
                });
                throw error;
            }
        },
    });
}

main().catch(async (error) => {
    console.error("Booking Service startup error:", error);
    try {
        await relay.stop();
        await consumer.disconnect();
        await producer.disconnect();
        await pool.end();
    } catch {
        // Ignore cleanup errors during fatal startup failure
    }
    process.exit(1);
});
