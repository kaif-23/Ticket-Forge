import { randomUUID } from "node:crypto";
import { Kafka } from "kafkajs";
import type {
    ConfirmSeatsCommand,
    ReleaseSeatsCommand,
    ReserveSeatsCommand,
    SeatsConfirmedEvent,
    SeatsReleasedEvent,
    SeatsReservationFailedEvent,
    SeatsReservedEvent,
} from "../../../packages/common/src/messages.js";
import { z } from "zod";
import { pool } from "./db.js";
import { reserveSeats } from "./reserveSeats.js";
import { confirmSeats } from "./confirmSeats.js";
import { releaseSeats } from "./releaseSeats.js";
import { availabilityApp } from "./availabilityApi.js";
import { ExpirySweeper } from "./expirySweeper.js";
import { recordProcessedMessage } from "./idempotency.js";
import { closeRedis } from "./redis.js";

const HTTP_PORT = Number(process.env.INVENTORY_HTTP_PORT ?? 3002);

const inventoryCommandSchema = z.object({
    messageId: z.string().min(1),
    bookingId: z.string().min(1),
    type: z.enum(["ReserveSeats", "ConfirmSeats", "ReleaseSeats"]),
    occurredAt: z.string().min(1),
    payload: z.object({
        eventId: z.string().min(1),
        seatNos: z.array(z.string().min(1)).min(1),
    }),
});

const kafka = new Kafka({
    clientId: "ticketforge-inventory",
    brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
    groupId: "inventory-service-group",
});

const producer = kafka.producer();
const sweeper = new ExpirySweeper(pool, producer, {
    sweepIntervalMs: Number(process.env.EXPIRY_SWEEPER_INTERVAL_MS ?? 1000),
});

const result = await pool.query("SELECT NOW() AS current_time");
console.log("PostgreSQL connected:", result.rows[0]);
async function main() {
    await producer.connect();
    await consumer.connect();

    sweeper.start();

    await consumer.subscribe({
        topic: "inventory.commands",
        fromBeginning: true,
    });

    availabilityApp.listen(HTTP_PORT, () => {
        console.log(`Inventory HTTP API listening on port ${HTTP_PORT}`);
    });

    console.log("Inventory Service listening for commands...");

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const rawValue = message.value?.toString();

            if (!rawValue) {
                console.warn("Received empty command; skipping");
                return;
            }

            console.log("Received Kafka command:", {
                topic,
                partition,
                offset: message.offset,
                value: rawValue,
            });

            let parsedJson: unknown;

            try {
                parsedJson = JSON.parse(rawValue);
            } catch {
                console.error("Invalid JSON received; skipping message");
                return;
            }

            const result = inventoryCommandSchema.safeParse(parsedJson);

            if (!result.success) {
                console.error("Invalid inventory command:", result.error.issues);
                return;
            }

            const command = result.data;

            const client = await pool.connect();
            try {
                await client.query("BEGIN");

                const { isDuplicate } = await recordProcessedMessage(
                    client,
                    command.messageId,
                    "inventory-service-group",
                    topic,
                );

                if (isDuplicate) {
                    await client.query("ROLLBACK");
                    console.log("Ignored duplicate inventory command:", {
                        messageId: command.messageId,
                        bookingId: command.bookingId,
                        type: command.type,
                    });
                    return;
                }

                if (command.type === "ReserveSeats") {
                    const reservation = await reserveSeats(
                        command.bookingId,
                        command.payload.eventId,
                        command.payload.seatNos,
                        client,
                    );

                    await client.query("COMMIT");

                    if (!reservation.success) {
                        const event: SeatsReservationFailedEvent = {
                            messageId: randomUUID(),
                            bookingId: command.bookingId,
                            type: "SeatsReservationFailed",
                            occurredAt: new Date().toISOString(),
                            payload: {
                                eventId: command.payload.eventId,
                                seatNos: command.payload.seatNos,
                                reason: reservation.reason as "SEAT_UNAVAILABLE",
                            },
                        };

                        await producer.send({
                            topic: "inventory.events",
                            messages: [
                                {
                                    key: command.bookingId,
                                    value: JSON.stringify(event),
                                },
                            ],
                        });

                        console.log("Published inventory reservation result:", {
                            bookingId: command.bookingId,
                            eventId: command.payload.eventId,
                            seatNos: command.payload.seatNos,
                            result: reservation.reason,
                        });
                        return;
                    }

                    const event: SeatsReservedEvent = {
                        messageId: randomUUID(),
                        bookingId: command.bookingId,
                        type: "SeatsReserved",
                        occurredAt: new Date().toISOString(),
                        payload: {
                            eventId: command.payload.eventId,
                            seatNos: command.payload.seatNos,
                        },
                    };

                    await producer.send({
                        topic: "inventory.events",
                        messages: [
                            {
                                key: command.bookingId,
                                value: JSON.stringify(event),
                            },
                        ],
                    });

                    console.log("Published inventory reservation result:", {
                        bookingId: command.bookingId,
                        eventId: command.payload.eventId,
                        seatNos: command.payload.seatNos,
                        result: "RESERVED",
                    });
                    return;
                }

                if (command.type === "ConfirmSeats") {
                    const confirmation = await confirmSeats(
                        command.bookingId,
                        command.payload.eventId,
                        command.payload.seatNos,
                        client,
                    );

                    if (!confirmation.success) {
                        await client.query("ROLLBACK");
                        console.error("Seats confirmation failed in inventory:", {
                            bookingId: command.bookingId,
                            reason: confirmation.reason,
                        });
                        return;
                    }

                    await client.query("COMMIT");

                    const event: SeatsConfirmedEvent = {
                        messageId: randomUUID(),
                        bookingId: command.bookingId,
                        type: "SeatsConfirmed",
                        occurredAt: new Date().toISOString(),
                        payload: {
                            eventId: command.payload.eventId,
                            seatNos: command.payload.seatNos,
                        },
                    };

                    await producer.send({
                        topic: "inventory.events",
                        messages: [
                            {
                                key: command.bookingId,
                                value: JSON.stringify(event),
                            },
                        ],
                    });

                    console.log("Published inventory confirmation result:", {
                        bookingId: command.bookingId,
                        result: "CONFIRMED",
                    });
                    return;
                }

                if (command.type === "ReleaseSeats") {
                    const release = await releaseSeats(
                        command.bookingId,
                        command.payload.eventId,
                        command.payload.seatNos,
                        client,
                    );

                    if (!release.success) {
                        await client.query("ROLLBACK");
                        console.error("Seats release failed in inventory:", {
                            bookingId: command.bookingId,
                            reason: release.reason,
                        });
                        return;
                    }

                    await client.query("COMMIT");

                    const event: SeatsReleasedEvent = {
                        messageId: randomUUID(),
                        bookingId: command.bookingId,
                        type: "SeatsReleased",
                        occurredAt: new Date().toISOString(),
                        payload: {
                            eventId: command.payload.eventId,
                            seatNos: command.payload.seatNos,
                        },
                    };

                    await producer.send({
                        topic: "inventory.events",
                        messages: [
                            {
                                key: command.bookingId,
                                value: JSON.stringify(event),
                            },
                        ],
                    });

                    console.log("Published inventory release result:", {
                        bookingId: command.bookingId,
                        result: "RELEASED",
                    });
                    return;
                }
            } catch (error) {
                try {
                    await client.query("ROLLBACK");
                } catch {
                    // ignore rollback error
                }
                console.error("Inventory command processing failed:", {
                    bookingId: command.bookingId,
                    commandType: command.type,
                    error,
                });
                throw error;
            } finally {
                client.release();
            }
        },
    });
}

main().catch(async (error) => {
    console.error("Inventory Service error:", error);
    try {
        await sweeper.stop();
        await consumer.disconnect();
        await producer.disconnect();
        await closeRedis();
    } catch {
        // ignore errors during fatal shutdown
    }
    process.exit(1);
});

const shutdown = async (signal: string) => {
    console.log(`Received ${signal}, shutting down Inventory service...`);
    try {
        await sweeper.stop();
        await consumer.disconnect();
        await producer.disconnect();
        await closeRedis();
    } catch (err) {
        console.error("Error during inventory shutdown:", err);
    }
    process.exit(0);
};

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
