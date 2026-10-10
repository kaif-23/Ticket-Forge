import { Kafka } from "kafkajs";
import type {
    ReserveSeatsCommand,
    SeatsReservationFailedEvent,
    SeatsReservedEvent,
} from "../../../packages/common/src/messages.js";
import { z } from "zod";
import { pool } from "./db.js";
import { reserveSeats } from "./reserveSeats.js";

const reserveSeatsCommandSchema = z.object({
    type: z.literal("ReserveSeats"),
    bookingId: z.string().min(1),
    eventId: z.string().min(1),
    seatNos: z.array(z.string().min(1)).min(1),
});

const kafka = new Kafka({
    clientId: "ticketforge-inventory",
    brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
    groupId: "inventory-service-group",
});

const producer = kafka.producer();
const result = await pool.query("SELECT NOW() AS current_time");
console.log("PostgreSQL connected:", result.rows[0]);
async function main() {
    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({
        topic: "inventory.commands",
        fromBeginning: true,
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

            const result = reserveSeatsCommandSchema.safeParse(parsedJson);

            if (!result.success) {
                console.error("Invalid ReserveSeats command:", result.error.issues);
                return;
            }

            const command: ReserveSeatsCommand = result.data;

            if (command.type !== "ReserveSeats") {
                console.log("Ignoring unsupported command:", command.type);
                return;
            }

            try {
                const reservation = await reserveSeats(
                    command.bookingId,
                    command.eventId,
                    command.seatNos,
                );

                if (!reservation.success) {
                    const event: SeatsReservationFailedEvent = {
                        type: "SeatsReservationFailed",
                        bookingId: command.bookingId,
                        eventId: command.eventId,
                        seatNos: command.seatNos,
                        reason: reservation.reason,
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
                        eventId: command.eventId,
                        seatNos: command.seatNos,
                        result: reservation.reason,
                    });
                    return;
                }

                const event: SeatsReservedEvent = {
                    type: "SeatsReserved",
                    bookingId: command.bookingId,
                    eventId: command.eventId,
                    seatNos: command.seatNos,
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
                    eventId: command.eventId,
                    seatNos: command.seatNos,
                    result: "RESERVED",
                });
            } catch (error) {
                console.error("Inventory reservation processing failed:", {
                    bookingId: command.bookingId,
                    eventId: command.eventId,
                    seatNos: command.seatNos,
                    error,
                });
                throw error;
            }
        },
    });
}

main().catch(async (error) => {
    console.error("Inventory Service error:", error);
    await consumer.disconnect();
    await producer.disconnect();
    process.exit(1);
});
