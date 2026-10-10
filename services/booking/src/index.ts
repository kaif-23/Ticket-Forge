import { Kafka } from "kafkajs";
import type { ReserveSeatsCommand } from "../../../packages/common/src/messages.js";
import {
    applyInventoryOutcome,
    InvalidInventoryEventError,
} from "./bookingEvents.js";
import { createBookingApp } from "./bookingApi.js";

const PORT = Number(process.env.BOOKING_HTTP_PORT ?? 3001);

const kafka = new Kafka({
    clientId: "ticketforge-booking",
    brokers: ["localhost:9092"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({
    groupId: "booking-service-group",
});

const app = createBookingApp(async (command) => {
    await producer.send({
        topic: "inventory.commands",
        messages: [
            {
                key: command.bookingId,
                value: JSON.stringify(command),
            },
        ],
    });
});

app.post("/test/reserve-seats", async (_req, res) => {
    const command: ReserveSeatsCommand = {
        type: "ReserveSeats",
        bookingId: "booking-123",
        eventId: "event-1",
        seatNos: ["A1", "A3"],
    };

    try {
        await producer.send({
            topic: "inventory.commands",
            messages: [
                {
                    key: command.bookingId,
                    value: JSON.stringify(command),
                },
            ],
        });

        res.status(202).json({
            message: "ReserveSeats command published",
            command,
        });
    } catch (error) {
        console.error("Failed to publish command:", error);

        res.status(500).json({
            error: "Failed to publish command",
        });
    }
});


async function main() {
    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({
        topic: "inventory.events",
        fromBeginning: true,
    });

    app.listen(PORT, () => {
        console.log(`Booking Service running on port ${PORT}`);
    });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const value = message.value?.toString();

            if (!value) return;

            let event: unknown;
            try {
                event = JSON.parse(value);
            } catch {
                console.error("Invalid Inventory event JSON; skipping message", {
                    topic,
                    partition,
                    offset: message.offset,
                });
                return;
            }

            try {
                const outcome = await applyInventoryOutcome(event);

                if (outcome.kind === "updated") {
                    console.log("Persisted Inventory outcome:", outcome);
                } else if (outcome.kind === "unknown") {
                    console.warn("Inventory event references an unknown booking:", outcome);
                } else {
                    console.warn("Ignored stale Inventory outcome:", outcome);
                }
            } catch (error) {
                if (error instanceof InvalidInventoryEventError) {
                    console.error("Invalid Inventory event; skipping message", {
                        topic,
                        partition,
                        offset: message.offset,
                        error: error.message,
                    });
                    return;
                }

                console.error("Failed to persist Inventory outcome:", {
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
    await producer.disconnect();
    process.exit(1);
});
