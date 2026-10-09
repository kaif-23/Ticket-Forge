import express from "express";
import { Kafka } from "kafkajs";
import type { ReserveSeatsCommand } from "../../../packages/common/src/messages.js";
const app = express();
const PORT = 3001;

app.use(express.json());

const kafka = new Kafka({
    clientId: "ticketforge-booking",
    brokers: ["localhost:9092"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({
    groupId: "booking-service-group",
});
app.get("/health", (_req, res) => {
    res.status(200).json({
        service: "booking",
        status: "ok",
    });
});

app.post("/test/reserve-seats", async (_req, res) => {
    const command :ReserveSeatsCommand = {
        type: "ReserveSeats",
        bookingId: "booking-123",
        eventId: "event-1",
        seatNos: ["A1", "A2"],
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

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const value = message.value?.toString();

            if (!value) return;

            console.log("Received inventory event:", {
                topic,
                partition,
                offset: message.offset,
                value: JSON.parse(value),
            });
        },
    });

    app.listen(PORT, () => {
        console.log(`Booking Service running on port ${PORT}`);
    });
}


main().catch(async (error) => {
    console.error("Booking Service startup error:", error);
    await producer.disconnect();
    process.exit(1);
});
