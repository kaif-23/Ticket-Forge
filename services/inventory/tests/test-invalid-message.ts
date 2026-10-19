
import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: "ticketforge-validation-test",
    brokers: ["localhost:9092"],
});

const producer = kafka.producer();

async function main() {
    await producer.connect();

    const invalidCommand = {
        messageId: "msg-invalid",
        occurredAt: "2026-10-10T00:00:00Z",
        type: "ReserveSeats",
        bookingId: 123,
        payload: {
            eventId: "event-1",
            seatNos: "A1",
        }
    };

    await producer.send({
        topic: "inventory.commands",
        messages: [
            {
                key: "validation-test",
                value: JSON.stringify(invalidCommand),
            },
        ],
    });

    console.log("Invalid test message published.");

    await producer.disconnect();
}

main().catch(async (error) => {
    console.error("Test producer failed:", error);
    await producer.disconnect();
    process.exitCode = 1;
});
