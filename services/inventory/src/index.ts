
import { Kafka } from "kafkajs";
import type {
    ReserveSeatsCommand,SeatsReservedEvent,} from "../../../packages/common/src/messages.js";
const kafka = new Kafka({
    clientId: "ticketforge-inventory",
    brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
    groupId: "inventory-service-group",
});

const producer = kafka.producer();

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

            const command: ReserveSeatsCommand = JSON.parse(rawValue);

            if (command.type !== "ReserveSeats") {
                console.log("Ignoring unsupported command:", command.type);
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

            console.log("Published inventory event:", event);
        },
    });
}

main().catch(async (error) => {
    console.error("Inventory Service error:", error);
    await consumer.disconnect();
    await producer.disconnect();
    process.exit(1);
});
