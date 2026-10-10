import express from "express";
import { getSeatAvailability, InvalidEventIdError } from "./availability.js";
import {
    createEvent,
    EventAlreadyExistsError,
    EventNotFoundError,
    getEventById,
    InvalidEventError,
    listEvents,
} from "./events.js";

export const availabilityApp = express();
availabilityApp.use(express.json());

availabilityApp.get("/events", async (_request, response) => {
    try {
        const events = await listEvents();
        response.status(200).json(events);
    } catch (error) {
        console.error("Failed to list events:", error);
        response.status(500).json({ error: "Failed to list events" });
    }
});

availabilityApp.get("/events/:eventId", async (request, response) => {
    const { eventId } = request.params;

    try {
        const event = await getEventById(eventId);
        if (!event) {
            response.status(404).json({ error: "Event not found" });
            return;
        }
        response.status(200).json(event);
    } catch (error) {
        if (error instanceof InvalidEventError) {
            response.status(400).json({ error: error.message });
            return;
        }
        console.error("Failed to retrieve event:", { eventId, error });
        response.status(500).json({ error: "Failed to retrieve event" });
    }
});

// This endpoint is intentionally unauthenticated until the project defines admin identity.
availabilityApp.post("/admin/events", async (request, response) => {
    try {
        const event = await createEvent(request.body);
        response.status(201).json(event);
    } catch (error) {
        if (error instanceof InvalidEventError) {
            response.status(400).json({ error: error.message });
            return;
        }
        if (error instanceof EventAlreadyExistsError) {
            response.status(409).json({ error: error.message });
            return;
        }
        console.error("Failed to create event:", error);
        response.status(500).json({ error: "Failed to create event" });
    }
});

availabilityApp.get("/events/:eventId/seats", async (request, response) => {
    const { eventId } = request.params;

    try {
        const seats = await getSeatAvailability(eventId);
        response.status(200).json({ eventId, seats });
    } catch (error) {
        if (error instanceof InvalidEventIdError) {
            response.status(400).json({ error: error.message });
            return;
        }
        if (error instanceof EventNotFoundError) {
            response.status(404).json({ error: error.message });
            return;
        }

        console.error("Failed to retrieve seat availability:", { eventId, error });
        response.status(500).json({ error: "Failed to retrieve seat availability" });
    }
});
