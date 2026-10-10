
export interface ReserveSeatsCommand {
    type: "ReserveSeats";
    bookingId: string;
    eventId: string;
    seatNos: string[];
}

export interface SeatsReservedEvent {
    type: "SeatsReserved";
    bookingId: string;
    eventId: string;
    seatNos: string[];
}

export interface SeatsReservationFailedEvent {
    type: "SeatsReservationFailed";
    bookingId: string;
    eventId: string;
    seatNos: string[];
    reason: "SEAT_UNAVAILABLE";
}
