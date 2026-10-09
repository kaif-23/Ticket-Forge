
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
