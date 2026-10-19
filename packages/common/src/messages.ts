export interface ReserveSeatsCommand {
    messageId: string;
    bookingId: string;
    type: "ReserveSeats";
    occurredAt: string;
    payload: {
        eventId: string;
        seatNos: string[];
    };
}

export interface ConfirmSeatsCommand {
    messageId: string;
    bookingId: string;
    type: "ConfirmSeats";
    occurredAt: string;
    payload: {
        eventId: string;
        seatNos: string[];
    };
}

export interface ReleaseSeatsCommand {
    messageId: string;
    bookingId: string;
    type: "ReleaseSeats";
    occurredAt: string;
    payload: {
        eventId: string;
        seatNos: string[];
    };
}

export interface ProcessPaymentCommand {
    messageId: string;
    bookingId: string;
    type: "ProcessPayment";
    occurredAt: string;
    payload: {
        amount: number;
        currency?: string;
    };
}

export interface SeatsReservedEvent {
    messageId: string;
    bookingId: string;
    type: "SeatsReserved";
    occurredAt: string;
    payload: {
        eventId: string;
        seatNos: string[];
    };
}

export interface SeatsReservationFailedEvent {
    messageId: string;
    bookingId: string;
    type: "SeatsReservationFailed";
    occurredAt: string;
    payload: {
        eventId: string;
        seatNos: string[];
        reason: "SEAT_UNAVAILABLE";
    };
}

export interface SeatsConfirmedEvent {
    messageId: string;
    bookingId: string;
    type: "SeatsConfirmed";
    occurredAt: string;
    payload: {
        eventId: string;
        seatNos: string[];
    };
}

export interface SeatsReleasedEvent {
    messageId: string;
    bookingId: string;
    type: "SeatsReleased";
    occurredAt: string;
    payload: {
        eventId: string;
        seatNos: string[];
    };
}

export interface PaymentSucceededEvent {
    messageId: string;
    bookingId: string;
    type: "PaymentSucceeded";
    occurredAt: string;
    payload: {
        paymentId: string;
        amount: number;
    };
}

export interface PaymentFailedEvent {
    messageId: string;
    bookingId: string;
    type: "PaymentFailed";
    occurredAt: string;
    payload: {
        reason: string;
    };
}

export interface BookingConfirmedEvent {
    messageId: string;
    bookingId: string;
    type: "BookingConfirmed";
    occurredAt: string;
    payload: {
        userId: string;
        eventId: string;
        seatNos: string[];
    };
}

export interface BookingCancelledEvent {
    messageId: string;
    bookingId: string;
    type: "BookingCancelled";
    occurredAt: string;
    payload: {
        reason: string;
    };
}

export interface HoldExpiredEvent {
    messageId: string;
    bookingId: string;
    type: "HoldExpired";
    occurredAt: string;
    payload: {
        eventId: string;
        seatNos: string[];
    };
}
