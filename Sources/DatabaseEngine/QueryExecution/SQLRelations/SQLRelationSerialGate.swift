/// Serializes suspension across actor reentrancy without holding a memory lock.
actor SQLRelationSerialGate {
    private final class Waiter {
        let continuation: CheckedContinuation<Void, Never>
        let reservation: DatabaseIntermediateReservation?
        var next: Waiter?
        init(_ continuation: CheckedContinuation<Void, Never>, reservation: DatabaseIntermediateReservation?) {
            self.continuation = continuation; self.reservation = reservation
        }
    }
    private var held = false
    private var first: Waiter?
    private var last: Waiter?

    func acquire(meter: DatabaseWorkMeter) async throws {
        if !held { held = true; return }
        let reservation = try meter.reserveIntermediate(bytes: 256, at: .bindingCandidate)
        await enqueue(reservation: reservation)
    }

    // Owner cleanup is already admitted with the producer and must also run after cancellation.
    func acquire() async {
        if !held { held = true; return }
        await enqueue(reservation: nil)
    }

    private func enqueue(reservation: DatabaseIntermediateReservation?) async {
        await withCheckedContinuation { continuation in
            let waiter = Waiter(continuation, reservation: reservation)
            if let last { last.next = waiter } else { first = waiter }
            last = waiter
        }
    }

    func release() {
        guard let waiter = first else { held = false; return }
        first = waiter.next
        if first == nil { last = nil }
        waiter.next = nil
        waiter.reservation?.release()
        waiter.continuation.resume()
    }
}

