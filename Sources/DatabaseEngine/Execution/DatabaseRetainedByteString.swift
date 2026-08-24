import DatabaseTypes

/// Couples byte ownership to an already-admitted request reservation.
/// Unknown or enclosing owners are copied into an exact-size allocation only
/// after admission; exact self-contained owners are wrapped without copying.
package enum DatabaseRetainedByteString {
    package static func make(
        _ bytes: ByteString,
        reservation: DatabaseIntermediateReservation,
        at stage: DatabaseWorkStage
    ) throws -> ByteString {
        if bytes.isStorageSelfContained,
           bytes.retainedByteCount == bytes.count {
            return ByteString(
                retaining: LeasedOwner(
                    bytes: bytes,
                    reservation: reservation
                )
            )
        }
        return try copying(
            bytes,
            reservation: reservation,
            at: stage
        )
    }

    /// Detaches bytes at an ownership-transfer boundary even when the source
    /// already has exact storage. This prevents the destination reservation
    /// from retaining an upstream request-scoped owner and its reservation.
    package static func copying(
        _ bytes: ByteString,
        reservation: DatabaseIntermediateReservation,
        at stage: DatabaseWorkStage
    ) throws -> ByteString {
        try DatabaseByteProcessingMeter.consume(
            byteCount: bytes.count,
            workMeter: reservation.workMeter,
            stage: stage
        )
        return ByteString(
            retaining: ExactCopyOwner(
                copying: bytes,
                reservation: reservation
            )
        )
    }

    /// Owns one exact-size immutable byte allocation. The reservation is
    /// claimed before init and remains retained for the owner's lifetime. A
    /// nonempty allocation is initialized over its complete `count` bytes,
    /// uses UInt8 alignment, is never bound or mutated afterward, and is
    /// deallocated exactly once. The pointer is exposed only to synchronous
    /// borrows and cannot escape. These invariants make concurrent immutable
    /// borrows safe despite the raw pointer requiring unchecked Sendable.
    private final class ExactCopyOwner:
        ByteStringOwner,
        @unchecked Sendable
    {
        let count: Int
        let retainedByteCount: Int?
        let isStorageSelfContained = true
        private let allocation: UnsafeMutableRawPointer?
        private let reservation: DatabaseIntermediateReservation

        init(
            copying bytes: ByteString,
            reservation: DatabaseIntermediateReservation
        ) {
            self.count = bytes.count
            self.retainedByteCount = bytes.count
            self.reservation = reservation
            guard !bytes.isEmpty else {
                self.allocation = nil
                return
            }
            let allocation = UnsafeMutableRawPointer.allocate(
                byteCount: bytes.count,
                alignment: MemoryLayout<UInt8>.alignment
            )
            bytes.withUnsafeBytes { source in
                UnsafeMutableRawBufferPointer(
                    start: allocation,
                    count: bytes.count
                ).copyMemory(from: source)
            }
            self.allocation = allocation
        }

        deinit {
            allocation?.deallocate()
        }

        func borrowBytes(
            _ body: (UnsafeRawBufferPointer) throws -> Void
        ) rethrows {
            try body(
                UnsafeRawBufferPointer(start: allocation, count: count)
            )
        }
    }

    private final class LeasedOwner: ByteStringOwner {
        let count: Int
        let retainedByteCount: Int?
        let isStorageSelfContained = true
        private let bytes: ByteString
        private let reservation: DatabaseIntermediateReservation

        init(
            bytes: ByteString,
            reservation: DatabaseIntermediateReservation
        ) {
            precondition(bytes.retainedByteCount == bytes.count)
            self.count = bytes.count
            self.retainedByteCount = bytes.count
            self.bytes = bytes
            self.reservation = reservation
        }

        func borrowBytes(
            _ body: (UnsafeRawBufferPointer) throws -> Void
        ) rethrows {
            try bytes.withUnsafeBytes(body)
        }
    }
}
