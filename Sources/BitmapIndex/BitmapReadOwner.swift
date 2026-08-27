import DatabaseEngine

/// Linear ownership for one decoded bitmap and its request-memory claim.
///
/// The bitmap is intentionally noncopyable. Set operators consume their input
/// owners and return one owner for the output, so releasing a reservation can
/// never leave a RoaringBitmap alias outside the claim that admitted it.
package struct BitmapReadOwner: ~Copyable, Sendable {
    private let value: RoaringBitmap
    private let reservation: DatabaseIntermediateReservation

    package init(
        value: consuming RoaringBitmap,
        reservation: DatabaseIntermediateReservation
    ) {
        self.value = value
        self.reservation = reservation
    }

    package static func empty(
        workMeter: DatabaseWorkMeter
    ) throws -> BitmapReadOwner {
        let value = RoaringBitmap()
        let reservation = try workMeter.reserveIntermediate(
            bytes: try value.retainedStorageByteCount(),
            at: .indexScan
        )
        return BitmapReadOwner(value: value, reservation: reservation)
    }

    package var cardinality: Int { value.cardinality }
    package var workMeter: DatabaseWorkMeter { reservation.workMeter }

    package borrowing func withValue<Result, Failure: Error>(
        _ body: (borrowing RoaringBitmap) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        let result = try body(value)
        withExtendedLifetime(reservation) {}
        return result
    }

    /// Consumes this owner at an explicit public output boundary.
    package consuming func promoteToOutput() -> RoaringBitmap {
        reservation.release()
        return value
    }

    package consuming func intersect(
        with other: consuming BitmapReadOwner
    ) throws -> BitmapReadOwner {
        let upperBound = try value.unionRetainedStorageUpperBound(
            with: other.value
        )
        let scratchByteCount = try value.intersectionScratchByteCount(
            with: other.value
        )
        return try combine(
            with: consume other,
            upperBound: upperBound,
            scratchByteCount: scratchByteCount,
            operation: { $0 && $1 }
        )
    }

    package consuming func union(
        with other: consuming BitmapReadOwner
    ) throws -> BitmapReadOwner {
        let upperBound = try value.unionRetainedStorageUpperBound(
            with: other.value
        )
        let scratchByteCount = try value.unionScratchByteCount(
            with: other.value
        )
        return try combine(
            with: consume other,
            upperBound: upperBound,
            scratchByteCount: scratchByteCount,
            operation: { $0 || $1 }
        )
    }

    private consuming func combine(
        with other: consuming BitmapReadOwner,
        upperBound: UInt64,
        scratchByteCount: UInt64,
        operation: (RoaringBitmap, RoaringBitmap) -> RoaringBitmap
    ) throws -> BitmapReadOwner {
        guard workMeter === other.workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        let (maximumBytes, overflow) = upperBound.addingReportingOverflow(
            scratchByteCount
        )
        guard !overflow else {
            throw RoaringBitmapFormatError.encodedSizeOverflow
        }
        let outputReservation = try workMeter.reserveIntermediate(
            bytes: maximumBytes,
            at: .indexScan
        )
        do {
            let output = operation(value, other.value)
            let outputBytes = try output.retainedStorageByteCount()
            guard outputBytes <= maximumBytes else {
                throw RoaringBitmapFormatError.encodedSizeOverflow
            }
            try outputReservation.releasePartial(
                bytes: maximumBytes - outputBytes
            )
            reservation.release()
            other.reservation.release()
            return BitmapReadOwner(
                value: output,
                reservation: outputReservation
            )
        } catch {
            outputReservation.release()
            reservation.release()
            other.reservation.release()
            throw error
        }
    }
}
