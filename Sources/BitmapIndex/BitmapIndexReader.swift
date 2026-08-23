import DatabaseEngine
import StorageKit

/// Immutable bitmap ownership paired with its request-memory reservation.
/// Copies share the bitmap's COW storage and the same reservation lifetime.
final class BitmapIndexRetainedBitmap: Sendable {
    let value: RoaringBitmap
    private let reservation: DatabaseIntermediateReservation

    init(
        value: consuming RoaringBitmap,
        reservation: DatabaseIntermediateReservation
    ) {
        self.value = value
        self.reservation = reservation
    }
}

/// Reads the persisted bitmap index layout without requiring a model type.
///
/// Model-specific decoding happens after primary-key resolution. Keeping this
/// reader independent from `Persistable` prevents polymorphic reads from
/// manufacturing a model solely to satisfy a generic maintainer constraint.
struct BitmapIndexReader: Sendable {
    private let dataSubspace: Subspace
    private let idsSubspace: Subspace

    init(subspace: Subspace) {
        self.dataSubspace = subspace.subspace("data")
        self.idsSubspace = subspace.subspace("ids")
    }

    func bitmap(
        for fieldValues: [any TupleElement],
        transaction: any TransactionReadAccess
    ) async throws -> RoaringBitmap {
        let key = dataSubspace.pack(Tuple(fieldValues))
        guard let bytes = try await transaction.getValue(
            for: key,
            snapshot: false
        ) else {
            return RoaringBitmap()
        }
        return try RoaringBitmap(serializedBytes: bytes)
    }

    func retainedBitmap(
        for fieldValues: [any TupleElement],
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> BitmapIndexRetainedBitmap {
        let key = dataSubspace.pack(Tuple(fieldValues))
        guard let bytes = try await transaction.getValue(
            for: key,
            snapshot: false
        ) else {
            return try retain(
                RoaringBitmap(),
                workMeter: workMeter
            )
        }

        // Each persisted container uses at least nine bytes and contributes
        // at most 64 bytes of logical container overhead. Its decoded payload
        // is no larger than its serialized payload, so 9x plus fixed storage
        // is a conservative pre-allocation bound for the retained model.
        let maximumDecodedBytes = try DatabaseIntermediateFootprint(
            bytes: UInt64(bytes.count)
        ).multiplied(by: 9).adding(
            DatabaseIntermediateFootprint(bytes: 128)
        ).bytes
        let reservation = try workMeter.reserveIntermediate(
            bytes: maximumDecodedBytes,
            at: .indexScan
        )
        var transferredReservation = false
        defer {
            if !transferredReservation { reservation.release() }
        }
        let bitmap = try RoaringBitmap(serializedBytes: bytes)
        let retainedBytes = try bitmap.retainedStorageByteCount()
        guard retainedBytes <= maximumDecodedBytes else {
            throw RoaringBitmapFormatError.encodedSizeOverflow
        }
        if retainedBytes < maximumDecodedBytes {
            try reservation.releasePartial(
                bytes: maximumDecodedBytes - retainedBytes
            )
        }
        transferredReservation = true
        return BitmapIndexRetainedBitmap(
            value: bitmap,
            reservation: reservation
        )
    }

    func intersection(
        of values: [[any TupleElement]],
        transaction: any TransactionReadAccess
    ) async throws -> RoaringBitmap {
        guard let first = values.first else {
            return RoaringBitmap()
        }
        var result = try await bitmap(
            for: first,
            transaction: transaction
        )
        for value in values.dropFirst() {
            let next = try await bitmap(
                for: value,
                transaction: transaction
            )
            result = result && next
        }
        return result
    }

    func retainedIntersection(
        of values: [[any TupleElement]],
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> BitmapIndexRetainedBitmap {
        guard let first = values.first else {
            return try retain(RoaringBitmap(), workMeter: workMeter)
        }
        var result = try await retainedBitmap(
            for: first,
            transaction: transaction,
            workMeter: workMeter
        )
        for value in values.dropFirst() {
            let next = try await retainedBitmap(
                for: value,
                transaction: transaction,
                workMeter: workMeter
            )
            result = try combine(
                result,
                next,
                workMeter: workMeter,
                operation: { $0 && $1 }
            )
        }
        return result
    }

    func union(
        of values: [[any TupleElement]],
        transaction: any TransactionReadAccess
    ) async throws -> RoaringBitmap {
        var result = RoaringBitmap()
        for value in values {
            let next = try await bitmap(
                for: value,
                transaction: transaction
            )
            result = result || next
        }
        return result
    }

    func retainedUnion(
        of values: [[any TupleElement]],
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> BitmapIndexRetainedBitmap {
        var result = try retain(RoaringBitmap(), workMeter: workMeter)
        for value in values {
            let next = try await retainedBitmap(
                for: value,
                transaction: transaction,
                workMeter: workMeter
            )
            result = try combine(
                result,
                next,
                workMeter: workMeter,
                operation: { $0 || $1 }
            )
        }
        return result
    }

    func primaryKeys(
        for bitmap: RoaringBitmap,
        transaction: any TransactionReadAccess,
        limit: Int? = nil
    ) async throws -> [Tuple] {
        var results: [Tuple] = []
        results.reserveCapacity(min(bitmap.cardinality, limit ?? bitmap.cardinality))

        for identifier in bitmap {
            guard results.count < (limit ?? Int.max) else { break }
            let key = idsSubspace.pack(Tuple(Int(identifier)))
            if let bytes = try await transaction.getValue(
                for: key,
                snapshot: false
            ) {
                results.append(Tuple(try Tuple.unpack(from: bytes)))
            }
        }
        return results
    }

    func retainedPrimaryKeys(
        for bitmap: BitmapIndexRetainedBitmap,
        transaction: any TransactionReadAccess,
        limit: Int? = nil,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<Tuple> {
        let expectedCount = min(
            bitmap.value.cardinality,
            limit ?? bitmap.value.cardinality
        )
        var results = try DatabaseRetainedArrayBuilder<Tuple>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: Tuple.self),
            expectedCount: expectedCount
        )
        for identifier in bitmap.value {
            guard results.count < (limit ?? Int.max) else { break }
            try workMeter.consume(at: .indexScan)
            let key = idsSubspace.pack(Tuple(Int(identifier)))
            if let bytes = try await transaction.getValue(
                for: key,
                snapshot: false
            ) {
                try results.append(
                    footprint: DatabaseIntermediateFootprint(
                        rows: 1,
                        bytes: UInt64(bytes.count) + 64
                    ),
                    at: .indexScan,
                    make: { Tuple(try Tuple.unpack(from: bytes)) }
                )
            }
        }
        return try results.finish().moveToSharedOwnership(at: .indexScan)
    }

    private func retain(
        _ bitmap: consuming RoaringBitmap,
        workMeter: DatabaseWorkMeter
    ) throws -> BitmapIndexRetainedBitmap {
        let reservation = try workMeter.reserveIntermediate(
            bytes: try bitmap.retainedStorageByteCount(),
            at: .indexScan
        )
        return BitmapIndexRetainedBitmap(
            value: bitmap,
            reservation: reservation
        )
    }

    private func combine(
        _ lhs: BitmapIndexRetainedBitmap,
        _ rhs: BitmapIndexRetainedBitmap,
        workMeter: DatabaseWorkMeter,
        operation: (RoaringBitmap, RoaringBitmap) -> RoaringBitmap
    ) throws -> BitmapIndexRetainedBitmap {
        let maximumOutputBytes = try DatabaseIntermediateFootprint(
            bytes: try lhs.value.retainedStorageByteCount()
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: try rhs.value.retainedStorageByteCount()
            )
        ).bytes
        let reservation = try workMeter.reserveIntermediate(
            bytes: maximumOutputBytes,
            at: .indexScan
        )
        var transferredReservation = false
        defer {
            if !transferredReservation { reservation.release() }
        }
        let output = operation(lhs.value, rhs.value)
        let outputBytes = try output.retainedStorageByteCount()
        guard outputBytes <= maximumOutputBytes else {
            throw RoaringBitmapFormatError.encodedSizeOverflow
        }
        if outputBytes < maximumOutputBytes {
            try reservation.releasePartial(
                bytes: maximumOutputBytes - outputBytes
            )
        }
        transferredReservation = true
        return BitmapIndexRetainedBitmap(
            value: output,
            reservation: reservation
        )
    }

    func distinctValues(
        transaction: any TransactionReadAccess
    ) async throws -> [[any TupleElement]] {
        let range = dataSubspace.range()
        let sequence = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        var results: [[any TupleElement]] = []
        for (key, _) in sequence {
            guard dataSubspace.contains(key) else {
                break
            }
            results.append(try dataSubspace.unpack(key).elements())
        }
        return results
    }
}
