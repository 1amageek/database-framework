import DatabaseEngine
import StorageKit

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
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> RoaringBitmap {
        let key = dataSubspace.pack(Tuple(fieldValues))
        guard let bytes = try await transaction.getValue(
            for: key,
            snapshot: false
        ) else {
            return RoaringBitmap()
        }
        let decodeAdmission = try workMeter?.reserveIntermediate(
            bytes: try DatabaseIntermediateFootprint(
                bytes: UInt64(bytes.count)
            ).adding(
                DatabaseIntermediateFootprint(bytes: 64)
            ).bytes,
            at: .indexScan
        )
        defer { decodeAdmission?.release() }
        let bitmap = try RoaringBitmap(serializedBytes: bytes)
        let serializedFootprint = try DatabaseIntermediateFootprint(
            bytes: UInt64(bytes.count)
        ).adding(
            DatabaseIntermediateFootprint(bytes: 64)
        ).bytes
        let retainedBytes = try bitmap.retainedStorageByteCount()
        if retainedBytes > serializedFootprint {
            try decodeAdmission?.reserveAdditional(
                bytes: retainedBytes - serializedFootprint,
                at: .indexScan
            )
        }
        return bitmap
    }

    func intersection(
        of values: [[any TupleElement]],
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> RoaringBitmap {
        guard let first = values.first else {
            return RoaringBitmap()
        }
        var result = try await bitmap(
            for: first,
            transaction: transaction,
            workMeter: workMeter
        )
        var resultReservation = try workMeter?.reserveIntermediate(
            bytes: try result.retainedStorageByteCount(),
            at: .indexScan
        )
        defer { resultReservation?.release() }
        for value in values.dropFirst() {
            let next = try await bitmap(
                for: value,
                transaction: transaction,
                workMeter: workMeter
            )
            let nextBytes = try next.retainedStorageByteCount()
            let nextReservation = try workMeter?.reserveIntermediate(
                bytes: nextBytes,
                at: .indexScan
            )
            defer { nextReservation?.release() }
            let maximumOutputBytes = try DatabaseIntermediateFootprint(
                bytes: try result.retainedStorageByteCount()
            ).adding(
                DatabaseIntermediateFootprint(bytes: nextBytes)
            ).bytes
            let outputAdmission = try workMeter?.reserveIntermediate(
                bytes: maximumOutputBytes,
                at: .indexScan
            )
            let output = result && next
            let outputBytes = try output.retainedStorageByteCount()
            if outputBytes < maximumOutputBytes {
                try outputAdmission?.releasePartial(
                    bytes: maximumOutputBytes - outputBytes
                )
            }
            resultReservation?.release()
            resultReservation = outputAdmission
            result = output
        }
        return result
    }

    func union(
        of values: [[any TupleElement]],
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> RoaringBitmap {
        var result = RoaringBitmap()
        var resultReservation = try workMeter?.reserveIntermediate(
            bytes: try result.retainedStorageByteCount(),
            at: .indexScan
        )
        defer { resultReservation?.release() }
        for value in values {
            let next = try await bitmap(
                for: value,
                transaction: transaction,
                workMeter: workMeter
            )
            let nextBytes = try next.retainedStorageByteCount()
            let nextReservation = try workMeter?.reserveIntermediate(
                bytes: nextBytes,
                at: .indexScan
            )
            defer { nextReservation?.release() }
            let maximumOutputBytes = try DatabaseIntermediateFootprint(
                bytes: try result.retainedStorageByteCount()
            ).adding(
                DatabaseIntermediateFootprint(bytes: nextBytes)
            ).bytes
            let outputAdmission = try workMeter?.reserveIntermediate(
                bytes: maximumOutputBytes,
                at: .indexScan
            )
            let output = result || next
            let outputBytes = try output.retainedStorageByteCount()
            if outputBytes < maximumOutputBytes {
                try outputAdmission?.releasePartial(
                    bytes: maximumOutputBytes - outputBytes
                )
            }
            resultReservation?.release()
            resultReservation = outputAdmission
            result = output
        }
        return result
    }

    func primaryKeys(
        for bitmap: RoaringBitmap,
        transaction: any TransactionAccess,
        limit: Int? = nil,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> [Tuple] {
        var results: [Tuple] = []
        results.reserveCapacity(min(bitmap.cardinality, limit ?? bitmap.cardinality))
        let bitmapReservation = try workMeter?.reserveIntermediate(
            bytes: try bitmap.retainedStorageByteCount(),
            at: .indexScan
        )
        defer { bitmapReservation?.release() }
        let resultReservation = try workMeter?.reserveIntermediate(
            bytes: UInt64(MemoryLayout<[Tuple]>.stride),
            at: .indexScan
        )
        defer { resultReservation?.release() }

        for identifier in bitmap {
            guard results.count < (limit ?? Int.max) else { break }
            try workMeter?.consume(at: .indexScan)
            let key = idsSubspace.pack(Tuple(Int(identifier)))
            if let bytes = try await transaction.getValue(
                for: key,
                snapshot: false
            ) {
                let tuple = Tuple(try Tuple.unpack(from: bytes))
                let retainedBytes = try DatabaseIntermediateFootprint(
                    bytes: UInt64(bytes.count)
                ).adding(
                    DatabaseIntermediateFootprint(bytes: 64)
                ).bytes
                try resultReservation?.reserveAdditional(
                    rows: 1,
                    bytes: retainedBytes,
                    at: .indexScan
                )
                results.append(tuple)
            }
        }
        return results
    }

    func distinctValues(
        transaction: any TransactionAccess
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
