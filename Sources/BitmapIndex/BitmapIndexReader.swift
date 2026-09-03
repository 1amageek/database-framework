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
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> BitmapReadOwner {
        let key = dataSubspace.pack(Tuple(fieldValues))
        guard let bytes = try await readPointValue(
            using: transaction,
            for: key,
            snapshot: false,
            workMeter: workMeter,
            at: .indexScan
        ) else {
            return try BitmapReadOwner.empty(workMeter: workMeter)
        }

        let reservation = try workMeter.reserveIntermediate(at: .indexScan)
        do {
            var admittedBytes: UInt64 = 0
            let value = try RoaringBitmap(
                serializedBytes: bytes,
                admittingRetainedBytes: { amount in
                    try reservation.reserveAdditional(
                        bytes: amount,
                        at: .indexScan
                    )
                    let (next, overflow) = admittedBytes
                        .addingReportingOverflow(amount)
                    guard !overflow else {
                        throw RoaringBitmapFormatError.encodedSizeOverflow
                    }
                    admittedBytes = next
                }
            )
            let retainedBytes = try value.retainedStorageByteCount()
            if retainedBytes > admittedBytes {
                try reservation.reserveAdditional(
                    bytes: retainedBytes - admittedBytes,
                    at: .indexScan
                )
            } else if admittedBytes > retainedBytes {
                try reservation.releasePartial(
                    bytes: admittedBytes - retainedBytes
                )
            }
            return BitmapReadOwner(
                value: value,
                reservation: reservation
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    func intersection(
        of values: [[any TupleElement]],
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> BitmapReadOwner {
        guard let first = values.first else {
            return try BitmapReadOwner.empty(workMeter: workMeter)
        }
        var result = try await bitmap(
            for: first,
            transaction: transaction,
            workMeter: workMeter
        )
        for value in values.dropFirst() {
            let next = try await bitmap(
                for: value,
                transaction: transaction,
                workMeter: workMeter
            )
            result = try result.intersect(with: consume next)
        }
        return result
    }

    func union(
        of values: [[any TupleElement]],
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> BitmapReadOwner {
        var result = try BitmapReadOwner.empty(workMeter: workMeter)
        for value in values {
            let next = try await bitmap(
                for: value,
                transaction: transaction,
                workMeter: workMeter
            )
            result = try result.union(with: consume next)
        }
        return result
    }

    /// Resolves sequential identifiers into a retained collection. The
    /// collection keeps the same meter alive until the consumer has fetched or
    /// promoted every borrowed primary key.
    func primaryKeys(
        for bitmap: borrowing BitmapReadOwner,
        transaction: any TransactionReadAccess,
        limit: Int? = nil,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseRetainedPrimaryKeys {
        guard bitmap.workMeter === workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        let requestedLimit = max(0, limit ?? bitmap.cardinality)
        let count = min(bitmap.cardinality, requestedLimit)
        var identifiers = try DatabaseRetainedArrayBuilder<UInt32>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try DatabaseRetainedArrayLayout.forElement(UInt32.self),
            expectedCount: count
        )
        try bitmap.withValue { value in
            let iteratorReservation = try workMeter.reserveIntermediate(
                bytes: try value.iteratorRetainedStorageByteCount(),
                at: .indexScan
            )
            defer { iteratorReservation.release() }
            var seen = 0
            for identifier in value {
                guard seen < requestedLimit else { break }
                try identifiers.append(
                    footprint: DatabaseIntermediateFootprint(
                        bytes: UInt64(MemoryLayout<UInt32>.stride)
                    ),
                    at: .indexScan
                ) {
                    identifier
                }
                seen += 1
            }
        }
        let retainedIdentifiers = identifiers.finish()

        var primaryKeys = try DatabaseRetainedArrayBuilder<
            DatabaseRetainedPrimaryKey
        >(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try DatabaseRetainedArrayLayout.forElement(
                DatabaseRetainedPrimaryKey.self
            ),
            expectedCount: count
        )
        for index in 0..<retainedIdentifiers.count {
            try await retainedIdentifiers.withElement(at: index) { identifier in
                try workMeter.consume(at: .indexScan)
                let key = idsSubspace.pack(Tuple(Int(identifier)))
                guard let bytes = try await readPointValue(
                    using: transaction,
                    for: key,
                    snapshot: false,
                    workMeter: workMeter,
                    at: .indexScan
                ) else {
                    return
                }
                let admission = try primaryKeys.prepareAppend(
                    footprint: DatabaseIntermediateFootprint(rows: 1),
                    at: .indexScan
                )
                let retained = try retainedPrimaryKey(
                    from: bytes,
                    workMeter: workMeter,
                    stage: .indexScan
                )
                primaryKeys.append(retained, using: admission)
            }
        }
        return try DatabaseRetainedPrimaryKeys(
            buffer: primaryKeys.finish()
        )
    }

    /// Decodes one mapping value only after its exact retained payload has
    /// been admitted. The backend-owned read result is copied into a
    /// framework-owned byte owner before tuple decoding can outlive the read.
    private func retainedPrimaryKey(
        from bytes: ByteString,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseRetainedPrimaryKey {
        let reservation = try workMeter.reserveIntermediate(
            bytes: UInt64(bytes.count),
            at: stage
        )
        do {
            let retainedBytes = try DatabaseRetainedByteString.copying(
                bytes,
                reservation: reservation,
                at: stage
            )
            let tuple = try Tuple(packed: retainedBytes) {
                additionalByteCount in
                try reservation.reserveAdditional(
                    bytes: UInt64(additionalByteCount),
                    at: stage
                )
            }
            return DatabaseRetainedPrimaryKey(
                value: tuple,
                reservation: reservation
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    func distinctValues(
        transaction: any TransactionReadAccess
    ) async throws -> [[any TupleElement]] {
        let range = dataSubspace.range()
        let sequence = try await TransactionRangeCollection.collect(
            using: transaction,
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
