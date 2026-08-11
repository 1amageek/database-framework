import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Reads the physical key layout owned by a permuted index.
struct PermutedIndexReader: Sendable {
    let permutation: Permutation
    let subspace: Subspace

    func primaryKeys(
        prefixedBy values: [any TupleElement],
        transaction: any TransactionAccess,
        limit: Int? = nil,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> [[any TupleElement]] {
        if let limit {
            guard limit >= 0 else {
                throw PermutedIndexError.invalidLimit(limit)
            }
            guard limit > 0 else { return [] }
        }
        guard values.count <= permutation.size else {
            throw PermutedIndexError.fieldCountMismatch(
                expected: permutation.size,
                got: values.count
            )
        }
        let canonicalValues = try values.map(canonicalStorageElement)
        let prefixSubspace = values.isEmpty
            ? subspace
            : Subspace(
                prefix: subspace.prefix.appending(
                    contentsOf: Tuple(canonicalValues).pack()
                )
            )
        let (begin, end) = prefixSubspace.range()
        var results: [[any TupleElement]] = []
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: limit ?? 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )
        let retention = try workMeter?.reserveIntermediate(
            bytes: UInt64(MemoryLayout<[[any TupleElement]]>.stride),
            at: .indexScan
        )
        defer { retention?.release() }
        do {
            while let (key, value) = try await cursor.next() {
                try workMeter?.consume(at: .indexScan)
                guard prefixSubspace.contains(key) else { break }
                let elements = try prefixSubspace.unpack(key).elements()
                let remainingFieldCount = permutation.size - values.count
                guard elements.count > remainingFieldCount else {
                    throw PermutedIndexError.corruptedEntry(
                        expectedMinimumElementCount: remainingFieldCount + 1,
                        actual: elements.count
                    )
                }
                let primaryKey = Array(
                    elements.suffix(from: remainingFieldCount)
                )
                try retention?.reserveAdditional(
                    rows: 1,
                    bytes: try retainedByteCount(
                        key: key,
                        value: value,
                        primaryKey: primaryKey
                    ),
                    at: .indexScan
                )
                results.append(primaryKey)
            }
        } catch {
            let iterationError = error
            do {
                try await cursor.finish()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: iterationError,
                    cleanupError: error
                )
            }
            throw iterationError
        }
        try await cursor.finish()
        return results
    }

    func primaryKeys(
        matching values: [any TupleElement],
        transaction: any TransactionAccess,
        limit: Int? = nil,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> [[any TupleElement]] {
        guard values.count == permutation.size else {
            throw PermutedIndexError.fieldCountMismatch(
                expected: permutation.size,
                got: values.count
            )
        }
        return try await primaryKeys(
            prefixedBy: values,
            transaction: transaction,
            limit: limit,
            workMeter: workMeter
        )
    }

    func entries(
        transaction: any TransactionAccess,
        limit: Int? = nil,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> [
        (permutedFields: [FieldValue], primaryKey: [any TupleElement])
    ] {
        if let limit {
            guard limit >= 0 else {
                throw PermutedIndexError.invalidLimit(limit)
            }
            guard limit > 0 else { return [] }
        }
        let (begin, end) = subspace.range()
        var results: [
            (permutedFields: [FieldValue], primaryKey: [any TupleElement])
        ] = []

        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: limit ?? 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )
        let retention = try workMeter?.reserveIntermediate(
            bytes: UInt64(
                MemoryLayout<[
                    (permutedFields: [FieldValue], primaryKey: [any TupleElement])
                ]>.stride
            ),
            at: .indexScan
        )
        defer { retention?.release() }
        do {
            while let (key, value) = try await cursor.next() {
                try workMeter?.consume(at: .indexScan)
                guard subspace.contains(key) else { break }
                let elements = try subspace.unpack(key).elements()
                guard elements.count > permutation.size else {
                    throw PermutedIndexError.corruptedEntry(
                        expectedMinimumElementCount: permutation.size + 1,
                        actual: elements.count
                    )
                }
                var fields: [FieldValue] = []
                fields.reserveCapacity(permutation.size)
                for element in elements.prefix(permutation.size) {
                    fields.append(try FieldValueTupleCodec.decode(element))
                }
                let primaryKey = Array(
                    elements.suffix(from: permutation.size)
                )
                try retention?.reserveAdditional(
                    rows: 1,
                    bytes: try retainedByteCount(
                        key: key,
                        value: value,
                        primaryKey: primaryKey,
                        fieldCount: fields.count
                    ),
                    at: .indexScan
                )
                results.append((fields, primaryKey))
            }
        } catch {
            let iterationError = error
            do {
                try await cursor.finish()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: iterationError,
                    cleanupError: error
                )
            }
            throw iterationError
        }
        try await cursor.finish()
        return results
    }

    func originalOrder(
        for values: [any TupleElement]
    ) throws -> [any TupleElement] {
        try permutation.inverse.apply(values)
    }

    private func canonicalStorageElement(
        _ element: any TupleElement
    ) throws -> any TupleElement {
        if case .bytes(let bytes)? = element.tupleValue,
           FieldValueTupleCodec.isCanonicalEncoding(bytes) {
            _ = try FieldValueTupleCodec.decode(element)
            return element
        }

        let value = try CanonicalTupleElementCodec.encode(element)
        return try FieldValueTupleCodec.tupleElement(for: value)
    }

    private func retainedByteCount(
        key: ByteString,
        value: ByteString,
        primaryKey: [any TupleElement],
        fieldCount: Int = 0
    ) throws -> UInt64 {
        let fieldBytes = try DatabaseIntermediateFootprint(
            bytes: 32
        ).multiplied(by: UInt64(fieldCount))
        return try DatabaseIntermediateFootprint(
            bytes: UInt64(key.count)
        ).adding(
            DatabaseIntermediateFootprint(bytes: UInt64(value.count))
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: UInt64(Tuple(primaryKey).pack().count)
            )
        ).adding(fieldBytes).adding(
            DatabaseIntermediateFootprint(bytes: 96)
        ).bytes
    }
}
