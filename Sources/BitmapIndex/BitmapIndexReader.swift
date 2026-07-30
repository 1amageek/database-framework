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
        transaction: any TransactionAccess
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

    func intersection(
        of values: [[any TupleElement]],
        transaction: any TransactionAccess
    ) async throws -> RoaringBitmap {
        guard let first = values.first else {
            return RoaringBitmap()
        }
        var result = try await bitmap(for: first, transaction: transaction)
        for value in values.dropFirst() {
            result = result && (try await bitmap(for: value, transaction: transaction))
        }
        return result
    }

    func union(
        of values: [[any TupleElement]],
        transaction: any TransactionAccess
    ) async throws -> RoaringBitmap {
        var result = RoaringBitmap()
        for value in values {
            result = result || (try await bitmap(for: value, transaction: transaction))
        }
        return result
    }

    func primaryKeys(
        for bitmap: RoaringBitmap,
        transaction: any TransactionAccess
    ) async throws -> [Tuple] {
        var results: [Tuple] = []
        results.reserveCapacity(bitmap.cardinality)

        for identifier in bitmap {
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
