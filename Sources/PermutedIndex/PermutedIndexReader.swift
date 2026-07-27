import DatabaseKit
import StorageKit

/// Reads the physical key layout owned by a permuted index.
struct PermutedIndexReader: Sendable {
    let permutation: Permutation
    let subspace: Subspace

    func primaryKeys(
        prefixedBy values: [any TupleElement],
        transaction: any TransactionAccess
    ) async throws -> [[any TupleElement]] {
        guard values.count <= permutation.size else {
            throw PermutedIndexError.fieldCountMismatch(
                expected: permutation.size,
                got: values.count
            )
        }
        let prefixSubspace = values.isEmpty
            ? subspace
            : Subspace(prefix: subspace.prefix + Tuple(values).pack())
        let (begin, end) = prefixSubspace.range()
        var results: [[any TupleElement]] = []

        let sequence = try await transaction.collectRange(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            snapshot: true
        )
        for (key, _) in sequence {
            guard prefixSubspace.contains(key) else { break }
            let elements = try prefixSubspace.unpack(key).elements()
            let remainingFieldCount = permutation.size - values.count
            guard elements.count > remainingFieldCount else {
                throw PermutedIndexError.corruptedEntry(
                    expectedMinimumElementCount: remainingFieldCount + 1,
                    actual: elements.count
                )
            }
            results.append(
                Array(elements.suffix(from: remainingFieldCount))
            )
        }
        return results
    }

    func primaryKeys(
        matching values: [any TupleElement],
        transaction: any TransactionAccess
    ) async throws -> [[any TupleElement]] {
        guard values.count == permutation.size else {
            throw PermutedIndexError.fieldCountMismatch(
                expected: permutation.size,
                got: values.count
            )
        }
        return try await primaryKeys(
            prefixedBy: values,
            transaction: transaction
        )
    }

    func entries(
        transaction: any TransactionAccess
    ) async throws -> [
        (permutedFields: [any TupleElement], primaryKey: [any TupleElement])
    ] {
        let (begin, end) = subspace.range()
        var results: [
            (permutedFields: [any TupleElement], primaryKey: [any TupleElement])
        ] = []

        let sequence = try await transaction.collectRange(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            snapshot: true
        )
        for (key, _) in sequence {
            guard subspace.contains(key) else { break }
            let elements = try subspace.unpack(key).elements()
            guard elements.count > permutation.size else {
                throw PermutedIndexError.corruptedEntry(
                    expectedMinimumElementCount: permutation.size + 1,
                    actual: elements.count
                )
            }
            results.append(
                (
                    Array(elements.prefix(permutation.size)),
                    Array(elements.suffix(from: permutation.size))
                )
            )
        }
        return results
    }

    func originalOrder(
        for values: [any TupleElement]
    ) throws -> [any TupleElement] {
        try permutation.inverse.apply(values)
    }
}
