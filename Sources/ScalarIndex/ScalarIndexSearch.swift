import DatabaseTypes
import DatabaseEngine
import StorageKit

/// Physical range query for a scalar index.
public struct ScalarIndexQuery: Sendable {
    public let start: [any TupleElement]?
    public let startInclusive: Bool
    public let end: [any TupleElement]?
    public let endInclusive: Bool
    public let reverse: Bool
    public let limit: Int?

    public init(
        start: [any TupleElement]? = nil,
        startInclusive: Bool = true,
        end: [any TupleElement]? = nil,
        endInclusive: Bool = true,
        reverse: Bool = false,
        limit: Int? = nil
    ) {
        self.start = start
        self.startInclusive = startInclusive
        self.end = end
        self.endInclusive = endInclusive
        self.reverse = reverse
        self.limit = limit
    }

    public static func equals(
        _ values: [any TupleElement]
    ) -> ScalarIndexQuery {
        ScalarIndexQuery(
            start: values,
            startInclusive: true,
            end: values,
            endInclusive: true
        )
    }

    public static var all: ScalarIndexQuery {
        ScalarIndexQuery()
    }
}

/// Reads the physical key layout owned by scalar indexes.
public struct ScalarIndexSearcher: Sendable {
    private let keyFieldCount: Int

    public init(keyFieldCount: Int = 1) {
        precondition(keyFieldCount > 0)
        self.keyFieldCount = keyFieldCount
    }

    public func search(
        query: ScalarIndexQuery,
        in subspace: Subspace,
        using reader: StorageReader
    ) async throws -> [IndexEntry] {
        if let limit = query.limit {
            guard limit >= 0 else {
                throw ScalarIndexSearchError.invalidLimit(limit)
            }
            guard limit > 0 else {
                return []
            }
        }
        if let start = query.start,
           let end = query.end,
           Tuple(start).pack() == Tuple(end).pack() {
            guard query.startInclusive && query.endInclusive else {
                return []
            }
            return try await searchWithPrefix(
                subspace: subspace,
                prefix: start,
                limit: query.limit,
                reverse: query.reverse,
                using: reader
            )
        }

        let startTuple = query.start.map(Tuple.init)
        let endTuple = query.end.map(Tuple.init)
        var results: [IndexEntry] = []

        var cursor = try reader.rangeCursor(
            subspace: subspace,
            start: startTuple,
            end: endTuple,
            startInclusive: query.startInclusive,
            endInclusive: query.endInclusive,
            reverse: query.reverse
        )
        do {
            while let (key, value) = try await cursor.next() {
                results.append(
                    try indexEntry(
                        key: key,
                        value: value,
                        subspace: subspace
                    )
                )
                if let limit = query.limit, results.count >= limit {
                    try await cursor.finish()
                    return results
                }
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

    private func searchWithPrefix(
        subspace: Subspace,
        prefix: [any TupleElement],
        limit: Int?,
        reverse: Bool,
        using reader: StorageReader
    ) async throws -> [IndexEntry] {
        let prefixSubspace = Subspace(
            prefix: subspace.prefix.appending(
                contentsOf: Tuple(prefix).pack()
            )
        )
        var results: [IndexEntry] = []

        var cursor = try reader.rangeCursor(
            subspace: prefixSubspace,
            start: nil,
            end: nil,
            startInclusive: true,
            endInclusive: false,
            reverse: reverse
        )
        do {
            while let (key, value) = try await cursor.next() {
                results.append(
                    try indexEntry(
                        key: key,
                        value: value,
                        subspace: subspace
                    )
                )
                if let limit, results.count >= limit {
                    try await cursor.finish()
                    return results
                }
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

    private func indexEntry(
        key: ByteString,
        value: ByteString,
        subspace: Subspace
    ) throws -> IndexEntry {
        let tuple = try subspace.unpack(key)
        guard tuple.count > keyFieldCount else {
            throw ScalarIndexSearchError.invalidKeyElementCount(
                actual: tuple.count,
                minimum: keyFieldCount + 1
            )
        }

        var keyElements: [any TupleElement] = []
        keyElements.reserveCapacity(keyFieldCount)
        for index in 0..<keyFieldCount {
            guard let element = tuple[index] else {
                throw ScalarIndexSearchError.missingKeyElement(index)
            }
            keyElements.append(element)
        }

        var identifierElements: [any TupleElement] = []
        identifierElements.reserveCapacity(tuple.count - keyFieldCount)
        for index in keyFieldCount..<tuple.count {
            guard let element = tuple[index] else {
                throw ScalarIndexSearchError.missingKeyElement(index)
            }
            identifierElements.append(element)
        }

        return IndexEntry(
            itemID: Tuple(identifierElements),
            keyValues: Tuple(keyElements),
            coveringValue: value
        )
    }
}

public enum ScalarIndexSearchError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible {
    case invalidLimit(Int)
    case invalidKeyElementCount(actual: Int, minimum: Int)
    case missingKeyElement(Int)

    public var description: String {
        switch self {
        case .invalidLimit(let limit):
            return "Scalar index limit must be non-negative; received \(limit)"
        case .invalidKeyElementCount(let actual, let minimum):
            return "Scalar index key has \(actual) elements; expected at least \(minimum)"
        case .missingKeyElement(let index):
            return "Scalar index key element \(index) is missing"
        }
    }
}
