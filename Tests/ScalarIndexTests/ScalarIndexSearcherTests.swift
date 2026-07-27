import DatabaseTypes
import Synchronization
import Testing
import DatabaseKit
import DatabaseEngine
import StorageKit
@testable import ScalarIndex

private final class ScalarIndexSearchReader: StorageReader, Sendable {
    private let entries = Mutex<[(key: ByteString, value: ByteString)]>([])
    let indexSubspace = Subspace(prefix: [0x49])

    func addEntry(
        indexName: String,
        keyValues: [any TupleElement],
        identifier: String,
        coveringValue: ByteString = []
    ) {
        var elements = keyValues
        elements.append(identifier)
        let key = indexSubspace
            .subspace(indexName)
            .pack(Tuple(elements))
        entries.withLock {
            $0.append((key: key, value: coveringValue))
        }
    }

    func fetchItem<T: Persistable & Codable>(
        id: any TupleElement,
        type: T.Type
    ) async throws -> T? {
        nil
    }

    func scanItems<T: Persistable & Codable>(
        type: T.Type
    ) -> AsyncThrowingStream<T, Error> {
        AsyncThrowingStream { $0.finish() }
    }

    func scanRange(
        subspace: Subspace,
        start: Tuple?,
        end: Tuple?,
        startInclusive: Bool,
        endInclusive: Bool,
        reverse: Bool
    ) -> AsyncThrowingStream<(key: ByteString, value: ByteString), Error> {
        var matches: [(key: ByteString, value: ByteString)] = []
        for entry in entries.withLock({ $0 }) {
            guard entry.key.starts(with: subspace.prefix) else {
                continue
            }
            if let start {
                let key = subspace.pack(start)
                let accepted = startInclusive
                    ? !entry.key.lexicographicallyPrecedes(key)
                    : key.lexicographicallyPrecedes(entry.key)
                guard accepted else {
                    continue
                }
            }
            if let end {
                let key = subspace.pack(end)
                let accepted = endInclusive
                    ? !key.lexicographicallyPrecedes(entry.key)
                    : entry.key.lexicographicallyPrecedes(key)
                guard accepted else {
                    continue
                }
            }
            matches.append(entry)
        }
        matches.sort { $0.key.lexicographicallyPrecedes($1.key) }
        if reverse {
            matches.reverse()
        }
        return AsyncThrowingStream { continuation in
            for match in matches {
                continuation.yield(match)
            }
            continuation.finish()
        }
    }

    func scanSubspace(
        _ subspace: Subspace
    ) -> AsyncThrowingStream<(key: ByteString, value: ByteString), Error> {
        scanRange(
            subspace: subspace,
            start: nil,
            end: nil,
            startInclusive: true,
            endInclusive: false,
            reverse: false
        )
    }

    func getValue(key: ByteString) async throws -> ByteString? {
        entries.withLock {
            $0.first(where: { $0.key == key })?.value
        }
    }
}

@Suite("Scalar index physical search")
struct ScalarIndexSearcherTests {
    @Test("Equality scans only the matching prefix")
    func equalitySearch() async throws {
        let reader = ScalarIndexSearchReader()
        reader.addEntry(
            indexName: "category",
            keyValues: ["electronics"],
            identifier: "one"
        )
        reader.addEntry(
            indexName: "category",
            keyValues: ["electronics"],
            identifier: "two"
        )
        reader.addEntry(
            indexName: "category",
            keyValues: ["clothing"],
            identifier: "three"
        )

        let results = try await ScalarIndexSearcher().search(
            query: .equals(["electronics"]),
            in: reader.indexSubspace.subspace("category"),
            using: reader
        )

        #expect(results.count == 2)
        #expect(Set(results.compactMap { $0.itemID[0] as? String }) == ["one", "two"])
    }

    @Test("Range bounds and ordering are preserved")
    func rangeSearch() async throws {
        let reader = ScalarIndexSearchReader()
        for value in [10, 20, 30, 40] {
            reader.addEntry(
                indexName: "price",
                keyValues: [value],
                identifier: "\(value)"
            )
        }

        let results = try await ScalarIndexSearcher().search(
            query: ScalarIndexQuery(
                start: [15],
                end: [35],
                reverse: true
            ),
            in: reader.indexSubspace.subspace("price"),
            using: reader
        )

        #expect(results.compactMap { $0.itemID[0] as? String } == ["30", "20"])
    }

    @Test("Composite keys preserve index values and identifier")
    func compositeKeySearch() async throws {
        let reader = ScalarIndexSearchReader()
        reader.addEntry(
            indexName: "region",
            keyValues: ["US", "CA"],
            identifier: "one"
        )

        let results = try await ScalarIndexSearcher(
            keyFieldCount: 2
        ).search(
            query: .equals(["US", "CA"]),
            in: reader.indexSubspace.subspace("region"),
            using: reader
        )

        let result = try #require(results.first)
        #expect(result.keyValues[0] as? String == "US")
        #expect(result.keyValues[1] as? String == "CA")
        #expect(result.itemID[0] as? String == "one")
    }

    @Test("Covering bytes remain in their owned representation")
    func coveringValueSearch() async throws {
        let reader = ScalarIndexSearchReader()
        let coveringValue: ByteString = [0x44, 0x42, 0x49, 0x58]
        reader.addEntry(
            indexName: "category",
            keyValues: ["electronics"],
            identifier: "one",
            coveringValue: coveringValue
        )

        let results = try await ScalarIndexSearcher().search(
            query: .all,
            in: reader.indexSubspace.subspace("category"),
            using: reader
        )

        #expect(results.first?.coveringValue == coveringValue)
    }

    @Test("Limit stops physical result collection")
    func limitedSearch() async throws {
        let reader = ScalarIndexSearchReader()
        for identifier in 0..<10 {
            reader.addEntry(
                indexName: "category",
                keyValues: ["electronics"],
                identifier: "\(identifier)"
            )
        }

        let results = try await ScalarIndexSearcher().search(
            query: ScalarIndexQuery(limit: 3),
            in: reader.indexSubspace.subspace("category"),
            using: reader
        )

        #expect(results.count == 3)
    }

    @Test("Malformed keys fail with a typed error")
    func malformedKeyFails() async throws {
        let reader = ScalarIndexSearchReader()
        reader.addEntry(
            indexName: "category",
            keyValues: [],
            identifier: "one"
        )

        await #expect(throws: ScalarIndexSearchError.self) {
            _ = try await ScalarIndexSearcher(
                keyFieldCount: 2
            ).search(
                query: .all,
                in: reader.indexSubspace.subspace("category"),
                using: reader
            )
        }
    }
}
