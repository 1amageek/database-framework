import Testing
import Synchronization
import DatabaseKitWasmCore
@testable import DatabaseFrameworkWasm

@Suite("DatabaseFrameworkWasm Tests")
struct DatabaseFrameworkWasmTests {
    @Test func handlePutAndGetRoundTripsThroughBinaryEnvelope() throws {
        let storage = WasmMemoryStorage()
        let runtime = DatabaseFrameworkWasmRuntime(storage: storage)
        let record = article(
            id: "article-1",
            status: "published",
            score: 10,
            title: "Swift on Workers",
            tags: ["swift", "wasm"]
        )

        let putResponse = try runtime.handle(
            DatabaseKitWasmCodec.encode(request: .putRecord(record))
        )
        #expect(try DatabaseKitWasmCodec.decodeResponse(putResponse) == .empty)

        let getResponse = try runtime.handle(
            DatabaseKitWasmCodec.encode(
                request: .getRecord(typeName: "Article", id: "article-1")
            )
        )

        #expect(try DatabaseKitWasmCodec.decodeResponse(getResponse) == .record(record))
    }

    @Test func queryWithoutPredicateAppliesStorageLimit() throws {
        let storage = WasmMemoryStorage()
        let runtime = DatabaseFrameworkWasmRuntime(storage: storage)
        try put(
            [
                article(id: "a", status: "draft", score: 1, title: "A", tags: []),
                article(id: "b", status: "published", score: 2, title: "B", tags: []),
                article(id: "c", status: "published", score: 3, title: "C", tags: [])
            ],
            into: runtime
        )

        let response = try runtime.execute(
            .query(DatabaseKitWasmQueryRequest(typeName: "Article", predicate: nil, limit: 2))
        )

        #expect(response == .records([
            article(id: "a", status: "draft", score: 1, title: "A", tags: []),
            article(id: "b", status: "published", score: 2, title: "B", tags: [])
        ]))
    }

    @Test func queryAppliesConjunctionAfterFullScanBeforeLimit() throws {
        let storage = WasmMemoryStorage()
        let runtime = DatabaseFrameworkWasmRuntime(storage: storage)
        try put(
            [
                article(id: "a", status: "draft", score: 100, title: "A", tags: []),
                article(id: "b", status: "published", score: 10, title: "B", tags: []),
                article(id: "c", status: "published", score: 90, title: "C", tags: []),
                article(id: "d", status: "published", score: 95, title: "D", tags: [])
            ],
            into: runtime
        )
        let query = DatabaseKitWasmQueryRequest(
            typeName: "Article",
            predicate: .and([
                .comparison(field: "status", op: .equal, value: .string("published")),
                .comparison(field: "score", op: .greaterThanOrEqual, value: .int64(90))
            ]),
            limit: 1
        )

        let response = try runtime.execute(.query(query))

        #expect(response == .records([
            article(id: "c", status: "published", score: 90, title: "C", tags: [])
        ]))
    }

    @Test func querySupportsDisjunctionNegationAndContains() throws {
        let storage = WasmMemoryStorage()
        let runtime = DatabaseFrameworkWasmRuntime(storage: storage)
        try put(
            [
                article(id: "a", status: "draft", score: 1, title: "Local Swift", tags: ["swift"]),
                article(id: "b", status: "archived", score: 2, title: "Old Notes", tags: ["notes"]),
                article(id: "c", status: "published", score: 3, title: "Workers Runtime", tags: ["cloudflare"]),
                article(id: "d", status: "published", score: 4, title: "Durable Swift", tags: ["swift", "cloudflare"])
            ],
            into: runtime
        )
        let query = DatabaseKitWasmQueryRequest(
            typeName: "Article",
            predicate: .and([
                .or([
                    .comparison(field: "title", op: .contains, value: .string("Swift")),
                    .comparison(field: "tags", op: .contains, value: .string("cloudflare"))
                ]),
                .not(.comparison(field: "status", op: .equal, value: .string("archived")))
            ]),
            limit: 10
        )

        let response = try runtime.execute(.query(query))

        #expect(response == .records([
            article(id: "a", status: "draft", score: 1, title: "Local Swift", tags: ["swift"]),
            article(id: "c", status: "published", score: 3, title: "Workers Runtime", tags: ["cloudflare"]),
            article(id: "d", status: "published", score: 4, title: "Durable Swift", tags: ["swift", "cloudflare"])
        ]))
    }

    @Test func malformedRequestReturnsInvalidRequestFailureEnvelope() throws {
        let storage = WasmMemoryStorage()
        let runtime = DatabaseFrameworkWasmRuntime(storage: storage)

        let responseBytes = try runtime.handle([0x01, 0xFF])
        let response = try DatabaseKitWasmCodec.decodeResponse(responseBytes)

        #expect(response == .failure(status: .invalidRequest, message: "unknown operation"))
    }

    private func put(
        _ records: [DatabaseKitWasmRecord],
        into runtime: DatabaseFrameworkWasmRuntime<WasmMemoryStorage>
    ) throws {
        for record in records {
            _ = try runtime.execute(.putRecord(record))
        }
    }

    private func article(
        id: String,
        status: String,
        score: Int64,
        title: String,
        tags: [String]
    ) -> DatabaseKitWasmRecord {
        DatabaseKitWasmRecord(
            typeName: "Article",
            id: id,
            fields: [
                DatabaseKitWasmNamedValue(name: "status", value: .string(status)),
                DatabaseKitWasmNamedValue(name: "score", value: .int64(score)),
                DatabaseKitWasmNamedValue(name: "title", value: .string(title)),
                DatabaseKitWasmNamedValue(
                    name: "tags",
                    value: .array(tags.map { .string($0) })
                )
            ]
        )
    }
}

private final class WasmMemoryStorage: DatabaseFrameworkWasmStorage {
    private let rows = Mutex<[DatabaseFrameworkWasmKeyValue]>([])

    func read(key: [UInt8]) throws(DatabaseFrameworkWasmError) -> [UInt8]? {
        rows.withLock { rows in
            for row in rows where row.key == key {
                return row.value
            }
            return nil
        }
    }

    func scan(
        begin: [UInt8],
        end: [UInt8],
        limit: Int,
        reverse: Bool
    ) throws(DatabaseFrameworkWasmError) -> [DatabaseFrameworkWasmKeyValue] {
        rows.withLock { rows in
            var result = rows.filter {
                compareBytes($0.key, begin) >= 0 && compareBytes($0.key, end) < 0
            }
            result.sort { compareBytes($0.key, $1.key) < 0 }
            if reverse {
                result.reverse()
            }
            if limit > 0 && result.count > limit {
                result = Array(result.prefix(limit))
            }
            return result
        }
    }

    func commit(_ writes: [DatabaseFrameworkWasmWriteOperation]) throws(DatabaseFrameworkWasmError) {
        rows.withLock { rows in
            for write in writes {
                switch write {
                case .set(let key, let value):
                    upsert(DatabaseFrameworkWasmKeyValue(key: key, value: value), into: &rows)
                case .clear(let key):
                    rows.removeAll { $0.key == key }
                }
            }
        }
    }

    private func upsert(
        _ row: DatabaseFrameworkWasmKeyValue,
        into rows: inout [DatabaseFrameworkWasmKeyValue]
    ) {
        if let index = rows.firstIndex(where: { $0.key == row.key }) {
            rows[index] = row
        } else {
            rows.append(row)
        }
    }

    private func compareBytes(_ lhs: [UInt8], _ rhs: [UInt8]) -> Int {
        let count = min(lhs.count, rhs.count)
        for index in 0..<count {
            if lhs[index] != rhs[index] {
                return lhs[index] < rhs[index] ? -1 : 1
            }
        }
        if lhs.count == rhs.count {
            return 0
        }
        return lhs.count < rhs.count ? -1 : 1
    }
}
