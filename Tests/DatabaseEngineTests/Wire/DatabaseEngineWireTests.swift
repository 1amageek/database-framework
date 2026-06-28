import Testing
import Synchronization
import DatabaseWire
@testable import DatabaseEngine

@Suite("DatabaseEngine Tests")
struct DatabaseEngineWireTests {
    @Test func handlePutAndGetRoundTripsThroughBinaryEnvelope() throws {
        let storage = MemoryStorage()
        let runtime = DatabaseEngineRuntime(storage: storage)
        let record = article(
            id: "article-1",
            status: "published",
            score: 10,
            title: "Swift on Workers",
            tags: ["swift", "wire"]
        )

        let putResponse = try runtime.handle(
            DatabaseWireCodec.encode(request: .putRecord(record))
        )
        #expect(try DatabaseWireCodec.decodeResponse(putResponse) == .empty)

        let getResponse = try runtime.handle(
            DatabaseWireCodec.encode(
                request: .getRecord(typeName: "Article", id: "article-1")
            )
        )

        #expect(try DatabaseWireCodec.decodeResponse(getResponse) == .record(record))
    }

    @Test func queryWithoutPredicateAppliesStorageLimit() throws {
        let storage = MemoryStorage()
        let runtime = DatabaseEngineRuntime(storage: storage)
        try put(
            [
                article(id: "a", status: "draft", score: 1, title: "A", tags: []),
                article(id: "b", status: "published", score: 2, title: "B", tags: []),
                article(id: "c", status: "published", score: 3, title: "C", tags: [])
            ],
            into: runtime
        )

        let response = try runtime.execute(
            .query(DatabaseWireQueryRequest(typeName: "Article", predicate: nil, limit: 2))
        )

        #expect(response == .records([
            article(id: "a", status: "draft", score: 1, title: "A", tags: []),
            article(id: "b", status: "published", score: 2, title: "B", tags: [])
        ]))
    }

    @Test func queryAppliesConjunctionAfterFullScanBeforeLimit() throws {
        let storage = MemoryStorage()
        let runtime = DatabaseEngineRuntime(storage: storage)
        try put(
            [
                article(id: "a", status: "draft", score: 100, title: "A", tags: []),
                article(id: "b", status: "published", score: 10, title: "B", tags: []),
                article(id: "c", status: "published", score: 90, title: "C", tags: []),
                article(id: "d", status: "published", score: 95, title: "D", tags: [])
            ],
            into: runtime
        )
        let query = DatabaseWireQueryRequest(
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

    @Test func queryPostFilterScansAcrossStorageBatchesBeforeLimit() throws {
        let storage = MemoryStorage()
        let runtime = DatabaseEngineRuntime(storage: storage, queryScanBatchSize: 2)
        try put(
            [
                article(id: "a", status: "draft", score: 1, title: "A", tags: []),
                article(id: "b", status: "draft", score: 2, title: "B", tags: []),
                article(id: "c", status: "draft", score: 3, title: "C", tags: []),
                article(id: "d", status: "draft", score: 4, title: "D", tags: []),
                article(id: "e", status: "published", score: 5, title: "E", tags: [])
            ],
            into: runtime
        )
        let query = DatabaseWireQueryRequest(
            typeName: "Article",
            predicate: .comparison(field: "status", op: .equal, value: .string("published")),
            limit: 1
        )

        let response = try runtime.execute(.query(query))

        #expect(response == .records([
            article(id: "e", status: "published", score: 5, title: "E", tags: [])
        ]))
    }

    @Test func querySupportsDisjunctionNegationAndContains() throws {
        let storage = MemoryStorage()
        let runtime = DatabaseEngineRuntime(storage: storage)
        try put(
            [
                article(id: "a", status: "draft", score: 1, title: "Local Swift", tags: ["swift"]),
                article(id: "b", status: "archived", score: 2, title: "Old Notes", tags: ["notes"]),
                article(id: "c", status: "published", score: 3, title: "Workers Runtime", tags: ["cloudflare"]),
                article(id: "d", status: "published", score: 4, title: "Durable Swift", tags: ["swift", "cloudflare"])
            ],
            into: runtime
        )
        let query = DatabaseWireQueryRequest(
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

    @Test func querySupportsComparisonOperatorVariants() throws {
        let storage = MemoryStorage()
        let runtime = DatabaseEngineRuntime(storage: storage)
        try put(
            [
                article(id: "a", status: "draft", score: 1, title: "A", tags: []),
                article(id: "b", status: "published", score: 2, title: "B", tags: []),
                article(id: "c", status: "archived", score: 3, title: "C", tags: [])
            ],
            into: runtime
        )

        #expect(try query(runtime, field: "score", op: .lessThan, value: .int64(2)) == [
            article(id: "a", status: "draft", score: 1, title: "A", tags: [])
        ])
        #expect(try query(runtime, field: "score", op: .lessThanOrEqual, value: .int64(2)) == [
            article(id: "a", status: "draft", score: 1, title: "A", tags: []),
            article(id: "b", status: "published", score: 2, title: "B", tags: [])
        ])
        #expect(try query(runtime, field: "score", op: .greaterThan, value: .int64(2)) == [
            article(id: "c", status: "archived", score: 3, title: "C", tags: [])
        ])
        #expect(try query(runtime, field: "status", op: .notEqual, value: .string("draft")) == [
            article(id: "b", status: "published", score: 2, title: "B", tags: []),
            article(id: "c", status: "archived", score: 3, title: "C", tags: [])
        ])
        #expect(try query(runtime, field: "missing", op: .equal, value: .string("value")) == [])
    }

    @Test func vectorQueryReturnsNearestRecordsWithDistancesAndPredicate() throws {
        let storage = MemoryStorage()
        let runtime = DatabaseEngineRuntime(storage: storage)
        _ = try runtime.execute(.applySchema(vectorSchema()))
        try put(
            [
                vectorDocument(id: "near", status: "published", title: "Near", embedding: [1, 0, 0]),
                vectorDocument(id: "middle", status: "published", title: "Middle", embedding: [0.8, 0.2, 0]),
                vectorDocument(id: "far", status: "published", title: "Far", embedding: [0, 1, 0]),
                vectorDocument(id: "draft-near", status: "draft", title: "Draft", embedding: [1, 0, 0])
            ],
            into: runtime
        )

        let response = try runtime.execute(.vectorQuery(DatabaseWireVectorQueryRequest(
            typeName: "Document",
            fieldName: "embedding",
            dimensions: 3,
            metric: .cosine,
            queryVector: [1, 0, 0],
            k: 2,
            predicate: .comparison(field: "status", op: .equal, value: .string("published"))
        )))

        guard case .scoredRecords(let records) = response else {
            Issue.record("Expected scored records response")
            return
        }
        #expect(records.map(\.record.id) == ["near", "middle"])
        #expect(records[0].distance == 0)
        #expect(records[1].distance > records[0].distance)
    }

    @Test func vectorQueryRequiresAppliedVectorSchema() throws {
        let storage = MemoryStorage()
        let runtime = DatabaseEngineRuntime(storage: storage)

        let responseBytes = try runtime.handle(
            DatabaseWireCodec.encode(request: .vectorQuery(DatabaseWireVectorQueryRequest(
                typeName: "Document",
                fieldName: "embedding",
                dimensions: 3,
                metric: .cosine,
                queryVector: [1, 0, 0],
                k: 1
            )))
        )

        #expect(try DatabaseWireCodec.decodeResponse(responseBytes) == .failure(
            status: .executionFailure,
            message: "schema not applied"
        ))
    }

    @Test func vectorQueryRejectsDimensionMismatch() throws {
        let storage = MemoryStorage()
        let runtime = DatabaseEngineRuntime(storage: storage)
        _ = try runtime.execute(.applySchema(vectorSchema()))

        let responseBytes = try runtime.handle(
            DatabaseWireCodec.encode(request: .vectorQuery(DatabaseWireVectorQueryRequest(
                typeName: "Document",
                fieldName: "embedding",
                dimensions: 3,
                metric: .cosine,
                queryVector: [1, 0],
                k: 1
            )))
        )

        #expect(try DatabaseWireCodec.decodeResponse(responseBytes) == .failure(
            status: .executionFailure,
            message: "query vector dimension mismatch. Expected: 3, Got: 2"
        ))
    }

    @Test func queryRejectsUnsupportedOrderedComparisons() throws {
        let storage = MemoryStorage()
        let runtime = DatabaseEngineRuntime(storage: storage)
        try put(
            [
                DatabaseWireRecord(
                    typeName: "Article",
                    id: "nan",
                    fields: [
                        DatabaseWireNamedValue(name: "score", value: .double(.nan))
                    ]
                )
            ],
            into: runtime
        )

        let responseBytes = try runtime.handle(
            DatabaseWireCodec.encode(
                request: .query(
                    DatabaseWireQueryRequest(
                        typeName: "Article",
                        predicate: .comparison(field: "score", op: .greaterThan, value: .double(1)),
                        limit: 10
                    )
                )
            )
        )

        #expect(try DatabaseWireCodec.decodeResponse(responseBytes) == .failure(
            status: .executionFailure,
            message: "unsupported predicate comparison"
        ))
    }

    @Test func malformedRequestReturnsInvalidRequestFailureEnvelope() throws {
        let storage = MemoryStorage()
        let runtime = DatabaseEngineRuntime(storage: storage)

        let responseBytes = try runtime.handle([DatabaseWireCodec.protocolVersion, 0xFF])
        let response = try DatabaseWireCodec.decodeResponse(responseBytes)

        #expect(response == .failure(status: .invalidRequest, message: "unknown operation"))
    }

    private func put(
        _ records: [DatabaseWireRecord],
        into runtime: DatabaseEngineRuntime<MemoryStorage>
    ) throws {
        for record in records {
            _ = try runtime.execute(.putRecord(record))
        }
    }

    private func query(
        _ runtime: DatabaseEngineRuntime<MemoryStorage>,
        field: String,
        op: DatabaseWireComparisonOperator,
        value: DatabaseWireFieldValue
    ) throws -> [DatabaseWireRecord] {
        let response = try runtime.execute(
            .query(
                DatabaseWireQueryRequest(
                    typeName: "Article",
                    predicate: .comparison(field: field, op: op, value: value),
                    limit: 10
                )
            )
        )
        guard case .records(let records) = response else {
            Issue.record("Expected records response")
            return []
        }
        return records
    }

    private func article(
        id: String,
        status: String,
        score: Int64,
        title: String,
        tags: [String]
    ) -> DatabaseWireRecord {
        DatabaseWireRecord(
            typeName: "Article",
            id: id,
            fields: [
                DatabaseWireNamedValue(name: "status", value: .string(status)),
                DatabaseWireNamedValue(name: "score", value: .int64(score)),
                DatabaseWireNamedValue(name: "title", value: .string(title)),
                DatabaseWireNamedValue(
                    name: "tags",
                    value: .array(tags.map { .string($0) })
                )
            ]
        )
    }

    private func vectorSchema() -> DatabaseWireSchema {
        DatabaseWireSchema(
            entities: [
                DatabaseWireEntitySchema(
                    typeName: "Document",
                    version: 1,
                    fields: [
                        DatabaseWireFieldSchema(name: "status", type: .string, isOptional: false, fieldNumber: 1),
                        DatabaseWireFieldSchema(name: "title", type: .string, isOptional: false, fieldNumber: 2),
                        DatabaseWireFieldSchema(name: "embedding", type: .array, isOptional: false, fieldNumber: 3)
                    ],
                    indexes: [
                        DatabaseWireIndexDescriptor(
                            name: "Document.embedding.vector",
                            kind: .vector,
                            fields: ["embedding"],
                            parameters: [
                                DatabaseWireNamedValue(name: "dimensions", value: .int64(3)),
                                DatabaseWireNamedValue(name: "metric", value: .string("cosine"))
                            ]
                        )
                    ]
                )
            ]
        )
    }

    private func vectorDocument(
        id: String,
        status: String,
        title: String,
        embedding: [Double]
    ) -> DatabaseWireRecord {
        DatabaseWireRecord(
            typeName: "Document",
            id: id,
            fields: [
                DatabaseWireNamedValue(name: "status", value: .string(status)),
                DatabaseWireNamedValue(name: "title", value: .string(title)),
                DatabaseWireNamedValue(
                    name: "embedding",
                    value: .array(embedding.map { .double($0) })
                )
            ]
        )
    }
}

private final class MemoryStorage: DatabaseStorage {
    private let rows = Mutex<[DatabaseKeyValue]>([])

    func read(key: [UInt8]) throws(DatabaseRuntimeError) -> [UInt8]? {
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
    ) throws(DatabaseRuntimeError) -> [DatabaseKeyValue] {
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

    func commit(_ writes: [DatabaseWriteOperation]) throws(DatabaseRuntimeError) {
        rows.withLock { rows in
            for write in writes {
                switch write {
                case .set(let key, let value):
                    upsert(DatabaseKeyValue(key: key, value: value), into: &rows)
                case .clear(let key):
                    rows.removeAll { $0.key == key }
                }
            }
        }
    }

    private func upsert(
        _ row: DatabaseKeyValue,
        into rows: inout [DatabaseKeyValue]
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
