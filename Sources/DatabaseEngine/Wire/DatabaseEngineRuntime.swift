import DatabaseWire

/// Executes DatabaseWire requests against an abstract storage boundary.
public struct DatabaseEngineRuntime<Storage: DatabaseStorage>: Sendable {
    private let queryScanBatchSize: Int
    private let storage: Storage

    public init(storage: Storage, queryScanBatchSize: Int = 1024) {
        self.storage = storage
        self.queryScanBatchSize = max(1, queryScanBatchSize)
    }

    public func handle(
        _ requestBytes: [UInt8]
    ) throws(DatabaseWireError) -> [UInt8] {
        let response: DatabaseWireResponse
        do {
            let request = try DatabaseWireCodec.decodeRequest(requestBytes)
            response = try execute(request)
        } catch let error as DatabaseWireError {
            response = .failure(status: .invalidRequest, message: message(for: error))
        } catch let error as DatabaseRuntimeError {
            response = .failure(status: .executionFailure, message: message(for: error))
        } catch {
            response = .failure(status: .executionFailure, message: "wire runtime execution failed")
        }
        return try DatabaseWireCodec.encode(response: response)
    }

    public func execute(
        _ request: DatabaseWireRequest
    ) throws(DatabaseRuntimeError) -> DatabaseWireResponse {
        switch request {
        case .applySchema(let schema):
            let operation: DatabaseWireKeyValueOperation
            do {
                operation = try DatabaseWireStorageBridge.schemaSetOperation(schema)
            } catch {
                throw .wire(error)
            }
            try apply(operation)
            return .empty
        case .putRecord(let record):
            let operation: DatabaseWireKeyValueOperation
            do {
                operation = try DatabaseWireStorageBridge.recordSetOperation(record)
            } catch {
                throw .wire(error)
            }
            try apply(operation)
            return .empty
        case .getRecord(let typeName, let id):
            let operation: DatabaseWireKeyValueOperation
            do {
                operation = try DatabaseWireStorageBridge.recordLookupOperation(
                    entityName: typeName,
                    id: id
                )
            } catch {
                throw .wire(error)
            }
            let value = try read(operation)
            if let value {
                do {
                    return .record(try DatabaseWireStorageBridge.decodeRecordValue(value))
                } catch {
                    throw .wire(error)
                }
            }
            return .record(nil)
        case .query(let query):
            let plan: DatabaseWireQueryPlan
            let limit: Int
            do {
                plan = try DatabaseWireStorageBridge.queryPlan(query)
                limit = try limitValue(query.limit)
            } catch {
                throw .wire(error)
            }
            var records: [DatabaseWireRecord] = []
            if limit > 0 {
                records.reserveCapacity(limit)
            }
            try appendMatchingRecords(
                plan.operation,
                predicate: plan.postFilter,
                limit: limit,
                into: &records
            )
            return .records(records)
        case .vectorQuery(let query):
            let executor = DatabaseVectorQueryExecutor(
                storage: storage,
                queryScanBatchSize: queryScanBatchSize
            )
            return .scoredRecords(try executor.execute(query))
        }
    }

    private func appendMatchingRecords(
        _ operation: DatabaseWireKeyValueOperation,
        predicate: DatabaseWirePredicate?,
        limit: Int,
        into records: inout [DatabaseWireRecord]
    ) throws(DatabaseRuntimeError) {
        switch operation {
        case .range(let begin, let end, let scanLimit, let reverse):
            if scanLimit > 0 || reverse {
                for row in try storage.scan(begin: begin, end: end, limit: scanLimit, reverse: reverse) {
                    if try append(row, predicate: predicate, limit: limit, into: &records) {
                        return
                    }
                }
                return
            }
            var nextBegin = begin
            while lexicographicCompare(nextBegin, end) < 0 {
                let rows = try storage.scan(
                    begin: nextBegin,
                    end: end,
                    limit: queryScanBatchSize,
                    reverse: false
                )
                guard !rows.isEmpty else {
                    return
                }
                for row in rows {
                    if try append(row, predicate: predicate, limit: limit, into: &records) {
                        return
                    }
                }
                guard rows.count >= queryScanBatchSize, let lastKey = rows.last?.key else {
                    return
                }
                nextBegin = keyAfter(lastKey)
            }
        case .get, .set, .clear:
            throw DatabaseRuntimeError.unsupportedKeyValueOperation(operation)
        }
    }

    private func append(
        _ row: DatabaseKeyValue,
        predicate: DatabaseWirePredicate?,
        limit: Int,
        into records: inout [DatabaseWireRecord]
    ) throws(DatabaseRuntimeError) -> Bool {
        let record = try decodeRecord(row.value)
        guard try DatabasePredicateEvaluator.matches(record, predicate: predicate) else {
            return false
        }
        records.append(record)
        return limit > 0 && records.count >= limit
    }

    private func decodeRecord(_ value: [UInt8]) throws(DatabaseRuntimeError) -> DatabaseWireRecord {
        do {
            return try DatabaseWireStorageBridge.decodeRecordValue(value)
        } catch {
            throw .wire(error)
        }
    }

    private func read(
        _ operation: DatabaseWireKeyValueOperation
    ) throws(DatabaseRuntimeError) -> [UInt8]? {
        switch operation {
        case .get(let key):
            return try storage.read(key: key)
        case .range, .set, .clear:
            throw DatabaseRuntimeError.unsupportedKeyValueOperation(operation)
        }
    }

    private func scan(
        _ operation: DatabaseWireKeyValueOperation
    ) throws(DatabaseRuntimeError) -> [DatabaseKeyValue] {
        switch operation {
        case .range(let begin, let end, let limit, let reverse):
            return try storage.scan(
                begin: begin,
                end: end,
                limit: limit,
                reverse: reverse
            )
        case .get, .set, .clear:
            throw DatabaseRuntimeError.unsupportedKeyValueOperation(operation)
        }
    }

    private func apply(
        _ operation: DatabaseWireKeyValueOperation
    ) throws(DatabaseRuntimeError) {
        switch operation {
        case .set(let key, let value):
            try storage.commit([.set(key: key, value: value)])
        case .clear(let key):
            try storage.commit([.clear(key: key)])
        case .get, .range:
            throw DatabaseRuntimeError.unsupportedKeyValueOperation(operation)
        }
    }

    private func message(for error: DatabaseWireError) -> String {
        switch error {
        case .truncated:
            return "truncated request"
        case .byteCountOverflow:
            return "byte count overflow"
        case .invalidBool:
            return "invalid boolean"
        case .invalidUTF8:
            return "invalid utf8"
        case .trailingBytes:
            return "trailing bytes"
        case .unsupportedProtocolVersion:
            return "unsupported protocol version"
        case .unknownOperation:
            return "unknown operation"
        case .unknownResponseStatus:
            return "unknown response status"
        case .unknownResponsePayload:
            return "unknown response payload"
        case .unknownFieldType:
            return "unknown field type"
        case .unknownFieldValue:
            return "unknown field value"
        case .unknownIndexKind:
            return "unknown index kind"
        case .unknownComparisonOperator:
            return "unknown comparison operator"
        case .unknownPredicate:
            return "unknown predicate"
        case .unknownVectorMetric:
            return "unknown vector metric"
        case .unsupportedPredicatePlan:
            return "unsupported predicate plan"
        }
    }

    private func limitValue(_ value: UInt32) throws(DatabaseWireError) -> Int {
        guard UInt64(value) <= UInt64(Int.max) else {
            throw DatabaseWireError.byteCountOverflow
        }
        return Int(value)
    }

    private func message(for error: DatabaseRuntimeError) -> String {
        switch error {
        case .wire(let error):
            return message(for: error)
        case .unsupportedKeyValueOperation:
            return "unsupported key-value operation"
        case .unsupportedPredicateComparison:
            return "unsupported predicate comparison"
        case .invalidVectorQuery(let message):
            return message
        case .storageFailure(let message):
            return message
        case .invalidStorageResponse(let message):
            return message
        }
    }

    private func keyAfter(_ key: [UInt8]) -> [UInt8] {
        key + [0x00]
    }

    private func lexicographicCompare(_ lhs: [UInt8], _ rhs: [UInt8]) -> Int {
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
