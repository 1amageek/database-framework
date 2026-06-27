import DatabaseKitWasmCore

/// Executes DatabaseKit WASM requests against an abstract WASM storage boundary.
public struct DatabaseFrameworkWasmRuntime<Storage: DatabaseFrameworkWasmStorage>: Sendable {
    private let storage: Storage

    public init(storage: Storage) {
        self.storage = storage
    }

    public func handle(
        _ requestBytes: [UInt8]
    ) throws(DatabaseKitWasmWireError) -> [UInt8] {
        let response: DatabaseKitWasmResponse
        do {
            let request = try DatabaseKitWasmCodec.decodeRequest(requestBytes)
            response = try execute(request)
        } catch let error as DatabaseKitWasmWireError {
            response = .failure(status: .invalidRequest, message: message(for: error))
        } catch let error as DatabaseFrameworkWasmError {
            response = .failure(status: .executionFailure, message: message(for: error))
        } catch {
            response = .failure(status: .executionFailure, message: "wasm runtime execution failed")
        }
        return try DatabaseKitWasmCodec.encode(response: response)
    }

    public func execute(
        _ request: DatabaseKitWasmRequest
    ) throws(DatabaseFrameworkWasmError) -> DatabaseKitWasmResponse {
        switch request {
        case .applySchema(let schema):
            let operation: DatabaseKitWasmKeyValueOperation
            do {
                operation = try DatabaseKitWasmStorageBridge.schemaSetOperation(schema)
            } catch {
                throw .wire(error)
            }
            try apply(operation)
            return .empty
        case .putRecord(let record):
            let operation: DatabaseKitWasmKeyValueOperation
            do {
                operation = try DatabaseKitWasmStorageBridge.recordSetOperation(record)
            } catch {
                throw .wire(error)
            }
            try apply(operation)
            return .empty
        case .getRecord(let typeName, let id):
            let operation: DatabaseKitWasmKeyValueOperation
            do {
                operation = try DatabaseKitWasmStorageBridge.recordLookupOperation(
                    entityName: typeName,
                    id: id
                )
            } catch {
                throw .wire(error)
            }
            let value = try read(operation)
            if let value {
                do {
                    return .record(try DatabaseKitWasmStorageBridge.decodeRecordValue(value))
                } catch {
                    throw .wire(error)
                }
            }
            return .record(nil)
        case .query(let query):
            let plan: DatabaseKitWasmQueryPlan
            let limit: Int
            do {
                plan = try DatabaseKitWasmStorageBridge.queryPlan(query)
                limit = try limitValue(query.limit)
            } catch {
                throw .wire(error)
            }
            let rows = try scan(plan.operation)
            var records: [DatabaseKitWasmRecord] = []
            records.reserveCapacity(rows.count)
            for row in rows {
                let record: DatabaseKitWasmRecord
                do {
                    record = try DatabaseKitWasmStorageBridge.decodeRecordValue(row.value)
                } catch {
                    throw .wire(error)
                }
                if try DatabaseFrameworkWasmPredicateEvaluator.matches(
                    record,
                    predicate: plan.postFilter
                ) {
                    records.append(record)
                    if limit > 0 && records.count >= limit {
                        break
                    }
                }
            }
            return .records(records)
        }
    }

    private func read(
        _ operation: DatabaseKitWasmKeyValueOperation
    ) throws(DatabaseFrameworkWasmError) -> [UInt8]? {
        switch operation {
        case .get(let key):
            return try storage.read(key: key)
        case .range, .set, .clear:
            throw DatabaseFrameworkWasmError.unsupportedKeyValueOperation(operation)
        }
    }

    private func scan(
        _ operation: DatabaseKitWasmKeyValueOperation
    ) throws(DatabaseFrameworkWasmError) -> [DatabaseFrameworkWasmKeyValue] {
        switch operation {
        case .range(let begin, let end, let limit, let reverse):
            return try storage.scan(
                begin: begin,
                end: end,
                limit: limit,
                reverse: reverse
            )
        case .get, .set, .clear:
            throw DatabaseFrameworkWasmError.unsupportedKeyValueOperation(operation)
        }
    }

    private func apply(
        _ operation: DatabaseKitWasmKeyValueOperation
    ) throws(DatabaseFrameworkWasmError) {
        switch operation {
        case .set(let key, let value):
            try storage.commit([.set(key: key, value: value)])
        case .clear(let key):
            try storage.commit([.clear(key: key)])
        case .get, .range:
            throw DatabaseFrameworkWasmError.unsupportedKeyValueOperation(operation)
        }
    }

    private func message(for error: DatabaseKitWasmWireError) -> String {
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
        case .unsupportedPredicatePlan:
            return "unsupported predicate plan"
        }
    }

    private func limitValue(_ value: UInt32) throws(DatabaseKitWasmWireError) -> Int {
        guard UInt64(value) <= UInt64(Int.max) else {
            throw DatabaseKitWasmWireError.byteCountOverflow
        }
        return Int(value)
    }

    private func message(for error: DatabaseFrameworkWasmError) -> String {
        switch error {
        case .wire(let error):
            return message(for: error)
        case .unsupportedKeyValueOperation:
            return "unsupported key-value operation"
        case .unsupportedPredicateComparison:
            return "unsupported predicate comparison"
        }
    }
}
