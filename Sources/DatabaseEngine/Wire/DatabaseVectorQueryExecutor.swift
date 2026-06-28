import DatabaseWire

/// Executes exact vector search for the storage-neutral wire runtime.
struct DatabaseVectorQueryExecutor<Storage: DatabaseStorage>: Sendable {
    private let queryScanBatchSize: Int
    private let storage: Storage

    init(storage: Storage, queryScanBatchSize: Int) {
        self.storage = storage
        self.queryScanBatchSize = max(1, queryScanBatchSize)
    }

    func execute(
        _ query: DatabaseWireVectorQueryRequest
    ) throws(DatabaseRuntimeError) -> [DatabaseWireScoredRecord] {
        let dimensions = try checkedPositiveInt(
            query.dimensions,
            name: "dimensions"
        )
        let k = try checkedPositiveInt(query.k, name: "k")
        try validateQueryVector(query.queryVector, dimensions: dimensions)
        try validateSchema(for: query, dimensions: dimensions)

        let operation: DatabaseWireKeyValueOperation
        do {
            operation = try DatabaseWireStorageBridge.entityScanOperation(
                entityName: query.typeName,
                limit: 0,
                reverse: false
            )
        } catch {
            throw .wire(error)
        }

        var results: [DatabaseWireScoredRecord] = []
        results.reserveCapacity(k)
        try appendNearestRecords(
            operation,
            query: query,
            dimensions: dimensions,
            k: k,
            into: &results
        )
        return results.sorted(by: isBetter)
    }

    private func appendNearestRecords(
        _ operation: DatabaseWireKeyValueOperation,
        query: DatabaseWireVectorQueryRequest,
        dimensions: Int,
        k: Int,
        into results: inout [DatabaseWireScoredRecord]
    ) throws(DatabaseRuntimeError) {
        guard case .range(let begin, let end, _, _) = operation else {
            throw DatabaseRuntimeError.unsupportedKeyValueOperation(operation)
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
                try append(row, query: query, dimensions: dimensions, k: k, into: &results)
            }
            guard rows.count >= queryScanBatchSize, let lastKey = rows.last?.key else {
                return
            }
            nextBegin = keyAfter(lastKey)
        }
    }

    private func append(
        _ row: DatabaseKeyValue,
        query: DatabaseWireVectorQueryRequest,
        dimensions: Int,
        k: Int,
        into results: inout [DatabaseWireScoredRecord]
    ) throws(DatabaseRuntimeError) {
        let record = try decodeRecord(row.value)
        guard try DatabasePredicateEvaluator.matches(record, predicate: query.predicate) else {
            return
        }
        guard let vector = try vectorValue(
            named: query.fieldName,
            in: record,
            dimensions: dimensions
        ) else {
            return
        }

        let distance = distance(
            query.queryVector,
            vector,
            metric: query.metric
        )
        let scored = DatabaseWireScoredRecord(record: record, distance: distance)
        appendCandidate(scored, k: k, into: &results)
    }

    private func validateSchema(
        for query: DatabaseWireVectorQueryRequest,
        dimensions: Int
    ) throws(DatabaseRuntimeError) {
        guard let schema = try loadSchema() else {
            throw .invalidVectorQuery("schema not applied")
        }
        guard let entity = schema.entities.first(where: { $0.typeName == query.typeName }) else {
            throw .invalidVectorQuery("entity schema not found: \(query.typeName)")
        }
        guard let descriptor = entity.indexes.first(where: {
            $0.kind == .vector && $0.fields == [query.fieldName]
        }) else {
            throw .invalidVectorQuery("vector index not found: \(query.typeName).\(query.fieldName)")
        }
        try validateDimensionsParameter(descriptor, dimensions: dimensions)
        try validateMetricParameter(descriptor, metric: query.metric)
    }

    private func loadSchema() throws(DatabaseRuntimeError) -> DatabaseWireSchema? {
        guard let bytes = try storage.read(key: DatabaseWireStorageBridge.schemaKey()) else {
            return nil
        }
        do {
            return try DatabaseWireCodec.decodeSchema(bytes)
        } catch {
            throw .wire(error)
        }
    }

    private func validateDimensionsParameter(
        _ descriptor: DatabaseWireIndexDescriptor,
        dimensions: Int
    ) throws(DatabaseRuntimeError) {
        guard let value = parameter(named: "dimensions", in: descriptor) else {
            throw .invalidVectorQuery("vector index dimensions parameter missing")
        }
        guard case .int64(let declared) = value else {
            throw .invalidVectorQuery("vector index dimensions parameter must be int64")
        }
        guard declared == Int64(dimensions) else {
            throw .invalidVectorQuery("vector dimensions mismatch. Expected: \(declared), Got: \(dimensions)")
        }
    }

    private func validateMetricParameter(
        _ descriptor: DatabaseWireIndexDescriptor,
        metric: DatabaseWireVectorMetric
    ) throws(DatabaseRuntimeError) {
        guard let value = parameter(named: "metric", in: descriptor) else {
            throw .invalidVectorQuery("vector index metric parameter missing")
        }
        guard case .string(let declared) = value else {
            throw .invalidVectorQuery("vector index metric parameter must be string")
        }
        guard declared == metric.parameterValue else {
            throw .invalidVectorQuery("vector metric mismatch. Expected: \(declared), Got: \(metric.parameterValue)")
        }
    }

    private func parameter(
        named name: String,
        in descriptor: DatabaseWireIndexDescriptor
    ) -> DatabaseWireFieldValue? {
        descriptor.parameters.first(where: { $0.name == name })?.value
    }

    private func validateQueryVector(
        _ vector: [Double],
        dimensions: Int
    ) throws(DatabaseRuntimeError) {
        guard vector.count == dimensions else {
            throw .invalidVectorQuery("query vector dimension mismatch. Expected: \(dimensions), Got: \(vector.count)")
        }
        for value in vector {
            guard value.isFinite else {
                throw .invalidVectorQuery("query vector contains a non-finite value")
            }
        }
    }

    private func vectorValue(
        named fieldName: String,
        in record: DatabaseWireRecord,
        dimensions: Int
    ) throws(DatabaseRuntimeError) -> [Double]? {
        guard let value = record.fields.first(where: { $0.name == fieldName })?.value else {
            return nil
        }
        switch value {
        case .null:
            return nil
        case .array(let values):
            var vector: [Double] = []
            vector.reserveCapacity(values.count)
            for value in values {
                switch value {
                case .double(let scalar):
                    guard scalar.isFinite else {
                        throw .invalidVectorQuery("vector field contains a non-finite value: \(fieldName)")
                    }
                    vector.append(scalar)
                case .int64(let scalar):
                    vector.append(Double(scalar))
                default:
                    throw .invalidVectorQuery("vector field must contain only numeric values: \(fieldName)")
                }
            }
            guard vector.count == dimensions else {
                throw .invalidVectorQuery("vector field dimension mismatch. Expected: \(dimensions), Got: \(vector.count)")
            }
            return vector
        default:
            throw .invalidVectorQuery("vector field must be an array: \(fieldName)")
        }
    }

    private func distance(
        _ lhs: [Double],
        _ rhs: [Double],
        metric: DatabaseWireVectorMetric
    ) -> Double {
        switch metric {
        case .cosine:
            return cosineDistance(lhs, rhs)
        case .euclidean:
            return euclideanDistance(lhs, rhs)
        case .dotProduct:
            return dotProductDistance(lhs, rhs)
        }
    }

    private func cosineDistance(_ lhs: [Double], _ rhs: [Double]) -> Double {
        var dotProduct = 0.0
        var lhsNorm = 0.0
        var rhsNorm = 0.0
        for index in lhs.indices {
            dotProduct += lhs[index] * rhs[index]
            lhsNorm += lhs[index] * lhs[index]
            rhsNorm += rhs[index] * rhs[index]
        }
        let denominator = lhsNorm.squareRoot() * rhsNorm.squareRoot()
        guard denominator > 0 else {
            return 2.0
        }
        return 1.0 - (dotProduct / denominator)
    }

    private func euclideanDistance(_ lhs: [Double], _ rhs: [Double]) -> Double {
        var sum = 0.0
        for index in lhs.indices {
            let difference = lhs[index] - rhs[index]
            sum += difference * difference
        }
        return sum.squareRoot()
    }

    private func dotProductDistance(_ lhs: [Double], _ rhs: [Double]) -> Double {
        var dotProduct = 0.0
        for index in lhs.indices {
            dotProduct += lhs[index] * rhs[index]
        }
        return -dotProduct
    }

    private func appendCandidate(
        _ candidate: DatabaseWireScoredRecord,
        k: Int,
        into results: inout [DatabaseWireScoredRecord]
    ) {
        if results.count < k {
            results.append(candidate)
            return
        }
        guard let worstIndex = results.indices.max(by: { isBetter(results[$0], results[$1]) }) else {
            return
        }
        if isBetter(candidate, results[worstIndex]) {
            results[worstIndex] = candidate
        }
    }

    private func isBetter(
        _ lhs: DatabaseWireScoredRecord,
        _ rhs: DatabaseWireScoredRecord
    ) -> Bool {
        if lhs.distance == rhs.distance {
            return lhs.record.id < rhs.record.id
        }
        return lhs.distance < rhs.distance
    }

    private func checkedPositiveInt(
        _ value: UInt32,
        name: String
    ) throws(DatabaseRuntimeError) -> Int {
        guard value > 0 else {
            throw .invalidVectorQuery("\(name) must be positive")
        }
        guard UInt64(value) <= UInt64(Int.max) else {
            throw .wire(.byteCountOverflow)
        }
        return Int(value)
    }

    private func decodeRecord(_ value: [UInt8]) throws(DatabaseRuntimeError) -> DatabaseWireRecord {
        do {
            return try DatabaseWireStorageBridge.decodeRecordValue(value)
        } catch {
            throw .wire(error)
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

private extension DatabaseWireVectorMetric {
    var parameterValue: String {
        switch self {
        case .cosine:
            return "cosine"
        case .euclidean:
            return "euclidean"
        case .dotProduct:
            return "dotProduct"
        }
    }
}
