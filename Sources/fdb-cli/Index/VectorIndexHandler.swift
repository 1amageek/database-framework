import Foundation
import FoundationDB

/// Handler for vector indexes (K-NN similarity search)
///
/// Storage layout:
/// - vectors/<id> = packed float array
/// - For HNSW: additional graph structure keys
public struct VectorIndexHandler: IndexHandler, Sendable {
    public let indexDefinition: IndexDefinition
    public let schemaName: String

    public init(indexDefinition: IndexDefinition, schemaName: String) {
        self.indexDefinition = indexDefinition
        self.schemaName = schemaName
    }

    public func updateIndex(
        oldItem: [String: Any]?,
        newItem: [String: Any]?,
        id: String,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws {
        guard let config = indexDefinition.config,
              case .vector(let vectorConfig) = config else {
            return
        }

        let field = indexDefinition.fields.first ?? ""
        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .vector,
            indexName: indexDefinition.name
        )
        let vectorsSubspace = indexSubspace.subspace(Tuple(["vectors"]))

        let oldVector = extractVector(from: oldItem, field: field)
        let newVector = extractVector(from: newItem, field: field)

        // Remove old vector
        if oldVector != nil {
            let oldKey = vectorsSubspace.pack(Tuple([id]))
            transaction.clear(key: oldKey)
        }

        // Add new vector
        if let vector = newVector {
            guard vector.count == vectorConfig.dimensions else {
                throw VectorIndexError.dimensionMismatch(
                    expected: vectorConfig.dimensions,
                    got: vector.count
                )
            }

            let vectorKey = vectorsSubspace.pack(Tuple([id]))
            let packedVector = packVector(vector)
            transaction.setValue(packedVector, for: vectorKey)
        }
    }

    public func scan(
        query: Any,
        limit: Int,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws -> [String] {
        guard let config = indexDefinition.config,
              case .vector(let vectorConfig) = config else {
            return []
        }

        guard let vectorQuery = query as? VectorQuery else {
            return []
        }

        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .vector,
            indexName: indexDefinition.name
        )
        let vectorsSubspace = indexSubspace.subspace(Tuple(["vectors"]))

        // Flat scan for now (brute force K-NN)
        // TODO: Implement HNSW graph traversal for large datasets
        var candidates: [(id: String, distance: Float)] = []

        let (begin, end) = vectorsSubspace.range()
        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

        for try await (key, value) in sequence {
            if let tuple = try? vectorsSubspace.unpack(key),
               let id = tuple[0] as? String {
                let vector = unpackVector(value)
                let distance = computeDistance(
                    vectorQuery.vector,
                    vector,
                    metric: vectorConfig.metric
                )
                candidates.append((id: id, distance: distance))
            }
        }

        // Sort by distance and take top K
        candidates.sort { $0.distance < $1.distance }
        return candidates.prefix(vectorQuery.k).map { $0.id }
    }

    // MARK: - Vector Operations

    private func extractVector(from item: [String: Any]?, field: String) -> [Float]? {
        guard let item = item,
              let array = item[field] as? [Any] else {
            return nil
        }

        return array.compactMap { element -> Float? in
            if let d = element as? Double { return Float(d) }
            if let i = element as? Int { return Float(i) }
            if let f = element as? Float { return f }
            return nil
        }
    }

    private func packVector(_ vector: [Float]) -> FDB.Bytes {
        var bytes: [UInt8] = []
        bytes.reserveCapacity(vector.count * 4)

        for value in vector {
            var v = value
            let valueBytes = withUnsafeBytes(of: &v) { Array($0) }
            bytes.append(contentsOf: valueBytes)
        }

        return bytes
    }

    private func unpackVector(_ bytes: FDB.Bytes) -> [Float] {
        var result: [Float] = []
        result.reserveCapacity(bytes.count / 4)

        var offset = 0
        while offset + 4 <= bytes.count {
            let valueBytes = Array(bytes[offset..<offset+4])
            let value = valueBytes.withUnsafeBytes { $0.load(as: Float.self) }
            result.append(value)
            offset += 4
        }

        return result
    }

    private func computeDistance(_ a: [Float], _ b: [Float], metric: VectorMetric) -> Float {
        guard a.count == b.count else { return Float.infinity }

        switch metric {
        case .cosine:
            return 1.0 - cosineSimilarity(a, b)
        case .euclidean:
            return euclideanDistance(a, b)
        case .dotProduct:
            return -dotProduct(a, b) // Negative because we want max similarity
        }
    }

    private func cosineSimilarity(_ a: [Float], _ b: [Float]) -> Float {
        var dot: Float = 0
        var normA: Float = 0
        var normB: Float = 0

        for i in 0..<a.count {
            dot += a[i] * b[i]
            normA += a[i] * a[i]
            normB += b[i] * b[i]
        }

        let denom = sqrt(normA) * sqrt(normB)
        return denom > 0 ? dot / denom : 0
    }

    private func euclideanDistance(_ a: [Float], _ b: [Float]) -> Float {
        var sum: Float = 0
        for i in 0..<a.count {
            let diff = a[i] - b[i]
            sum += diff * diff
        }
        return sqrt(sum)
    }

    private func dotProduct(_ a: [Float], _ b: [Float]) -> Float {
        var sum: Float = 0
        for i in 0..<a.count {
            sum += a[i] * b[i]
        }
        return sum
    }
}

// MARK: - Vector Query

public struct VectorQuery {
    public let vector: [Float]
    public let k: Int
    public let metric: VectorMetric

    public init(vector: [Float], k: Int, metric: VectorMetric = .cosine) {
        self.vector = vector
        self.k = k
        self.metric = metric
    }
}

// MARK: - Errors

public enum VectorIndexError: Error, CustomStringConvertible {
    case dimensionMismatch(expected: Int, got: Int)

    public var description: String {
        switch self {
        case .dimensionMismatch(let expected, let got):
            return "Vector dimension mismatch: expected \(expected), got \(got)"
        }
    }
}
