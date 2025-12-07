// CodebookTrainer.swift
// VectorIndex/Quantization - Codebook training and persistence
//
// Trains vector quantizers on sample data and manages codebook storage.
//
// References:
// - Product Quantization: Jégou et al., "Product Quantization for Nearest Neighbor Search", IEEE TPAMI 2011

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Vector

// MARK: - CodebookTrainer

/// Trains and manages vector quantizer codebooks
///
/// **Training Workflow**:
/// 1. Create quantizer with configuration
/// 2. Sample vectors from the database
/// 3. Train quantizer on samples
/// 4. Save codebook to database
///
/// **Usage**:
/// ```swift
/// // Create and train quantizer
/// var pq = ProductQuantizer(config: .forDimensions(384), dimensions: 384)
/// var trainer = CodebookTrainer<Product, ProductQuantizer>(
///     keyPath: \.embedding,
///     quantizer: pq
/// )
///
/// // Train on sample data
/// try await trainer.train(sampleSize: 10000, context: ctx)
///
/// // Save for persistence
/// try await trainer.saveCodebook(context: ctx)
///
/// // Later: load from database
/// try await trainer.loadCodebook(context: ctx)
/// ```
///
/// **Storage Layout**:
/// ```
/// [indexSubspace]/_meta/codebook/[quantizerType] → Serialized codebook data
/// ```
///
/// **Note**: `@unchecked Sendable` is used because `KeyPath` is immutable and thread-safe.
public struct CodebookTrainer<T: Persistable, Q: VectorQuantizer>: @unchecked Sendable {
    private let fieldName: String
    private var quantizer: Q

    // MARK: - Initialization

    /// Create a CodebookTrainer for a vector field
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the vector field
    ///   - quantizer: Quantizer instance to train
    public init(keyPath: KeyPath<T, [Float]>, quantizer: Q) {
        self.fieldName = T.fieldName(for: keyPath)
        self.quantizer = quantizer
    }

    /// Create a CodebookTrainer for an optional vector field
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the optional vector field
    ///   - quantizer: Quantizer instance to train
    public init(keyPath: KeyPath<T, [Float]?>, quantizer: Q) {
        self.fieldName = T.fieldName(for: keyPath)
        self.quantizer = quantizer
    }

    // MARK: - Training

    /// Train quantizer on sample vectors from the database
    ///
    /// Samples vectors using reservoir sampling for uniform distribution.
    ///
    /// - Parameters:
    ///   - sampleSize: Number of vectors to sample for training
    ///   - context: IndexQueryContext for database access
    /// - Throws: If training fails or insufficient samples
    ///
    /// **Training Recommendations**:
    /// - PQ: 10,000-50,000 samples for good codebook quality
    /// - SQ: 1,000-10,000 samples for min/max estimation
    /// - BQ: No training required (but can optimize with samples)
    public mutating func train(sampleSize: Int, context: IndexQueryContext) async throws {
        // Sample vectors from index
        let samples = try await sampleVectors(count: sampleSize, context: context)

        guard !samples.isEmpty else {
            throw CodebookTrainerError.insufficientSamples(requested: sampleSize, available: 0)
        }

        // Train quantizer
        try await quantizer.train(vectors: samples)
    }

    /// Train quantizer on provided vectors directly
    ///
    /// Use this when you already have vectors in memory.
    ///
    /// - Parameter vectors: Training vectors
    /// - Throws: If training fails
    public mutating func train(vectors: [[Float]]) async throws {
        try await quantizer.train(vectors: vectors)
    }

    // MARK: - Persistence

    /// Save trained codebook to database
    ///
    /// Storage key: `[indexSubspace]/_meta/codebook/[quantizerType]`
    ///
    /// - Parameter context: IndexQueryContext for database access
    /// - Throws: If quantizer not trained or serialization fails
    public func saveCodebook(context: IndexQueryContext) async throws {
        guard quantizer.isTrained else {
            throw QuantizerError.notTrained
        }

        // Find index descriptor
        guard let descriptor = findIndexDescriptor() else {
            throw CodebookTrainerError.indexNotFound(field: fieldName)
        }

        let indexName = descriptor.name
        let codebookData = try quantizer.serialize()

        // Get index subspace
        let typeSubspace = try await context.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)
        let metaSubspace = indexSubspace.subspace("_meta").subspace("codebook")

        // Store codebook with quantizer type as key
        let quantizerType = String(describing: type(of: quantizer))
            .components(separatedBy: "<").first ?? "unknown"

        let codebookBytes = Array(codebookData)
        try await context.withTransaction { transaction in
            let key = metaSubspace.pack(Tuple(quantizerType))
            transaction.setValue(codebookBytes, for: key)
        }
    }

    /// Load codebook from database
    ///
    /// - Parameter context: IndexQueryContext for database access
    /// - Throws: If codebook not found or deserialization fails
    public mutating func loadCodebook(context: IndexQueryContext) async throws {
        // Find index descriptor
        guard let descriptor = findIndexDescriptor() else {
            throw CodebookTrainerError.indexNotFound(field: fieldName)
        }

        let indexName = descriptor.name

        // Get index subspace
        let typeSubspace = try await context.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)
        let metaSubspace = indexSubspace.subspace("_meta").subspace("codebook")

        // Get quantizer type
        let quantizerType = String(describing: type(of: quantizer))
            .components(separatedBy: "<").first ?? "unknown"

        // Load codebook
        let codebookBytes: [UInt8]? = try await context.withTransaction { transaction in
            let key = metaSubspace.pack(Tuple(quantizerType))
            return try await transaction.getValue(for: key, snapshot: true)
        }

        guard let bytes = codebookBytes else {
            throw CodebookTrainerError.codebookNotFound(quantizerType: quantizerType)
        }

        try quantizer.deserialize(from: Data(bytes))
    }

    /// Get the trained quantizer
    ///
    /// - Returns: The quantizer (trained if training was successful)
    public var trainedQuantizer: Q {
        quantizer
    }

    // MARK: - Sampling

    /// Sample vectors from the index using reservoir sampling
    ///
    /// Uses Algorithm R (Vitter, 1985) for uniform random sampling.
    ///
    /// - Parameters:
    ///   - count: Number of vectors to sample
    ///   - context: IndexQueryContext for database access
    /// - Returns: Sampled vectors
    private func sampleVectors(count: Int, context: IndexQueryContext) async throws -> [[Float]] {
        // Find index descriptor
        guard let descriptor = findIndexDescriptor() else {
            throw CodebookTrainerError.indexNotFound(field: fieldName)
        }

        guard let kind = descriptor.kind as? VectorIndexKind<T> else {
            throw CodebookTrainerError.indexNotFound(field: fieldName)
        }

        let dimensions = kind.dimensions
        let indexName = descriptor.name

        // Get index subspace
        let typeSubspace = try await context.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Perform reservoir sampling within transaction and return result
        let sampleCount = count
        let vectorDimensions = dimensions

        let samples: [[Float]] = try await context.withTransaction { transaction in
            let (begin, end) = indexSubspace.range()
            let sequence = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(begin),
                endSelector: .firstGreaterOrEqual(end),
                snapshot: true
            )

            // Reservoir sampling state (local to transaction)
            var reservoir: [[Float]] = []
            reservoir.reserveCapacity(sampleCount)
            var seen = 0

            for try await (key, value) in sequence {
                // Skip metadata keys
                if let keyStr = String(data: Data(key), encoding: .utf8),
                   keyStr.contains("_meta") || keyStr.contains("hnsw") {
                    continue
                }

                // Decode vector
                guard let vectorTuple = try? Tuple.unpack(from: value) else {
                    continue
                }

                var vector: [Float] = []
                vector.reserveCapacity(vectorDimensions)
                var isValid = true

                for i in 0..<vectorDimensions {
                    guard i < vectorTuple.count else {
                        isValid = false
                        break
                    }

                    let element = vectorTuple[i]
                    if let f = element as? Float {
                        vector.append(f)
                    } else if let d = element as? Double {
                        vector.append(Float(d))
                    } else if let i64 = element as? Int64 {
                        vector.append(Float(i64))
                    } else if let i = element as? Int {
                        vector.append(Float(i))
                    } else {
                        isValid = false
                        break
                    }
                }

                guard isValid && vector.count == vectorDimensions else { continue }

                // Reservoir sampling: Algorithm R
                if reservoir.count < sampleCount {
                    reservoir.append(vector)
                } else {
                    // Randomly replace with decreasing probability
                    let j = Int.random(in: 0...seen)
                    if j < sampleCount {
                        reservoir[j] = vector
                    }
                }

                seen += 1
            }

            return reservoir
        }

        return samples
    }

    // MARK: - Index Discovery

    /// Find the index descriptor for the vector field
    private func findIndexDescriptor() -> IndexDescriptor? {
        T.indexDescriptors.first { descriptor in
            guard descriptor.kindIdentifier == VectorIndexKind<T>.identifier else {
                return false
            }
            guard let kind = descriptor.kind as? VectorIndexKind<T> else {
                return false
            }
            return kind.fieldNames.contains(fieldName)
        }
    }
}

// MARK: - CodebookTrainerError

/// Errors from CodebookTrainer operations
public enum CodebookTrainerError: Error, CustomStringConvertible, Sendable {
    case indexNotFound(field: String)
    case insufficientSamples(requested: Int, available: Int)
    case codebookNotFound(quantizerType: String)
    case saveFailed(String)
    case loadFailed(String)

    public var description: String {
        switch self {
        case .indexNotFound(let field):
            return "Vector index not found for field: \(field)"
        case .insufficientSamples(let requested, let available):
            return "Insufficient samples: requested \(requested), available \(available)"
        case .codebookNotFound(let quantizerType):
            return "Codebook not found for quantizer type: \(quantizerType)"
        case .saveFailed(let message):
            return "Failed to save codebook: \(message)"
        case .loadFailed(let message):
            return "Failed to load codebook: \(message)"
        }
    }
}

// MARK: - Convenience Extensions

extension CodebookTrainer where Q == ProductQuantizer {
    /// Create a CodebookTrainer with ProductQuantizer optimized for dimensions
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the vector field
    ///   - dimensions: Vector dimensions
    ///   - config: PQ configuration (default: optimized for dimensions)
    public static func productQuantizer(
        keyPath: KeyPath<T, [Float]>,
        dimensions: Int,
        config: PQConfig? = nil
    ) -> CodebookTrainer<T, ProductQuantizer> {
        let pqConfig = config ?? .forDimensions(dimensions)
        let pq = ProductQuantizer(config: pqConfig, dimensions: dimensions)
        return CodebookTrainer(keyPath: keyPath, quantizer: pq)
    }
}

extension CodebookTrainer where Q == ScalarQuantizer {
    /// Create a CodebookTrainer with ScalarQuantizer
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the vector field
    ///   - dimensions: Vector dimensions
    ///   - config: SQ configuration (default: 8-bit)
    ///   - metric: Vector metric (default: euclidean)
    public static func scalarQuantizer(
        keyPath: KeyPath<T, [Float]>,
        dimensions: Int,
        config: SQConfig = .default,
        metric: VectorMetric = .euclidean
    ) -> CodebookTrainer<T, ScalarQuantizer> {
        let sq = ScalarQuantizer(config: config, dimensions: dimensions, metric: metric)
        return CodebookTrainer(keyPath: keyPath, quantizer: sq)
    }
}

extension CodebookTrainer where Q == BinaryQuantizer {
    /// Create a CodebookTrainer with BinaryQuantizer
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the vector field
    ///   - dimensions: Vector dimensions
    ///   - config: BQ configuration (default: 4x rescoring)
    public static func binaryQuantizer(
        keyPath: KeyPath<T, [Float]>,
        dimensions: Int,
        config: BQConfig = .default
    ) -> CodebookTrainer<T, BinaryQuantizer> {
        let bq = BinaryQuantizer(config: config, dimensions: dimensions)
        return CodebookTrainer(keyPath: keyPath, quantizer: bq)
    }
}
