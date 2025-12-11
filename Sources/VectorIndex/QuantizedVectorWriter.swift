// QuantizedVectorWriter.swift
// VectorIndex - Writes quantized vector codes to the index
//
// Connects vector quantization to the data pipeline by writing
// quantized codes to the /q/ subspace for QuantizedSimilar queries.
//
// References:
// - Product Quantization: Jégou et al., "Product Quantization for Nearest Neighbor Search", IEEE TPAMI 2011

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Vector

// MARK: - QuantizedVectorWriter

/// Writes quantized vector codes to the index for efficient similarity search
///
/// **Purpose**: Connects trained quantizers to the data pipeline by encoding
/// vectors and storing quantized codes in the `/q/` subspace.
///
/// **Workflow**:
/// 1. Train quantizer using `CodebookTrainer`
/// 2. Save codebook using `CodebookTrainer.saveCodebook()`
/// 3. Build quantized index using `QuantizedVectorWriter.buildQuantizedIndex()`
/// 4. Search using `QuantizedSimilar`
///
/// **Storage Layout**:
/// ```
/// [indexSubspace]/q/[primaryKey] -> Tuple(UInt8, UInt8, ..., UInt8)
/// ```
///
/// **Usage**:
/// ```swift
/// // Train and save quantizer
/// var trainer = CodebookTrainer<Product, ProductQuantizer>.productQuantizer(
///     keyPath: \.embedding,
///     dimensions: 384
/// )
/// try await trainer.train(sampleSize: 10000, context: ctx)
/// try await trainer.saveCodebook(context: ctx)
///
/// // Build quantized index
/// let writer = QuantizedVectorWriter<Product, ProductQuantizer>(
///     keyPath: \.embedding,
///     quantizer: trainer.trainedQuantizer
/// )
/// try await writer.buildQuantizedIndex(context: ctx)
/// ```
public struct QuantizedVectorWriter<T: Persistable, Q: VectorQuantizer>: @unchecked Sendable
    where Q.Code == [UInt8]
{
    private let fieldName: String
    private let quantizer: Q
    private let idExpression: KeyExpression

    // MARK: - Initialization

    /// Create a QuantizedVectorWriter for a vector field
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the vector field
    ///   - quantizer: Trained quantizer to use for encoding
    ///   - idExpression: Expression to extract primary key (defaults to "id" field)
    public init(
        keyPath: KeyPath<T, [Float]>,
        quantizer: Q,
        idExpression: KeyExpression = FieldKeyExpression(fieldName: "id")
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.quantizer = quantizer
        self.idExpression = idExpression
    }

    /// Create a QuantizedVectorWriter for an optional vector field
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the optional vector field
    ///   - quantizer: Trained quantizer to use for encoding
    ///   - idExpression: Expression to extract primary key (defaults to "id" field)
    public init(
        keyPath: KeyPath<T, [Float]?>,
        quantizer: Q,
        idExpression: KeyExpression = FieldKeyExpression(fieldName: "id")
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.quantizer = quantizer
        self.idExpression = idExpression
    }

    // MARK: - Index Building

    /// Build quantized index for all items in the collection
    ///
    /// Scans all items and writes quantized codes to the `/q/` subspace.
    /// Uses batched transactions to handle large datasets.
    ///
    /// **Prerequisites**:
    /// - Quantizer must be trained before calling this method
    /// - Items should already be indexed (vectors stored in flat index)
    ///
    /// **FDB Constraints**:
    /// - Transactions limited to ~10MB writes, 5s duration
    /// - Uses resumable cursor for fault tolerance
    ///
    /// - Parameters:
    ///   - context: IndexQueryContext for database access
    ///   - batchSize: Number of items to process per transaction (default: 1000)
    /// - Returns: BuildProgress with statistics
    /// - Throws: QuantizerError.notTrained if quantizer is not trained
    @discardableResult
    public func buildQuantizedIndex(
        context: IndexQueryContext,
        batchSize: Int = 1000
    ) async throws -> BuildProgress {
        guard quantizer.isTrained else {
            throw QuantizerError.notTrained
        }

        // Find index descriptor
        guard let descriptor = findIndexDescriptor() else {
            throw VectorIndexError.invalidArgument("Vector index not found for field '\(fieldName)'")
        }

        let indexName = descriptor.name

        // Get subspaces
        let typeSubspace = try await context.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)
        let quantizedSubspace = indexSubspace.subspace("q")

        // Get items subspace
        let itemsSubspace = try await context.itemSubspace(for: T.self)
        let (rangeBegin, rangeEnd) = itemsSubspace.range()

        // Batched processing with resumable cursor
        var cursor: [UInt8]? = nil
        var totalProcessed = 0
        var totalSkipped = 0
        let effectiveBatchSize = max(1, min(batchSize, 5000))  // Clamp to reasonable range

        repeat {
            let batchResult = try await processBatch(
                context: context,
                itemsSubspace: itemsSubspace,
                quantizedSubspace: quantizedSubspace,
                rangeBegin: cursor ?? rangeBegin,
                rangeEnd: rangeEnd,
                batchSize: effectiveBatchSize
            )

            totalProcessed += batchResult.processed
            totalSkipped += batchResult.skipped
            cursor = batchResult.nextCursor

        } while cursor != nil

        return BuildProgress(
            totalProcessed: totalProcessed,
            totalSkipped: totalSkipped
        )
    }

    /// Process a single batch of items
    private func processBatch(
        context: IndexQueryContext,
        itemsSubspace: Subspace,
        quantizedSubspace: Subspace,
        rangeBegin: [UInt8],
        rangeEnd: [UInt8],
        batchSize: Int
    ) async throws -> BatchResult {
        try await context.withTransaction { transaction in
            let sequence = transaction.getRange(
                from: .firstGreaterOrEqual(rangeBegin),
                to: .firstGreaterOrEqual(rangeEnd),
                limit: batchSize,
                snapshot: true,
                streamingMode: .iterator
            )

            var processed = 0
            var skipped = 0
            var lastKey: [UInt8]? = nil

            for try await (key, value) in sequence {
                lastKey = key

                // Decode primary key
                guard let keyTuple = try? itemsSubspace.unpack(key) else {
                    skipped += 1
                    continue
                }

                // Decode item
                guard let item: T = try? DataAccess.deserialize(value) else {
                    skipped += 1
                    continue
                }

                // Extract vector
                guard let vector = item[dynamicMember: self.fieldName] as? [Float] else {
                    skipped += 1
                    continue
                }

                // Encode vector using quantizer
                let code = try self.quantizer.encode(vector)

                // Write quantized code
                let quantizedKey = quantizedSubspace.pack(keyTuple)
                let codeValue = self.packCode(code)
                transaction.setValue(codeValue, for: quantizedKey)

                processed += 1
            }

            // Compute next cursor (key after last processed)
            let nextCursor: [UInt8]?
            if let lastKey = lastKey, processed == batchSize {
                nextCursor = lastKey + [0x00]  // Next key after last
            } else {
                nextCursor = nil  // No more items
            }

            return BatchResult(
                processed: processed,
                skipped: skipped,
                nextCursor: nextCursor
            )
        }
    }

    /// Result of a single batch processing
    private struct BatchResult {
        let processed: Int
        let skipped: Int
        let nextCursor: [UInt8]?
    }

    /// Progress of build operation
    public struct BuildProgress: Sendable {
        public let totalProcessed: Int
        public let totalSkipped: Int
    }

    /// Write quantized code for a single item
    ///
    /// Use this method to maintain the quantized index when inserting/updating items.
    ///
    /// - Parameters:
    ///   - item: Item to encode
    ///   - context: IndexQueryContext for database access
    /// - Throws: If encoding fails or quantizer not trained
    public func writeQuantizedCode(
        for item: T,
        context: IndexQueryContext
    ) async throws {
        guard quantizer.isTrained else {
            throw QuantizerError.notTrained
        }

        // Extract vector
        guard let vector = item[dynamicMember: fieldName] as? [Float] else {
            throw VectorIndexError.invalidArgument("Field '\(fieldName)' must contain [Float]")
        }

        // Find index descriptor
        guard let descriptor = findIndexDescriptor() else {
            throw VectorIndexError.invalidArgument("Vector index not found for field '\(fieldName)'")
        }

        let indexName = descriptor.name

        // Get subspaces
        let typeSubspace = try await context.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)
        let quantizedSubspace = indexSubspace.subspace("q")

        // Encode and write
        let code = try quantizer.encode(vector)
        let primaryKey = try DataAccess.extractId(from: item, using: idExpression)
        let quantizedKey = quantizedSubspace.pack(primaryKey)
        let codeValue = packCode(code)

        try await context.withTransaction { transaction in
            transaction.setValue(codeValue, for: quantizedKey)
        }
    }

    /// Delete quantized code for an item
    ///
    /// Use this method to maintain the quantized index when deleting items.
    /// Consistent with IndexMaintainer pattern which receives the old item on delete.
    ///
    /// - Parameters:
    ///   - item: The item being deleted
    ///   - context: IndexQueryContext for database access
    public func deleteQuantizedCode(
        for item: T,
        context: IndexQueryContext
    ) async throws {
        // Find index descriptor
        guard let descriptor = findIndexDescriptor() else {
            throw VectorIndexError.invalidArgument("Vector index not found for field '\(fieldName)'")
        }

        let indexName = descriptor.name

        // Get subspaces
        let typeSubspace = try await context.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)
        let quantizedSubspace = indexSubspace.subspace("q")

        let primaryKey = try DataAccess.extractId(from: item, using: idExpression)
        let quantizedKey = quantizedSubspace.pack(primaryKey)

        try await context.withTransaction { transaction in
            transaction.clear(key: quantizedKey)
        }
    }

    // MARK: - Private Helpers

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

    /// Pack quantized code into tuple for storage
    private func packCode(_ code: [UInt8]) -> [UInt8] {
        let elements: [any TupleElement] = code.map { Int64($0) as any TupleElement }
        return Tuple(elements).pack()
    }
}

// MARK: - FDBContext Extension

extension FDBContext {

    /// Create a QuantizedVectorWriter for building quantized indexes
    ///
    /// **Usage**:
    /// ```swift
    /// // After training quantizer
    /// let writer = context.quantizedWriter(
    ///     keyPath: \Product.embedding,
    ///     quantizer: trainedPQ
    /// )
    /// try await writer.buildQuantizedIndex(context: context.indexQueryContext)
    /// ```
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the vector field
    ///   - quantizer: Trained quantizer to use
    ///   - idExpression: Expression to extract primary key (defaults to "id" field)
    /// - Returns: QuantizedVectorWriter instance
    public func quantizedWriter<T: Persistable, Q: VectorQuantizer>(
        keyPath: KeyPath<T, [Float]>,
        quantizer: Q,
        idExpression: KeyExpression = FieldKeyExpression(fieldName: "id")
    ) -> QuantizedVectorWriter<T, Q> where Q.Code == [UInt8] {
        QuantizedVectorWriter(keyPath: keyPath, quantizer: quantizer, idExpression: idExpression)
    }
}
