// IndexMaintenanceService.swift
// DatabaseEngine - Centralized index maintenance operations
//
// Single responsibility: All index-related operations
// - Index key building
// - Index value extraction
// - Diff-based index updates (via IndexMaintainer protocol)
// - Uniqueness constraint checking (delegated to IndexMaintainer where applicable)
//
// Uses the IndexMaintainerFactory contract to delegate maintenance to the
// provider registered for each index type.

import DatabaseKit
import DatabaseTypes
import StorageKit

/// Centralized service for index maintenance operations
///
/// **Responsibilities**:
/// - Coordinate index maintenance across all index types
/// - Manage index state checks (skip disabled indexes)
/// - Resolve an IndexDescriptor through the IndexMaintainerFactory contract.
///
/// This ensures all index types (Vector, FullText, Graph, Scalar, Aggregation, etc.)
/// are maintained correctly via their specialized IndexMaintainer implementations.
///
/// **Not Responsible For**:
/// - Persistable serialization/deserialization (DataAccess)
/// - Transaction management (Database)
/// - Directory resolution (DBContainer)
/// - Index state persistence (IndexLifecycleStore)
internal final class IndexMaintenanceService: Sendable {

    // MARK: - Properties

    private let indexLifecycleStore: IndexLifecycleStore
    private let violationTracker: UniquenessViolationTracker
    private let logger: DatabaseLogger
    private let configurations: [any IndexRuntimeConfiguration]

    // MARK: - Initialization

    init(
        indexLifecycleStore: IndexLifecycleStore,
        violationTracker: UniquenessViolationTracker,
        configurations: [any IndexRuntimeConfiguration] = []
    ) {
        self.indexLifecycleStore = indexLifecycleStore
        self.violationTracker = violationTracker
        self.configurations = configurations
        self.logger = indexLifecycleStore.container.configuration.logging.logger(
            label: "com.database.framework.index-maintenance"
        )
    }

    // MARK: - Public API

    /// Update indexes for type-erased models
    ///
    /// For batch operations where type information is erased.
    /// Resolves the runtime model type through the IndexMaintainerFactory contract.
    ///
    /// **Note**: This method uses existential types and is less efficient than the typed version.
    /// For polymorphic operations, the typed `updateIndexes<T>()` is preferred when possible.
    ///
    /// - Parameters:
    ///   - oldModel: Previous model (nil for insert) - should be pre-deserialized by caller
    ///   - newModel: New model (nil for delete)
    ///   - id: Primary key tuple
    ///   - transaction: Current FDB transaction
    func updateIndexesUntyped(
        runtime: EntityRuntimeRegistration,
        oldModel: PersistedModel?,
        newModel: PersistedModel?,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        try await updateIndexesUntyped(
            runtime: runtime,
            oldModel: oldModel,
            newModel: newModel,
            id: id,
            descriptors: nil,
            logicalTypeName: nil,
            transaction: transaction
        )
    }

    func updateIndexesUntyped(
        runtime: EntityRuntimeRegistration,
        oldModel: PersistedModel?,
        newModel: PersistedModel?,
        id: Tuple,
        descriptors: [IndexDescriptor]?,
        logicalTypeName: String?,
        transaction: any TransactionAccess
    ) async throws {
        try await runtime.updateIndexes(
            lifecycleStore: indexLifecycleStore,
            violationTracker: violationTracker,
            configurations: configurations,
            wallClock: indexLifecycleStore.container.wallClock,
            oldModel: oldModel,
            newModel: newModel,
            id: id,
            descriptors: descriptors,
            logicalTypeName: logicalTypeName,
            transaction: transaction
        )
    }

    // MARK: - Private: ResolvedIndex Building

    /// Build Index from IndexDescriptor
    ///
    /// Creates an Index object from an IndexDescriptor, constructing the rootExpression
    /// from the keyPaths. This converts IndexDescriptor metadata
    /// with IndexMaintainer (runtime execution).
    ///
    /// - Parameters:
    ///   - descriptor: The IndexDescriptor to convert
    ///   - persistableType: The type name for itemTypes
    /// - Returns: An Index object suitable for IndexMaintainer
    private static func buildIndex(from descriptor: IndexDescriptor, persistableType: String) -> ResolvedIndex {
        let rootExpression = KeyExpressionFactory.from(keyPaths: descriptor.fieldNames)

        return ResolvedIndex(
            descriptor: descriptor,
            rootExpression: rootExpression,
            itemTypes: Set([persistableType]),
        )
    }

    // MARK: - Static Utilities

    /// Build index keys with proper array field handling
    ///
    /// For single-field indexes where the field returns multiple values (array),
    /// creates one index key per value. This enables reverse lookups for To-Many relationships.
    ///
    /// - Parameters:
    ///   - subspace: ResolvedIndex subspace
    ///   - values: ResolvedIndex values extracted from model
    ///   - id: Primary key tuple
    ///   - keyPathCount: Number of keyPaths in the index (determines array handling)
    /// - Returns: Array of packed index keys
    static func buildIndexKeys(
        subspace: Subspace,
        values: [any TupleElement],
        id: Tuple,
        keyPathCount: Int
    ) -> [ByteString] {
        let isSingleFieldArrayIndex = keyPathCount == 1 && values.count > 1

        if isSingleFieldArrayIndex {
            // Array field: one key per element
            return values.map { value in
                var elements: [any TupleElement] = [value]
                appendIDElements(from: id, to: &elements)
                return subspace.pack(Tuple(elements))
            }
        } else {
            // Scalar/composite: single key with all values
            var elements: [any TupleElement] = values
            appendIDElements(from: id, to: &elements)
            return [subspace.pack(Tuple(elements))]
        }
    }

    // MARK: - Private: Helpers

    private static func appendIDElements(from id: Tuple, to elements: inout [any TupleElement]) {
        for i in 0..<id.count {
            if let element = id[i] {
                elements.append(element)
            }
        }
    }

}

