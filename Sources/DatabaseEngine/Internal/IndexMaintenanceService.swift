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

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import StorageKit
import DatabaseKit

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
    private let indexSubspace: Subspace
    private let logger: DatabaseLogger
    private let configurations: [any IndexRuntimeConfiguration]
    private let maintainerProviders: IndexMaintainerProviderRegistry

    // MARK: - Initialization

    init(
        indexLifecycleStore: IndexLifecycleStore,
        violationTracker: UniquenessViolationTracker,
        indexSubspace: Subspace,
        maintainerProviders: IndexMaintainerProviderRegistry,
        configurations: [any IndexRuntimeConfiguration] = []
    ) {
        self.indexLifecycleStore = indexLifecycleStore
        self.violationTracker = violationTracker
        self.indexSubspace = indexSubspace
        self.maintainerProviders = maintainerProviders
        self.configurations = configurations
        self.logger = indexLifecycleStore.container.configuration.logging.logger(
            label: "com.database.framework.index-maintenance"
        )
    }

    // MARK: - Public API

    /// Update indexes for a model change (typed)
    ///
    /// Uses IndexMaintainerFactory protocol to delegate index maintenance to
    /// the appropriate IndexMaintainer for each index type.
    ///
    /// - Parameters:
    ///   - oldModel: Previous model state (nil for insert)
    ///   - newModel: New model state (nil for delete)
    ///   - id: Primary key tuple
    ///   - transaction: Current FDB transaction
    func updateIndexes<T: Persistable>(
        oldModel: T?,
        newModel: T?,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        let indexDescriptors = try T.indexDescriptors
        logger.trace("updateIndexes<\(T.persistableType)>: indexDescriptors.count=\(indexDescriptors.count)")
        guard !indexDescriptors.isEmpty else { return }

        // Batch fetch all index states for performance
        let indexNames = indexDescriptors.map(\.name)
        let indexStates = try await indexLifecycleStore.states(of: indexNames, transaction: transaction)

        for descriptor in indexDescriptors {
            // Check if index should be maintained based on its state
            let state = indexStates[descriptor.name] ?? .disabled
            logger.trace("updateIndexes: processing descriptor=\(descriptor.name), isUnique=\(descriptor.isUnique), state=\(state)")
            guard state.shouldMaintain else {
                logger.trace("Skipping index '\(descriptor.name)' maintenance (state: \(state))")
                continue
            }

            let indexSubspaceForIndex = indexSubspace.subspace(descriptor.name)

            let index = Self.buildIndex(
                from: descriptor,
                persistableType: T.persistableType
            )
            // The caller already resolved the model identifier through
            // PersistableIdentifierKeyCodec. Reusing that tuple keeps primary
            // rows and index suffixes byte-identical for scalar and composite
            // identifiers without re-encoding the persisted `id` field.
            let idExpression = TupleKeyExpression(value: id)
            let maintainer: any IndexMaintainer<T> = try maintainerProviders
                .makeIndexMaintainer(
                    index: index,
                    subspace: indexSubspaceForIndex,
                    idExpression: idExpression,
                    configurations: configurations
                )

            if descriptor.isUnique, let newModel = newModel {
                try await IndexUniquenessConstraint.enforce(
                    index: index,
                    item: newModel,
                    id: id,
                    state: state,
                    maintainer: maintainer,
                    violationTracker: violationTracker,
                    transaction: transaction
                )
            }

            try await maintainer.updateIndex(
                oldItem: oldModel,
                newItem: newModel,
                transaction: transaction
            )
        }
    }

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
        oldModel: (any Persistable)?,
        newModel: (any Persistable)?,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        try await updateIndexesUntyped(
            oldModel: oldModel,
            newModel: newModel,
            id: id,
            descriptors: nil,
            logicalTypeName: nil,
            transaction: transaction
        )
    }

    func updateIndexesUntyped(
        oldModel: (any Persistable)?,
        newModel: (any Persistable)?,
        id: Tuple,
        descriptors: [IndexDescriptor]?,
        logicalTypeName: String?,
        transaction: any TransactionAccess
    ) async throws {
        // Determine which model type we're working with
        let modelType: any Persistable.Type
        if let newModel = newModel {
            modelType = type(of: newModel)
        } else if let oldModel = oldModel {
            modelType = type(of: oldModel)
        } else {
            return  // No model to process
        }

        let indexDescriptors: [IndexDescriptor]
        if let descriptors {
            indexDescriptors = descriptors
        } else {
            indexDescriptors = try modelType.indexDescriptors
        }
        guard !indexDescriptors.isEmpty else { return }

        // Batch fetch all index states for performance
        let indexNames = indexDescriptors.map(\.name)
        let indexStates = try await indexLifecycleStore.states(of: indexNames, transaction: transaction)

        for descriptor in indexDescriptors {
            // Check if index should be maintained based on its state
            let state = indexStates[descriptor.name] ?? .disabled
            guard state.shouldMaintain else {
                logger.trace("Skipping index '\(descriptor.name)' maintenance (state: \(state))")
                continue
            }

            let indexSubspaceForIndex = indexSubspace.subspace(descriptor.name)

            let index = Self.buildIndex(
                from: descriptor,
                persistableType: logicalTypeName ?? modelType.persistableType
            )
            // The canonical identifier tuple is also the physical index
            // suffix. Extracting `id` as a general FieldValue would produce a
            // different tuple representation from the primary-row key.
            let idExpression: KeyExpression = TupleKeyExpression(value: id)

            try await Self.updateIndexWithProvider(
                maintainerProviders: maintainerProviders,
                violationTracker: violationTracker,
                index: index,
                subspace: indexSubspaceForIndex,
                idExpression: idExpression,
                configurations: configurations,
                oldModel: oldModel,
                newModel: newModel,
                id: id,
                state: state,
                transaction: transaction
            )
        }
    }

    // MARK: - Private: Index Building

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
    private static func buildIndex(from descriptor: IndexDescriptor, persistableType: String) -> Index {
        let rootExpression = KeyExpressionFactory.from(keyPaths: descriptor.fieldNames)

        return Index(
            name: descriptor.name,
            kind: descriptor.kind,
            rootExpression: rootExpression,
            subspaceKey: descriptor.name,
            itemTypes: Set([persistableType]),
            isUnique: descriptor.isUnique,
            storedFieldNames: descriptor.storedFieldNames
        )
    }

    /// Type-erased helper for updating an index through its registered provider.
    ///
    /// This method handles the type erasure required for `updateIndexesUntyped`.
    /// Uses _openExistential for runtime type dispatch from existential to concrete type.
    private static func updateIndexWithProvider(
        maintainerProviders: IndexMaintainerProviderRegistry,
        violationTracker: UniquenessViolationTracker,
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        oldModel: (any Persistable)?,
        newModel: (any Persistable)?,
        id: Tuple,
        state: IndexState,
        transaction: any TransactionAccess
    ) async throws {
        // Determine the concrete model type
        let modelType: any Persistable.Type
        if let new = newModel {
            modelType = type(of: new)
        } else if let old = oldModel {
            modelType = type(of: old)
        } else {
            return
        }

        // Dispatch the runtime model type to its registered index maintainer.
        func updateConcreteIndex<T: Persistable>(_ type: T.Type) async throws {
            let maintainer: any IndexMaintainer<T> = try maintainerProviders
                .makeIndexMaintainer(
                    index: index,
                    subspace: subspace,
                    idExpression: idExpression,
                    configurations: configurations
                )

            // Safe cast - we derived modelType from the models so types will match
            let typedOld = oldModel as? T
            let typedNew = newModel as? T

            if index.isUnique, let typedNew {
                try await IndexUniquenessConstraint.enforce(
                    index: index,
                    item: typedNew,
                    id: id,
                    state: state,
                    maintainer: maintainer,
                    violationTracker: violationTracker,
                    transaction: transaction
                )
            }

            try await maintainer.updateIndex(
                oldItem: typedOld,
                newItem: typedNew,
                transaction: transaction
            )
        }

        try await _openExistential(modelType, do: updateConcreteIndex)
    }

    // MARK: - Static Utilities

    /// Build index keys with proper array field handling
    ///
    /// For single-field indexes where the field returns multiple values (array),
    /// creates one index key per value. This enables reverse lookups for To-Many relationships.
    ///
    /// - Parameters:
    ///   - subspace: Index subspace
    ///   - values: Index values extracted from model
    ///   - id: Primary key tuple
    ///   - keyPathCount: Number of keyPaths in the index (determines array handling)
    /// - Returns: Array of packed index keys
    static func buildIndexKeys(
        subspace: Subspace,
        values: [any TupleElement],
        id: Tuple,
        keyPathCount: Int
    ) -> [Bytes] {
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

// MARK: - Errors

/// Errors from IndexMaintenanceService
enum IndexMaintenanceError: Error, CustomStringConvertible {
    case corruptedIndexKey(indexName: String)

    var description: String {
        switch self {
        case .corruptedIndexKey(let indexName):
            return "IndexMaintenanceError: Index '\(indexName)' contains a malformed key"
        }
    }
}
