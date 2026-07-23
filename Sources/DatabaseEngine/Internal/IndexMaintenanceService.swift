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
import Core

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
    private let configurations: [any IndexConfiguration]
    private let maintainerProviders: IndexMaintainerProviderRegistry

    // MARK: - Initialization

    init(
        indexLifecycleStore: IndexLifecycleStore,
        violationTracker: UniquenessViolationTracker,
        indexSubspace: Subspace,
        maintainerProviders: IndexMaintainerProviderRegistry,
        configurations: [any IndexConfiguration] = []
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
        let indexDescriptors = T.indexDescriptors
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
            let idExpression = FieldKeyExpression(fieldName: "id")
            let maintainer: any IndexMaintainer<T> = try maintainerProviders
                .makeIndexMaintainer(
                    index: index,
                    subspace: indexSubspaceForIndex,
                    idExpression: idExpression,
                    configurations: configurations
                )

            if descriptor.isUnique, let newModel = newModel {
                try await checkUniquenessConstraint(
                    descriptor: descriptor,
                    model: newModel,
                    id: id,
                    oldModel: oldModel,
                    state: state,
                    indexSubspace: indexSubspaceForIndex,
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

        let indexDescriptors = descriptors ?? modelType.indexDescriptors
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
            let idExpression: KeyExpression = logicalTypeName == nil
                ? FieldKeyExpression(fieldName: "id")
                : TupleKeyExpression(value: id)

            if descriptor.isUnique, let newModel = newModel {
                try await checkUniquenessConstraintUntyped(
                    descriptor: descriptor,
                    model: newModel,
                    id: id,
                    oldModel: oldModel,
                    state: state,
                    indexSubspace: indexSubspaceForIndex,
                    transaction: transaction
                )
            }

            try await Self.updateIndexWithProvider(
                maintainerProviders: maintainerProviders,
                index: index,
                subspace: indexSubspaceForIndex,
                idExpression: idExpression,
                configurations: configurations,
                oldModel: oldModel,
                newModel: newModel,
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
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration],
        oldModel: (any Persistable)?,
        newModel: (any Persistable)?,
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

            try await maintainer.updateIndex(
                oldItem: typedOld,
                newItem: typedNew,
                transaction: transaction
            )
        }

        try await _openExistential(modelType, do: updateConcreteIndex)
    }

    /// Type-erased uniqueness constraint check
    ///
    /// This method wraps the typed `checkUniquenessConstraint` for use in `updateIndexesUntyped`.
    /// Uses _openExistential for runtime type dispatch from existential to concrete type.
    private func checkUniquenessConstraintUntyped(
        descriptor: IndexDescriptor,
        model: any Persistable,
        id: Tuple,
        oldModel: (any Persistable)?,
        state: IndexState,
        indexSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws {
        let modelType = type(of: model)

        func checkConcreteUniquenessConstraint<T: Persistable>(
            _ type: T.Type
        ) async throws {
            guard let typedModel = model as? T else { return }
            let typedOld = oldModel as? T

            try await checkUniquenessConstraint(
                descriptor: descriptor,
                model: typedModel,
                id: id,
                oldModel: typedOld,
                state: state,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }

        try await _openExistential(
            modelType,
            do: checkConcreteUniquenessConstraint
        )
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

    /// Extract index values from model using KeyPaths
    ///
    /// - Parameters:
    ///   - model: The model to extract values from
    ///   - keyPaths: KeyPaths defining which fields to extract
    /// - Returns: Array of extracted values as TupleElements
    static func extractIndexValues(
        from model: any Persistable,
        fieldNames: [String]
    ) throws -> [any TupleElement] {
        try DataAccess.evaluate(
            item: model,
            expression: KeyExpressionFactory.from(keyPaths: fieldNames)
        )
    }

    /// Extract ID as Tuple from model
    ///
    /// - Parameter model: The model to extract ID from
    /// - Returns: ID as Tuple containing the ID element
    /// - Throws: If ID cannot be converted to TupleElement
    ///
    /// **Note**: `Tuple` itself cannot be a `Persistable.ID` because `Tuple` does not
    /// conform to `Codable` (required by `Persistable.ID`). The ID is always a single
    /// `TupleElement` (e.g., String, Int64) which is wrapped in a `Tuple` for key building.
    static func extractIDTuple(from model: any Persistable) throws -> Tuple {
        try model.persistableIdentifierTuple()
    }

    // MARK: - Private: Helpers

    private static func appendIDElements(from id: Tuple, to elements: inout [any TupleElement]) {
        for i in 0..<id.count {
            if let element = id[i] {
                elements.append(element)
            }
        }
    }

    // MARK: - Private: Uniqueness Constraint

    /// Check uniqueness constraint for a unique index
    ///
    /// - Readable state: throws `UniquenessViolationError` if duplicate exists
    /// - WriteOnly state: tracks violation using `violationTracker` (does not throw)
    ///
    /// **Array Field Handling**:
    /// For single-field indexes on array types (e.g., `tags: [String]`), each array element
    /// is checked separately. This matches how `ScalarIndexMaintainer` creates one index
    /// entry per array element, enabling per-element uniqueness enforcement.
    private func checkUniquenessConstraint<T: Persistable>(
        descriptor: IndexDescriptor,
        model: T,
        id: Tuple,
        oldModel: T?,
        state: IndexState,
        indexSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws {
        // Extract index values from the new model
        let values = try Self.extractIndexValues(
            from: model,
            fieldNames: descriptor.fieldNames
        )
        logger.trace("checkUniquenessConstraint: index=\(descriptor.name), values=\(values), state=\(state)")
        guard !values.isEmpty else {
            return
        }

        // Detect array field: single keyPath but multiple values
        // This matches the logic in buildIndexKeys() and ScalarIndexMaintainer
        let isArrayField = descriptor.fieldNames.count == 1 && values.count > 1

        if isArrayField {
            // Array field: check each element separately
            // Index structure: [subspace][element][id] for each element
            for value in values {
                try await checkValuesUniqueness(
                    values: [value],
                    descriptor: descriptor,
                    model: model,
                    id: id,
                    oldModel: oldModel,
                    state: state,
                    indexSubspace: indexSubspace,
                    transaction: transaction
                )
            }
        } else {
            // Scalar or composite field: check all values together
            // Index structure: [subspace][value1][value2]...[id]
            try await checkValuesUniqueness(
                values: values,
                descriptor: descriptor,
                model: model,
                id: id,
                oldModel: oldModel,
                state: state,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }
    }

    /// Check uniqueness for a specific set of values
    ///
    /// Core uniqueness checking logic extracted for reuse with array fields.
    private func checkValuesUniqueness<T: Persistable>(
        values: [any TupleElement],
        descriptor: IndexDescriptor,
        model: T,
        id: Tuple,
        oldModel: T?,
        state: IndexState,
        indexSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws {
        // Build the index key (without ID suffix) to check for existing entries
        // Note: We use pack() to get the key prefix, not subspace() which creates a nested tuple
        let valueTuple = Tuple(values)
        let keyPrefix = indexSubspace.pack(valueTuple)

        // Build range by appending FDB range markers to the key prefix
        // Range: [keyPrefix, keyPrefix + 0xFF] covers all keys with this prefix
        let rangeBegin = keyPrefix
        var rangeEnd = keyPrefix
        rangeEnd.append(0xFF)

        var existingEntryFound = false
        var existingPrimaryKey: Bytes?

        for (key, _) in try await transaction.collectRange(from: .firstGreaterOrEqual(rangeBegin), to: .firstGreaterOrEqual(rangeEnd), limit: 2, snapshot: false) {
            // Parse the key to extract the primary key (last element after value tuple)
            let keyTuple = Tuple(try Tuple.unpack(from: key))

            // Skip if this is the same entity (update case)
            // Uses Tuple equality which is type-agnostic (compares encoded bytes)
            // This supports all TupleElement ID types: String, Int64, UUID, etc.
            if let oldModel = oldModel {
                let oldId = try Self.extractIDTuple(from: oldModel)
                if keyTuple.count >= oldId.count {
                    // Extract ID portion from the END of the key tuple
                    // Key structure: [subspace prefix][values...][id...]
                    // The ID is always the last oldId.count elements
                    let idStartIndex = keyTuple.count - oldId.count
                    var existingIdElements: [any TupleElement] = []
                    for i in idStartIndex..<keyTuple.count {
                        if let element = keyTuple[i] {
                            existingIdElements.append(element)
                        }
                    }

                    // Use Tuple's type-agnostic equality (compares encoded bytes)
                    // This matches the pattern used in IndexSearcher and AverageIndexMaintainer
                    if oldId == Tuple(existingIdElements) {
                        continue // Skip our own old entry
                    }
                }
            }

            existingEntryFound = true
            existingPrimaryKey = key
            break
        }

        guard existingEntryFound else { return }

        // Build value description for error message
        let conflictingValues = values.map { String(describing: $0) }

        // Parse the existing primary key from the index entry
        let existingId: Tuple
        if let existingKey = existingPrimaryKey {
            let elements = try Tuple.unpack(from: existingKey)
            guard elements.count > values.count else {
                throw IndexMaintenanceError.corruptedIndexKey(
                    indexName: descriptor.name
                )
            }
            let keyTuple = Tuple(elements)
            // Extract ID elements from the end of the key tuple
            var idElements: [any TupleElement] = []
            for i in values.count..<keyTuple.count {
                if let element = keyTuple[i] {
                    idElements.append(element)
                }
            }
            existingId = Tuple(idElements)
        } else {
            throw IndexMaintenanceError.corruptedIndexKey(
                indexName: descriptor.name
            )
        }

        switch state {
        case .readable:
            // Throw immediately in readable state
            throw UniquenessViolationError(
                indexName: descriptor.name,
                persistableType: T.persistableType,
                conflictingValues: conflictingValues,
                existingPrimaryKey: existingId,
                newPrimaryKey: id
            )

        case .writeOnly:
            // Track violation for later resolution
            try await violationTracker.recordViolation(
                indexName: descriptor.name,
                persistableType: T.persistableType,
                valueKey: keyPrefix,
                existingPrimaryKey: existingId,
                newPrimaryKey: id,
                transaction: transaction
            )

        case .disabled:
            // Should not reach here (disabled indexes are skipped)
            break
        }
    }
}

// MARK: - Errors

/// Errors from IndexMaintenanceService
enum IndexMaintenanceError: Error, CustomStringConvertible {
    case invalidID(type: String)
    case corruptedIndexKey(indexName: String)

    var description: String {
        switch self {
        case .invalidID(let type):
            return "IndexMaintenanceError: ID for '\(type)' must conform to TupleElement"
        case .corruptedIndexKey(let indexName):
            return "IndexMaintenanceError: Index '\(indexName)' contains a malformed key"
        }
    }
}
