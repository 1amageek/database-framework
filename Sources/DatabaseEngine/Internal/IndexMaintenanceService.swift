// IndexMaintenanceService.swift
// DatabaseEngine - Centralized index maintenance operations
//
// Single responsibility: All index-related operations
// - Index key building
// - Index value extraction
// - Diff-based index updates (via IndexMaintainer protocol)
// - Uniqueness constraint checking (delegated to IndexMaintainer where applicable)
//
// **Design**: Uses IndexKindMaintainable protocol bridge pattern to delegate
// index maintenance to specialized IndexMaintainer implementations for each index type.

import Foundation
import FoundationDB
import Core
import Logging

/// Centralized service for index maintenance operations
///
/// **Responsibilities**:
/// - Coordinate index maintenance across all index types
/// - Manage index state checks (skip disabled indexes)
/// - Bridge IndexDescriptor to IndexMaintainer via IndexKindMaintainable protocol
///
/// **Design Pattern**:
/// Uses the protocol bridge pattern from CLAUDE.md:
/// ```
/// IndexKind (metadata) → IndexKindMaintainable (bridge) → IndexMaintainer (runtime)
/// ```
///
/// This ensures all index types (Vector, FullText, Graph, Scalar, Aggregation, etc.)
/// are maintained correctly via their specialized IndexMaintainer implementations.
///
/// **Not Responsible For**:
/// - Record serialization/deserialization (DataAccess)
/// - Transaction management (Database)
/// - Directory resolution (FDBContainer)
/// - Index state persistence (IndexStateManager)
internal final class IndexMaintenanceService: Sendable {

    // MARK: - Properties

    private let indexStateManager: IndexStateManager
    private let violationTracker: UniquenessViolationTracker
    private let indexSubspace: Subspace
    private let logger: Logger
    private let configurations: [any IndexConfiguration]

    // MARK: - Initialization

    init(
        indexStateManager: IndexStateManager,
        violationTracker: UniquenessViolationTracker,
        indexSubspace: Subspace,
        configurations: [any IndexConfiguration] = [],
        logger: Logger? = nil
    ) {
        self.indexStateManager = indexStateManager
        self.violationTracker = violationTracker
        self.indexSubspace = indexSubspace
        self.configurations = configurations
        self.logger = logger ?? Logger(label: "com.fdb.index.maintenance")
    }

    // MARK: - Public API

    /// Update indexes for a model change (typed)
    ///
    /// Uses IndexKindMaintainable protocol to delegate index maintenance to
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
        transaction: any TransactionProtocol
    ) async throws {
        let indexDescriptors = T.indexDescriptors
        logger.trace("updateIndexes<\(T.persistableType)>: indexDescriptors.count=\(indexDescriptors.count)")
        guard !indexDescriptors.isEmpty else { return }

        // Batch fetch all index states for performance
        let indexNames = indexDescriptors.map(\.name)
        let indexStates = try await indexStateManager.states(of: indexNames, transaction: transaction)

        for descriptor in indexDescriptors {
            // Check if index should be maintained based on its state
            let state = indexStates[descriptor.name] ?? .disabled
            logger.trace("updateIndexes: processing descriptor=\(descriptor.name), isUnique=\(descriptor.isUnique), state=\(state)")
            guard state.shouldMaintain else {
                logger.trace("Skipping index '\(descriptor.name)' maintenance (state: \(state))")
                continue
            }

            let indexSubspaceForIndex = indexSubspace.subspace(descriptor.name)

            // Use IndexKindMaintainable protocol bridge pattern
            // This ensures all index types are handled correctly by their specialized IndexMaintainer
            if let maintainable = descriptor.kind as? any IndexKindMaintainable {
                // Build Index from IndexDescriptor
                let index = Self.buildIndex(from: descriptor, persistableType: T.persistableType)
                let idExpression = FieldKeyExpression(fieldName: "id")

                // Create the appropriate IndexMaintainer via protocol bridge
                let maintainer: any IndexMaintainer<T> = maintainable.makeIndexMaintainer(
                    index: index,
                    subspace: indexSubspaceForIndex,
                    idExpression: idExpression,
                    configurations: configurations
                )

                // Check uniqueness constraint for inserts (newModel != nil)
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

                // Delegate to IndexMaintainer
                try await maintainer.updateIndex(
                    oldItem: oldModel,
                    newItem: newModel,
                    transaction: transaction
                )
            } else {
                // Fallback for IndexKinds that don't conform to IndexKindMaintainable
                // This should not happen for well-implemented indexes
                let kindIdentifier = type(of: descriptor.kind).identifier
                logger.warning(
                    "IndexKind '\(kindIdentifier)' does not conform to IndexKindMaintainable. Index '\(descriptor.name)' will not be maintained. Please add IndexKindMaintainable conformance to the IndexKind."
                )
            }
        }
    }

    /// Update indexes for type-erased models
    ///
    /// For batch operations where type information is erased.
    /// Uses IndexKindMaintainable protocol bridge pattern.
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
        transaction: any TransactionProtocol
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

        let indexDescriptors = modelType.indexDescriptors
        guard !indexDescriptors.isEmpty else { return }

        // Batch fetch all index states for performance
        let indexNames = indexDescriptors.map(\.name)
        let indexStates = try await indexStateManager.states(of: indexNames, transaction: transaction)

        for descriptor in indexDescriptors {
            // Check if index should be maintained based on its state
            let state = indexStates[descriptor.name] ?? .disabled
            guard state.shouldMaintain else {
                logger.trace("Skipping index '\(descriptor.name)' maintenance (state: \(state))")
                continue
            }

            let indexSubspaceForIndex = indexSubspace.subspace(descriptor.name)

            // Use IndexKindMaintainable protocol bridge pattern
            if let maintainable = descriptor.kind as? any IndexKindMaintainable {
                // Build Index from IndexDescriptor
                let index = Self.buildIndex(from: descriptor, persistableType: modelType.persistableType)
                let idExpression = FieldKeyExpression(fieldName: "id")

                // Check uniqueness constraint for inserts (newModel != nil)
                // or updates where the indexed value changes
                if descriptor.isUnique, let newModel = newModel {
                    // Extract ID for uniqueness check
                    let idTuple: Tuple
                    if let element = newModel.id as? any TupleElement {
                        idTuple = Tuple([element])
                    } else {
                        idTuple = Tuple(["unknown"])
                    }

                    try await checkUniquenessConstraintUntyped(
                        descriptor: descriptor,
                        model: newModel,
                        id: idTuple,
                        oldModel: oldModel,
                        state: state,
                        indexSubspace: indexSubspaceForIndex,
                        transaction: transaction
                    )
                }

                // Create maintainer and update index using type-erased helper
                try await Self.updateIndexWithMaintainable(
                    maintainable: maintainable,
                    index: index,
                    subspace: indexSubspaceForIndex,
                    idExpression: idExpression,
                    configurations: configurations,
                    oldModel: oldModel,
                    newModel: newModel,
                    transaction: transaction
                )
            } else {
                // Fallback for IndexKinds that don't conform to IndexKindMaintainable
                let kindIdentifier = type(of: descriptor.kind).identifier
                logger.warning(
                    "IndexKind '\(kindIdentifier)' does not conform to IndexKindMaintainable. Index '\(descriptor.name)' will not be maintained."
                )
            }
        }
    }

    // MARK: - Private: Index Building

    /// Build Index from IndexDescriptor
    ///
    /// Creates an Index object from an IndexDescriptor, constructing the rootExpression
    /// from the keyPaths. This is used to bridge IndexDescriptor (compile-time metadata)
    /// with IndexMaintainer (runtime execution).
    ///
    /// - Parameters:
    ///   - descriptor: The IndexDescriptor to convert
    ///   - persistableType: The type name for itemTypes
    /// - Returns: An Index object suitable for IndexMaintainer
    private static func buildIndex(from descriptor: IndexDescriptor, persistableType: String) -> Index {
        // Build rootExpression from keyPaths
        // Most IndexMaintainers prefer keyPaths over rootExpression, so we provide a simple expression
        let rootExpression: KeyExpression
        if descriptor.keyPaths.isEmpty {
            rootExpression = EmptyKeyExpression()
        } else {
            // Use the first keyPath's field name as a simple expression
            // Note: IndexMaintainers should use Index.keyPaths directly for accurate field extraction
            let firstKeyPathString = String(describing: descriptor.keyPaths.first!)
            let fieldName = extractFieldName(from: firstKeyPathString)
            rootExpression = FieldKeyExpression(fieldName: fieldName)
        }

        return Index(
            name: descriptor.name,
            kind: descriptor.kind,
            rootExpression: rootExpression,
            keyPaths: descriptor.keyPaths,
            subspaceKey: descriptor.name,
            itemTypes: Set([persistableType]),
            isUnique: descriptor.isUnique
        )
    }

    /// Extract field name from keyPath string representation
    ///
    /// KeyPath string representation looks like: "\\Type.fieldName" or "Swift.KeyPath<Type, FieldType>"
    private static func extractFieldName(from keyPathString: String) -> String {
        // Try to extract field name from various formats
        // Format 1: "\Type.fieldName"
        if let dotIndex = keyPathString.lastIndex(of: ".") {
            let afterDot = keyPathString[keyPathString.index(after: dotIndex)...]
            // Remove any trailing type info
            if let parenIndex = afterDot.firstIndex(of: "(") {
                return String(afterDot[..<parenIndex])
            }
            return String(afterDot)
        }
        // Fallback: return the whole string
        return keyPathString
    }

    /// Type-erased helper for updating index with IndexKindMaintainable
    ///
    /// This method handles the type erasure required for `updateIndexesUntyped`.
    /// Uses _openExistential for runtime type dispatch from existential to concrete type.
    private static func updateIndexWithMaintainable(
        maintainable: any IndexKindMaintainable,
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration],
        oldModel: (any Persistable)?,
        newModel: (any Persistable)?,
        transaction: any TransactionProtocol
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

        // Use _openExistential to dispatch to the concrete type
        // This unwraps the existential and calls the generic helper with the concrete type
        func helper<T: Persistable>(_ type: T.Type) async throws {
            let maintainer: any IndexMaintainer<T> = maintainable.makeIndexMaintainer(
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

        try await _openExistential(modelType, do: helper)
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
        transaction: any TransactionProtocol
    ) async throws {
        let modelType = type(of: model)

        func helper<T: Persistable>(_ type: T.Type) async throws {
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

        try await _openExistential(modelType, do: helper)
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
    ) -> [[UInt8]] {
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
    static func extractIndexValues(from model: any Persistable, keyPaths: [AnyKeyPath]) -> [any TupleElement] {
        (try? DataAccess.extractFieldsUsingKeyPaths(from: model, keyPaths: keyPaths)) ?? []
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
        let id = model.id
        let typeName = type(of: model).persistableType
        if let element = id as? any TupleElement {
            return Tuple([element])
        } else {
            throw IndexMaintenanceError.invalidID(type: typeName)
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

    // MARK: - Private: Uniqueness Constraint

    /// Check uniqueness constraint for a unique index
    ///
    /// - Readable state: throws `UniquenessViolationError` if duplicate exists
    /// - WriteOnly state: tracks violation using `violationTracker` (does not throw)
    private func checkUniquenessConstraint<T: Persistable>(
        descriptor: IndexDescriptor,
        model: T,
        id: Tuple,
        oldModel: T?,
        state: IndexState,
        indexSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws {
        // Extract index values from the new model
        let values = Self.extractIndexValues(from: model, keyPaths: descriptor.keyPaths)
        logger.trace("checkUniquenessConstraint: index=\(descriptor.name), values=\(values), state=\(state)")
        guard !values.isEmpty else {
            return
        }

        // Build the index key (without ID suffix) to check for existing entries
        // Note: We use pack() to get the key prefix, not subspace() which creates a nested tuple
        let valueTuple = Tuple(values)
        let keyPrefix = indexSubspace.pack(valueTuple)

        // Build range by appending FDB range markers to the key prefix
        // Range: [keyPrefix, keyPrefix + 0xFF] covers all keys with this prefix
        var rangeBegin = keyPrefix
        var rangeEnd = keyPrefix
        rangeEnd.append(0xFF)

        var existingEntryFound = false
        var existingPrimaryKey: [UInt8]? = nil

        for try await (key, _) in transaction.getRange(from: rangeBegin, to: rangeEnd, limit: 2, snapshot: false) {
            // Parse the key to extract the primary key (last element after value tuple)
            let keyTuple: Tuple? = (try? Tuple.unpack(from: key)).map { Tuple($0) }

            // Skip if this is the same record (update case)
            if let oldModel = oldModel {
                let oldId = try Self.extractIDTuple(from: oldModel)
                if let keyTuple = keyTuple, keyTuple.count > values.count {
                    // Check if the ID portion matches oldModel's ID
                    var matches = true
                    for i in 0..<oldId.count {
                        let keyIdx = values.count + i
                        if keyIdx < keyTuple.count {
                            if let oldElement = oldId[i] as? String, let keyElement = keyTuple[keyIdx] as? String {
                                if oldElement != keyElement { matches = false; break }
                            } else if let oldElement = oldId[i] as? Int64, let keyElement = keyTuple[keyIdx] as? Int64 {
                                if oldElement != keyElement { matches = false; break }
                            } else {
                                matches = false; break
                            }
                        }
                    }
                    if matches { continue } // Skip our own old entry
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
        if let existingKey = existingPrimaryKey,
           let elements = try? Tuple.unpack(from: existingKey),
           elements.count > values.count {
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
            existingId = Tuple(["unknown"])
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

    var description: String {
        switch self {
        case .invalidID(let type):
            return "IndexMaintenanceError: ID for '\(type)' must conform to TupleElement"
        }
    }
}
