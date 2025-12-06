// IndexMaintenanceService.swift
// DatabaseEngine - Centralized index maintenance operations
//
// Single responsibility: All index-related operations
// - Index key building
// - Index value extraction
// - Diff-based index updates
// - Uniqueness constraint checking
// - Support for all index types (Scalar, Count, Sum, Min/Max)

import Foundation
import FoundationDB
import Core
import Logging

/// Centralized service for index maintenance operations
///
/// **Responsibilities**:
/// - Build index keys from model values
/// - Extract index values from models using KeyPaths
/// - Perform diff-based index updates (FDB Record Layer pattern)
/// - Check uniqueness constraints
/// - Support all index types: Scalar, Count, Sum, Min/Max
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

    // MARK: - Initialization

    init(
        indexStateManager: IndexStateManager,
        violationTracker: UniquenessViolationTracker,
        indexSubspace: Subspace,
        logger: Logger? = nil
    ) {
        self.indexStateManager = indexStateManager
        self.violationTracker = violationTracker
        self.indexSubspace = indexSubspace
        self.logger = logger ?? Logger(label: "com.fdb.index.maintenance")
    }

    // MARK: - Public API

    /// Update indexes for a model change (typed)
    ///
    /// Handles all index types: Scalar, Count, Sum, Min/Max
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
            let kindIdentifier = type(of: descriptor.kind).identifier

            switch kindIdentifier {
            case "count":
                try await updateCountIndex(
                    descriptor: descriptor,
                    subspace: indexSubspaceForIndex,
                    oldModel: oldModel,
                    newModel: newModel,
                    transaction: transaction
                )

            case "sum":
                try await updateSumIndex(
                    descriptor: descriptor,
                    subspace: indexSubspaceForIndex,
                    oldModel: oldModel,
                    newModel: newModel,
                    transaction: transaction
                )

            case "min", "max":
                try await updateMinMaxIndex(
                    descriptor: descriptor,
                    subspace: indexSubspaceForIndex,
                    oldModel: oldModel,
                    newModel: newModel,
                    id: id,
                    transaction: transaction
                )

            default:
                // ScalarIndexKind, VersionIndexKind: Standard key-value index
                try await updateScalarIndex(
                    descriptor: descriptor,
                    subspace: indexSubspaceForIndex,
                    oldModel: oldModel,
                    newModel: newModel,
                    id: id,
                    indexState: state,
                    persistableType: T.persistableType,
                    transaction: transaction
                )
            }
        }
    }

    /// Update indexes for type-erased models
    ///
    /// For batch operations where type information is erased.
    /// Uses "clear and re-add" strategy since Protobuf is not self-describing.
    ///
    /// - Parameters:
    ///   - oldData: Previous record data (nil for insert)
    ///   - newModel: New model (nil for delete)
    ///   - id: Primary key tuple
    ///   - transaction: Current FDB transaction
    ///   - deletingModel: Model being deleted (for delete operations)
    func updateIndexesUntyped(
        oldData: [UInt8]?,
        newModel: (any Persistable)?,
        id: Tuple,
        transaction: any TransactionProtocol,
        deletingModel: (any Persistable)? = nil
    ) async throws {
        // Determine which model type we're working with
        let modelType: any Persistable.Type
        if let newModel = newModel {
            modelType = type(of: newModel)
        } else if let deletingModel = deletingModel {
            modelType = type(of: deletingModel)
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

            // For update operations (oldData exists), clear existing index entries for this ID
            if oldData != nil {
                try await clearIndexEntriesForId(
                    indexSubspace: indexSubspaceForIndex,
                    id: id,
                    transaction: transaction
                )
            }

            // For delete operations, extract values from the model being deleted
            if let deletingModel = deletingModel {
                let oldValues = Self.extractIndexValues(from: deletingModel, keyPaths: descriptor.keyPaths)
                if !oldValues.isEmpty {
                    let oldIndexKeys = Self.buildIndexKeys(
                        subspace: indexSubspaceForIndex,
                        values: oldValues,
                        id: id,
                        keyPathCount: descriptor.keyPaths.count
                    )
                    for key in oldIndexKeys {
                        transaction.clear(key: key)
                    }
                }
            }

            // Add new index entries
            if let newModel = newModel {
                let newValues = Self.extractIndexValues(from: newModel, keyPaths: descriptor.keyPaths)
                if !newValues.isEmpty {
                    let newIndexKeys = Self.buildIndexKeys(
                        subspace: indexSubspaceForIndex,
                        values: newValues,
                        id: id,
                        keyPathCount: descriptor.keyPaths.count
                    )

                    // Check unique constraint (only for non-array indexes)
                    if descriptor.isUnique && newIndexKeys.count == 1 {
                        let mode = uniquenessCheckMode(for: state)
                        try await checkUniqueConstraint(
                            descriptor: descriptor,
                            subspace: indexSubspaceForIndex,
                            values: newValues,
                            excludingId: id,
                            persistableType: modelType.persistableType,
                            mode: mode,
                            transaction: transaction
                        )
                    }

                    for key in newIndexKeys {
                        transaction.setValue([], for: key)
                    }
                }
            }
        }
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

    // MARK: - Private: Scalar Index

    private func updateScalarIndex<T: Persistable>(
        descriptor: IndexDescriptor,
        subspace: Subspace,
        oldModel: T?,
        newModel: T?,
        id: Tuple,
        indexState: IndexState,
        persistableType: String,
        transaction: any TransactionProtocol
    ) async throws {
        let keyPathCount = descriptor.keyPaths.count

        // Compute old index keys (to potentially remove)
        var oldKeys: Set<[UInt8]> = []
        if let old = oldModel {
            let oldValues = Self.extractIndexValues(from: old, keyPaths: descriptor.keyPaths)
            if !oldValues.isEmpty {
                for key in Self.buildIndexKeys(subspace: subspace, values: oldValues, id: id, keyPathCount: keyPathCount) {
                    oldKeys.insert(key)
                }
            }
        }

        // Compute new index keys (to potentially add)
        var newKeys: Set<[UInt8]> = []
        if let new = newModel {
            let newValues = Self.extractIndexValues(from: new, keyPaths: descriptor.keyPaths)
            if !newValues.isEmpty {
                // Check unique constraint with appropriate mode
                if descriptor.isUnique {
                    let mode = uniquenessCheckMode(for: indexState)
                    try await checkUniqueConstraint(
                        descriptor: descriptor,
                        subspace: subspace,
                        values: newValues,
                        excludingId: id,
                        persistableType: persistableType,
                        mode: mode,
                        transaction: transaction
                    )
                }

                for key in Self.buildIndexKeys(subspace: subspace, values: newValues, id: id, keyPathCount: keyPathCount) {
                    newKeys.insert(key)
                }
            }
        }

        // Apply diff: remove keys that are in old but not in new
        for key in oldKeys.subtracting(newKeys) {
            transaction.clear(key: key)
        }

        // Apply diff: add keys that are in new but not in old
        for key in newKeys.subtracting(oldKeys) {
            transaction.setValue([], for: key)
        }
    }

    // MARK: - Private: Count Index

    private func updateCountIndex<T: Persistable>(
        descriptor: IndexDescriptor,
        subspace: Subspace,
        oldModel: T?,
        newModel: T?,
        transaction: any TransactionProtocol
    ) async throws {
        // Decrement count for old group key
        if let old = oldModel {
            let groupValues = Self.extractIndexValues(from: old, keyPaths: descriptor.keyPaths)
            if !groupValues.isEmpty {
                let key = subspace.pack(Tuple(groupValues))
                let decrementValue = withUnsafeBytes(of: Int64(-1).littleEndian) { Array($0) }
                transaction.atomicOp(key: key, param: decrementValue, mutationType: .add)
            }
        }

        // Increment count for new group key
        if let new = newModel {
            let groupValues = Self.extractIndexValues(from: new, keyPaths: descriptor.keyPaths)
            if !groupValues.isEmpty {
                let key = subspace.pack(Tuple(groupValues))
                let incrementValue = withUnsafeBytes(of: Int64(1).littleEndian) { Array($0) }
                transaction.atomicOp(key: key, param: incrementValue, mutationType: .add)
            }
        }
    }

    // MARK: - Private: Sum Index

    private func updateSumIndex<T: Persistable>(
        descriptor: IndexDescriptor,
        subspace: Subspace,
        oldModel: T?,
        newModel: T?,
        transaction: any TransactionProtocol
    ) async throws {
        guard descriptor.keyPaths.count >= 2,
              let valueKeyPath = descriptor.keyPaths.last else { return }

        let groupKeyPaths = Array(descriptor.keyPaths.dropLast())

        var oldNumeric: Double = 0.0
        var newNumeric: Double = 0.0
        var groupKey: [UInt8]?

        if let old = oldModel {
            let groupValues = Self.extractIndexValues(from: old, keyPaths: groupKeyPaths)
            let valueValues = Self.extractIndexValues(from: old, keyPaths: [valueKeyPath])

            if !groupValues.isEmpty, let oldValue = valueValues.first {
                groupKey = subspace.pack(Tuple(groupValues))
                oldNumeric = toDouble(oldValue) ?? 0.0
            }
        }

        if let new = newModel {
            let groupValues = Self.extractIndexValues(from: new, keyPaths: groupKeyPaths)
            let valueValues = Self.extractIndexValues(from: new, keyPaths: [valueKeyPath])

            if !groupValues.isEmpty, let newValue = valueValues.first {
                groupKey = subspace.pack(Tuple(groupValues))
                newNumeric = toDouble(newValue) ?? 0.0
            }
        }

        guard let key = groupKey else { return }

        let delta = newNumeric - oldNumeric
        if delta == 0.0 { return }

        // Read current sum
        let currentBytes = try await transaction.getValue(for: key, snapshot: false)
        var currentSum: Double = 0.0
        if let bytes = currentBytes, bytes.count == 8 {
            let bitPattern = bytes.withUnsafeBytes { $0.load(as: UInt64.self) }
            currentSum = Double(bitPattern: UInt64(littleEndian: bitPattern))
        }

        // Write new sum
        let newSum = currentSum + delta
        let newSumBytes = withUnsafeBytes(of: newSum.bitPattern.littleEndian) { Array($0) }
        transaction.setValue(newSumBytes, for: key)
    }

    // MARK: - Private: Min/Max Index

    private func updateMinMaxIndex<T: Persistable>(
        descriptor: IndexDescriptor,
        subspace: Subspace,
        oldModel: T?,
        newModel: T?,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        guard descriptor.keyPaths.count >= 2,
              let valueKeyPath = descriptor.keyPaths.last else { return }

        let groupKeyPaths = Array(descriptor.keyPaths.dropLast())

        // Remove old entry
        if let old = oldModel {
            let groupValues = Self.extractIndexValues(from: old, keyPaths: groupKeyPaths)
            let valueValues = Self.extractIndexValues(from: old, keyPaths: [valueKeyPath])

            if !groupValues.isEmpty, !valueValues.isEmpty {
                var keyElements: [any TupleElement] = groupValues
                keyElements.append(contentsOf: valueValues)
                Self.appendIDElements(from: id, to: &keyElements)
                let oldKey = subspace.pack(Tuple(keyElements))
                transaction.clear(key: oldKey)
            }
        }

        // Add new entry
        if let new = newModel {
            let groupValues = Self.extractIndexValues(from: new, keyPaths: groupKeyPaths)
            let valueValues = Self.extractIndexValues(from: new, keyPaths: [valueKeyPath])

            if !groupValues.isEmpty, !valueValues.isEmpty {
                var keyElements: [any TupleElement] = groupValues
                keyElements.append(contentsOf: valueValues)
                Self.appendIDElements(from: id, to: &keyElements)
                let newKey = subspace.pack(Tuple(keyElements))
                transaction.setValue([], for: newKey)
            }
        }
    }

    // MARK: - Private: Uniqueness

    private func checkUniqueConstraint(
        descriptor: IndexDescriptor,
        subspace: Subspace,
        values: [any TupleElement],
        excludingId: Tuple,
        persistableType: String,
        mode: UniquenessCheckMode,
        transaction: any TransactionProtocol
    ) async throws {
        guard mode != .skip else { return }

        let valueKey = Tuple(values).pack()
        let valueSubspace = Subspace(prefix: subspace.prefix + valueKey)
        let (begin, end) = valueSubspace.range()

        let sequence = transaction.getRange(begin: begin, end: end, snapshot: false)

        for try await (key, _) in sequence {
            if let existingId = extractIDFromIndexKey(key, subspace: valueSubspace, idElementCount: excludingId.count) {
                let existingBytes = existingId.pack()
                let excludingBytes = excludingId.pack()

                if existingBytes != excludingBytes {
                    switch mode {
                    case .immediate:
                        throw UniquenessViolationError(
                            indexName: descriptor.name,
                            persistableType: persistableType,
                            conflictingValues: values.map { String(describing: $0) },
                            existingPrimaryKey: existingId,
                            newPrimaryKey: excludingId
                        )

                    case .track:
                        try await violationTracker.recordViolation(
                            indexName: descriptor.name,
                            persistableType: persistableType,
                            valueKey: valueKey,
                            existingPrimaryKey: existingId,
                            newPrimaryKey: excludingId,
                            transaction: transaction
                        )
                        logger.warning(
                            "Recorded uniqueness violation (tracking mode)",
                            metadata: [
                                "index": "\(descriptor.name)",
                                "type": "\(persistableType)"
                            ]
                        )

                    case .skip:
                        break
                    }
                }
            }
        }
    }

    private func uniquenessCheckMode(for indexState: IndexState) -> UniquenessCheckMode {
        switch indexState {
        case .readable:
            return .immediate
        case .writeOnly:
            return .track
        case .disabled:
            return .skip
        }
    }

    // MARK: - Private: Helpers

    private func clearIndexEntriesForId(
        indexSubspace: Subspace,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let idCount = id.count
        let (begin, end) = indexSubspace.range()
        let sequence = transaction.getRange(begin: begin, end: end, snapshot: false)

        for try await (key, _) in sequence {
            if let extractedId = extractIDFromIndexKey(key, subspace: indexSubspace, idElementCount: idCount),
               extractedId.pack() == id.pack() {
                transaction.clear(key: key)
            }
        }
    }

    /// Extract ID from index key
    ///
    /// Index key structure: [indexSubspace][value1][value2]...[valueN][id1][id2]...[idM]
    /// ID is the last idElementCount elements of the tuple
    private func extractIDFromIndexKey(_ key: [UInt8], subspace: Subspace, idElementCount: Int) -> Tuple? {
        do {
            let tuple = try subspace.unpack(key)
            let totalCount = tuple.count
            guard totalCount >= idElementCount else { return nil }

            // Extract last idElementCount elements as ID
            var idElements: [any TupleElement] = []
            for i in (totalCount - idElementCount)..<totalCount {
                if let element = tuple[i] {
                    idElements.append(element)
                }
            }
            return Tuple(idElements)
        } catch {
            // Key doesn't belong to this subspace
        }
        return nil
    }

    private func toDouble(_ value: any TupleElement) -> Double? {
        switch value {
        case let v as Int64: return Double(v)
        case let v as Double: return v
        case let v as Int: return Double(v)
        case let v as Float: return Double(v)
        default: return nil
        }
    }

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
    case invalidID(type: String)

    var description: String {
        switch self {
        case .invalidID(let type):
            return "IndexMaintenanceError: ID for '\(type)' must conform to TupleElement"
        }
    }
}
