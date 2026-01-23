import Foundation
import FoundationDB

/// Handler for relationship indexes (foreign keys, referential integrity)
///
/// Storage layout:
/// - refs/<targetId>/<sourceId> = empty (for reverse lookups)
/// - fk/<sourceId> = targetId (for forward lookups)
public struct RelationshipIndexHandler: IndexHandler, Sendable {
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
              case .relationship(let relConfig) = config else {
            return
        }

        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .relationship,
            indexName: indexDefinition.name
        )
        let refsSubspace = indexSubspace.subspace(Tuple(["refs"]))
        let fkSubspace = indexSubspace.subspace(Tuple(["fk"]))

        let oldTargetId = oldItem?[relConfig.foreignKeyField] as? String
        let newTargetId = newItem?[relConfig.foreignKeyField] as? String

        // Remove old reference
        if let targetId = oldTargetId {
            let refKey = refsSubspace.pack(Tuple([targetId, id]))
            let fkKey = fkSubspace.pack(Tuple([id]))
            transaction.clear(key: refKey)
            transaction.clear(key: fkKey)
        }

        // Add new reference
        if let targetId = newTargetId {
            // Validate target exists (optional, based on delete rule)
            // This would require reading from the target schema
            // For now, we just store the reference

            let refKey = refsSubspace.pack(Tuple([targetId, id]))
            let fkKey = fkSubspace.pack(Tuple([id]))
            transaction.setValue([], for: refKey)
            transaction.setValue(Array(targetId.utf8), for: fkKey)
        }
    }

    public func scan(
        query: Any,
        limit: Int,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws -> [String] {
        guard let relQuery = query as? RelationshipQuery else {
            return []
        }

        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .relationship,
            indexName: indexDefinition.name
        )

        switch relQuery {
        case .getRelated(let targetId):
            let refsSubspace = indexSubspace.subspace(Tuple(["refs", targetId]))
            let (begin, end) = refsSubspace.range()

            var sourceIds: [String] = []
            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await (key, _) in sequence {
                guard sourceIds.count < limit else { break }
                if let tuple = try? refsSubspace.unpack(key),
                   let sourceId = tuple[0] as? String {
                    sourceIds.append(sourceId)
                }
            }
            return sourceIds

        case .getTarget(let sourceId):
            let fkSubspace = indexSubspace.subspace(Tuple(["fk"]))
            let key = fkSubspace.pack(Tuple([sourceId]))

            if let bytes = try await transaction.getValue(for: key, snapshot: true) {
                return [String(decoding: bytes, as: UTF8.self)]
            }
            return []

        case .checkConstraint(let targetId):
            // Check if any source records reference this target
            let refsSubspace = indexSubspace.subspace(Tuple(["refs", targetId]))
            let (begin, end) = refsSubspace.range()

            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await _ in sequence {
                return ["has_references"]  // Only need to find one
            }
            return ["no_references"]
        }
    }

    /// Handle delete rule enforcement
    public func handleParentDelete(
        parentId: String,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws -> DeleteRuleResult {
        guard let config = indexDefinition.config,
              case .relationship(let relConfig) = config else {
            return .allowed
        }

        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .relationship,
            indexName: indexDefinition.name
        )
        let refsSubspace = indexSubspace.subspace(Tuple(["refs", parentId]))
        let (begin, end) = refsSubspace.range()

        // Find all child records
        var childIds: [String] = []
        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
        for try await (key, _) in sequence {
            if let tuple = try? refsSubspace.unpack(key),
               let childId = tuple[0] as? String {
                childIds.append(childId)
            }
        }

        if childIds.isEmpty {
            return .allowed
        }

        switch relConfig.deleteRule {
        case .cascade:
            return .cascade(childIds)
        case .nullify:
            return .nullify(childIds, field: relConfig.foreignKeyField)
        case .deny:
            return .denied(reason: "Cannot delete: \(childIds.count) child record(s) reference this record")
        case .noAction:
            return .allowed
        }
    }
}

// MARK: - Relationship Query

public enum RelationshipQuery {
    case getRelated(targetId: String)
    case getTarget(sourceId: String)
    case checkConstraint(targetId: String)
}

// MARK: - Delete Rule Result

public enum DeleteRuleResult: Sendable {
    case allowed
    case cascade([String])
    case nullify([String], field: String)
    case denied(reason: String)
}
