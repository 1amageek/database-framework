import DatabaseKit
import DatabaseTypes
import StorageKit

extension SQLSequenceRequest {
    /// Stages catalog and allocator-incarnation changes in the same data transaction.
    func administer(_ command: SQLSequenceCommand, step: Int,
                    transaction: DatabaseTransaction) async throws -> SQLCommandKind {
        let policy = try context.readPolicy()
        let catalog = policy.sqlCatalog.catalog
        var sequences = catalog.sequences
        let kind: SQLCommandKind
        let changedID: String
        let headStart: Int64?
        switch command {
        case .create(let declaration):
            guard !sequences.contains(where: { $0.id.utf8.elementsEqual(declaration.id.utf8) }) else {
                throw SQLSequenceExecutionError.sequenceAlreadyExists(declaration.id)
            }
            sequences.append(declaration)
            kind = .createSequence; changedID = declaration.id; headStart = declaration.start
        case .alter(let declaration, let restart):
            guard let position = sequences.firstIndex(where: { $0.id.utf8.elementsEqual(declaration.id.utf8) }) else {
                throw SQLSequenceExecutionError.unknownSequence(declaration.id)
            }
            let current = sequences[position]
            // FIXME(INCOMPLETE_IMPLEMENTATION): The active S13 sequence administration path supports numeric options and ownership only. S15 must preserve admitted default identity bindings across logical sequence rename or namespace movement before those operations can succeed.
            guard current.name.utf8.elementsEqual(declaration.name.utf8),
                  current.namespaceID.utf8.elementsEqual(declaration.namespaceID.utf8) else {
                throw SQLSequenceExecutionError.unsupportedSequenceRename(declaration.id)
            }
            if let restart, restart < declaration.minimum || restart > declaration.maximum {
                throw SQLSequenceExecutionError.invalidRestart(restart)
            }
            sequences[position] = declaration
            kind = .alterSequence; changedID = declaration.id; headStart = restart
        case .drop(let id, let cascade):
            guard let position = sequences.firstIndex(where: { $0.id.utf8.elementsEqual(id.utf8) }) else {
                throw SQLSequenceExecutionError.unknownSequence(id)
            }
            var hasDependent = sequences[position].ownedBy != nil
            for relation in catalog.relations {
                for column in relation.columns {
                    if try references(column, relation: relation, snapshot: policy.sqlCatalog, sequenceID: id) { hasDependent = true }
                }
            }
            guard cascade || !hasDependent else { throw SQLSequenceExecutionError.dependentIdentity(id) }
            sequences.remove(at: position)
            kind = .dropSequence; changedID = id; headStart = nil
        }
        var relations = catalog.relations
        if case .drop(let id, true) = command {
            relations = try relations.map { relation in
                let columns = try relation.columns.map { column in
                    if try references(column, relation: relation, snapshot: policy.sqlCatalog, sequenceID: id) {
                        return try SQLColumn(id: column.id, name: column.name, backing: column.backing)
                    }
                    return column
                }
                return try SQLRelation(id: relation.id, namespaceID: relation.namespaceID,
                    name: relation.name, entityName: relation.entityName, columns: columns,
                    primaryKeyColumnIDs: relation.primaryKeyColumnIDs,
                    primaryKeyConstraint: relation.primaryKeyConstraint, constraints: relation.constraints)
            }
        }
        let candidate = try SQLCatalog(namespaces: catalog.namespaces, relations: relations, sequences: sequences)
        let storage = try context.executionStorage()
        let store = SQLSequenceStore(root: storage.systemRoot)
        if let headStart {
            let head = forkHead(step: step, start: headStart)
            try transaction.storageAccess.setValue(head, for: store.headKey(changedID))
        } else if case .drop = command {
            try transaction.storageAccess.clear(key: store.headKey(changedID))
        }
        _ = try await transaction.stageSQLCatalog(candidate, expectedGeneration: policy.schemaGeneration,
            idempotencyKey: commandIdentity(step: step))
        return kind
    }

    /// Stages only identities owned by the complete TRUNCATE table closure.
    package func restartOwnedIdentities(relationIDs: [String], step: Int,
        transaction: DatabaseTransaction) async throws {
        let catalog = try context.readPolicy().sqlCatalog.catalog
        let storage = try context.executionStorage()
        let store = SQLSequenceStore(root: storage.systemRoot)
        for sequence in catalog.sequences {
            try meter.consume(at: .mutationPlanning)
            guard let owner = sequence.ownedBy,
                  relationIDs.contains(where: { SQLConstraintCompiler.same($0, owner.relationID) }),
                  let relation = catalog.relations.first(where: { SQLConstraintCompiler.same($0.id, owner.relationID) }),
                  let column = relation.columns.first(where: { SQLConstraintCompiler.same($0.id, owner.columnID) }),
                  case .identity(let identity) = column.policy,
                  SQLConstraintCompiler.same(identity.sequenceID, sequence.id) else { continue }
            try transaction.storageAccess.setValue(forkHead(step: step, start: sequence.start),
                for: store.headKey(sequence.id))
        }
    }

    private func references(_ column: SQLColumn, relation: SQLRelation,
        snapshot: SQLCatalogSnapshot, sequenceID: String
    ) throws -> Bool {
        try meter.consume(at: .validation)
        switch column.policy {
        case .identity(let identity): return identity.sequenceID.utf8.elementsEqual(sequenceID.utf8)
        case .input:
            return try snapshot.sequenceDefaultBindings(relationID: relation.id, columnID: column.id)?
                .references(sequenceID, workMeter: meter) == true
        case .generated: return false
        }
    }

    private func commandIdentity(step: Int) -> String {
        "sql-sequence-" + nonceIdentity + "-" + String(step)
    }
}
