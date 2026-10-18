import DatabaseKit

extension SQLDDLExecutor {
    func executeSequence(_ command: SQLDDLStatement, step: Int) async throws -> SQLCommandKind {
        guard let sequenceRequest = SQLSequenceRuntime.request else { throw SQLSequenceExecutionError.requestRequired }
        let lease = await transaction.currentSQLSchemaLease()
        let catalog = lease.sqlCatalog.catalog
        switch command {
        case .createSequence(let declaration):
            let namespace = try namespace(for: declaration.name, catalog: catalog)
            if nameExists(declaration.name.table, namespaceID: namespace.id, catalog: catalog) {
                guard declaration.ifNotExists else { throw SQLDDLExecutionError.duplicateObject(declaration.name.table) }
                return .createSequence
            }
            let sequence = try sequenceDeclaration(id: request.objectID(step: step, role: "sequence"),
                namespaceID: namespace.id, name: declaration.name.table, options: declaration.options,
                owner: ownership(declaration.ownedBy, snapshot: lease.sqlCatalog), previous: nil)
            return try await sequenceRequest.administer(.create(sequence), step: step, transaction: transaction)
        case .alterSequence(let declaration):
            guard let old = try resolveSequence(declaration.name, catalog: catalog) else {
                guard declaration.ifExists else { throw SQLDDLExecutionError.missingObject(declaration.name.table) }
                return .alterSequence
            }
            let name = declaration.renameTo ?? old.name
            if !same(name, old.name), nameExists(name, namespaceID: old.namespaceID, catalog: catalog) {
                throw SQLDDLExecutionError.duplicateObject(name)
            }
            let owner: SQLSequenceOwner?
            if let declaredOwner = declaration.ownedBy { owner = try ownership(declaredOwner, snapshot: lease.sqlCatalog) }
            else { owner = old.ownedBy }
            let sequence = try sequenceDeclaration(id: old.id, namespaceID: old.namespaceID, name: name,
                options: declaration.options, owner: owner, previous: old)
            let restart: Int64?
            switch declaration.restart { case .none: restart = nil; case .start?: restart = sequence.start; case .value(let value)?: restart = value }
            return try await sequenceRequest.administer(.alter(sequence, restart: restart), step: step, transaction: transaction)
        case .dropSequence(let names, let ifExists, let behavior):
            for (offset, name) in names.enumerated() {
                let current = await transaction.currentSQLSchemaLease()
                guard let sequence = try resolveSequence(name, catalog: current.sqlCatalog.catalog) else {
                    if !ifExists { throw SQLDDLExecutionError.missingObject(name.table) }
                    continue
                }
                let identityDependent = current.sqlCatalog.catalog.relations.contains { relation in
                    relation.columns.contains { column in
                        if case .identity(let identity) = column.policy { return same(identity.sequenceID, sequence.id) }
                        return false
                    }
                }
                guard !identityDependent else { throw SQLDDLExecutionError.dependentObject(sequence.name) }
                _ = try await sequenceRequest.administer(.drop(id: sequence.id, cascade: behavior == .cascade),
                    step: step + offset, transaction: transaction)
            }
            return .dropSequence
        default: throw SQLDDLExecutionError.invalidDefinition("expected sequence command")
        }
    }

    func sequenceDeclaration(id: String, namespaceID: String, name: String, options: SQLSequenceOptions,
        owner: SQLSequenceOwner?, previous: SQLSequence?) throws -> SQLSequence {
        let type = options.type ?? previous?.declaredType ?? .bigint
        let lower: Int64
        let upper: Int64
        switch type {
        case .smallint: lower = Int64(Int16.min); upper = Int64(Int16.max)
        case .integer: lower = Int64(Int32.min); upper = Int64(Int32.max)
        case .bigint: lower = .min; upper = .max
        default: throw SQLDDLExecutionError.invalidDefinition("sequence type must be smallint, integer or bigint")
        }
        let increment = options.increment ?? previous?.increment ?? 1
        func bound(_ option: SQLSequenceBound?, old: Int64?, fallback: Int64) -> Int64 {
            switch option { case .value(let value)?: return value; case .none?: return fallback; case nil: return old ?? fallback }
        }
        var priorMinimum = previous?.minimum
        var priorMaximum = previous?.maximum
        if let previous, let selectedType = options.type, selectedType != previous.declaredType {
            let oldLower: Int64
            let oldUpper: Int64
            switch previous.declaredType {
            case .smallint: oldLower = Int64(Int16.min); oldUpper = Int64(Int16.max)
            case .integer: oldLower = Int64(Int32.min); oldUpper = Int64(Int32.max)
            default: oldLower = .min; oldUpper = .max
            }
            if previous.minimum == (previous.increment > 0 ? 1 : oldLower) { priorMinimum = nil }
            if previous.maximum == (previous.increment > 0 ? oldUpper : -1) { priorMaximum = nil }
        }
        let minimum = bound(options.minimum, old: priorMinimum, fallback: increment > 0 ? 1 : lower)
        let maximum = bound(options.maximum, old: priorMaximum, fallback: increment > 0 ? upper : -1)
        guard minimum >= lower, maximum <= upper else { throw SQLDDLExecutionError.invalidDefinition("sequence bounds exceed type") }
        let start = options.start ?? previous?.start ?? (increment > 0 ? minimum : maximum)
        return try SQLSequence(id: id, namespaceID: namespaceID, name: name, start: start,
            increment: increment, minimum: minimum, maximum: maximum,
            cycle: options.cycle ?? previous?.cycle ?? false, ownedBy: owner, declaredType: type)
    }

    private func ownership(_ ownership: SQLSequenceOwnership?, snapshot: SQLCatalogSnapshot) throws -> SQLSequenceOwner? {
        guard let ownership else { return nil }
        switch ownership {
        case .none: return nil
        case .column(let table, let name):
            guard let relation = try resolveIfPresent(table, snapshot: snapshot),
                  let column = relation.declaration.columns.first(where: { same($0.name, name) }) else {
                throw SQLDDLExecutionError.missingObject(name)
            }
            return try SQLSequenceOwner(relationID: relation.declaration.id, columnID: column.id)
        }
    }

    private func resolveSequence(_ name: TableRef, catalog: SQLCatalog) throws -> SQLSequence? {
        if let explicit = name.schema {
            guard let namespace = catalog.namespaces.first(where: { same($0.name, explicit) }) else { return nil }
            return catalog.sequences.first { same($0.namespaceID, namespace.id) && same($0.name, name.table) }
        }
        let user = try context.readPolicy().sqlCurrentUser
        for entry in options.searchPath.entries {
            let selected: String?
            switch entry { case .schema(let value): selected = value; case .currentUser: selected = user }
            guard let selected, let namespace = catalog.namespaces.first(where: { same($0.name, selected) }) else { continue }
            if let sequence = catalog.sequences.first(where: { same($0.namespaceID, namespace.id) && same($0.name, name.table) }) { return sequence }
        }
        return nil
    }
}
