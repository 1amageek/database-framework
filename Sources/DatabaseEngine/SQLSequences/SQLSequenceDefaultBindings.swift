import DatabaseKit

/// Immutable sequence identities admitted for one typed column default.
struct SQLSequenceDefaultBindings: Sendable, Equatable {
    let sequenceIDsByLiteral: [SQLCatalogName: SQLCatalogName]

    func references(_ sequenceID: String, workMeter: DatabaseWorkMeter) throws -> Bool {
        for id in sequenceIDsByLiteral.values {
            try workMeter.consume(at: .validation)
            if id == SQLCatalogName(sequenceID) { return true }
        }
        return false
    }

    static func literalTarget(_ expression: Expression) -> String? {
        guard case .function(let function) = expression,
              ["nextval", "currval", "setval"].contains(function.name),
              function.schema == nil || function.schema == "pg_catalog",
              let argument = function.arguments.first else { return nil }
        switch argument {
        case .literal(.string(let name)), .literal(.sqlUnknownString(let name)): return name
        default: return nil
        }
    }

    static func compile(_ expression: Expression, column: SQLColumn,
        relation: SQLRelation, catalog: SQLCatalog, physicalSchema: Schema
    ) throws -> Self {
        let bound = try SQLConstraintCompiler.binder(relation: relation, physicalSchema: physicalSchema)
            .bindScalarAssignment(expression,
                to: SQLConstraintCompiler.columnType(column, relation: relation, physicalSchema: physicalSchema))
        var bindings: [SQLCatalogName: SQLCatalogName] = [:]
        try collect(bound, relation: relation, catalog: catalog, into: &bindings)
        return Self(sequenceIDsByLiteral: bindings)
    }

    private static func collect(_ bound: CanonicalBoundExpression, relation: SQLRelation,
        catalog: SQLCatalog, into bindings: inout [SQLCatalogName: SQLCatalogName]
    ) throws {
        if let name = literalTarget(bound.expression) {
            let parts = try SQLSequenceRuntime.identifiers(name)
            let namespaceID: String?
            if parts.count == 2 {
                namespaceID = catalog.namespaces.first {
                    $0.name.utf8.elementsEqual(parts[0].utf8)
                }?.id
            } else { namespaceID = relation.namespaceID }
            guard let namespaceID, let sequence = catalog.sequences.first(where: {
                $0.namespaceID.utf8.elementsEqual(namespaceID.utf8)
                    && $0.name.utf8.elementsEqual(parts.last!.utf8)
            }) else { throw SQLSequenceExecutionError.unknownSequence(name) }
            bindings[SQLCatalogName(name)] = SQLCatalogName(sequence.id)
        }
        for child in bound.children {
            try collect(child, relation: relation, catalog: catalog, into: &bindings)
        }
    }
}
