import DatabaseKit

/// Uses the existing bounded query graph walk to admit sequence writes in SELECT.
package enum SQLSequenceEffectAnalysis {
    package static func containsEffect(_ statement: QueryStatement, workMeter: DatabaseWorkMeter) throws -> Bool {
        try contains(statement, names: ["nextval", "setval"], workMeter: workMeter)
    }

    package static func containsSequence(_ statement: QueryStatement, workMeter: DatabaseWorkMeter) throws -> Bool {
        try contains(statement, names: ["nextval", "currval", "lastval", "setval"], workMeter: workMeter)
    }

    private static func contains(_ statement: QueryStatement, names: [String], workMeter: DatabaseWorkMeter) throws -> Bool {
        guard case .select(let query) = statement else { return false }
        var found = false
        try FusionSelectQueryGraphWalker.forEachQuery(in: query, workMeter: workMeter,
            { _ in }, visitTable: { _, _ in }, visitPolymorphic: { _, _ in },
            visitExpression: { expression in
                guard case .function(let call) = expression,
                      call.schema == nil || call.schema == "pg_catalog" else { return }
                let name = call.name
                if names.contains(name) { found = true }
            })
        return found
    }
}
