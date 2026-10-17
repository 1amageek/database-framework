import DatabaseKit
import DatabaseTypes

/// Session values describe admitted identity; they never acquire data authority.
package enum SQLSessionRuntime {
    package static func evaluate(_ name: String, arguments: [FieldValue],
        resolution suppliedResolution: SQLReadResolution?, databaseName suppliedName: String?,
        workMeter: DatabaseWorkMeter) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?)? {
        guard ["current_schema", "current_schemas", "current_database", "current_user", "session_user"].contains(name) else { return nil }
        try workMeter.consume(at: .expressionEvaluation)
        let resolution = suppliedResolution ?? ActiveSQLSession.binding?.resolution
        func text(_ value: String?) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
            guard let value else { return (.primitive(.null), nil) }
            let claim = try workMeter.reserveIntermediate(bytes: 64 + UInt64(value.utf8.count), at: .expressionEvaluation)
            return (.primitive(.string(value)), DatabaseQueryPayloadLease(retaining: claim))
        }
        if name == "current_database" {
            guard arguments.isEmpty else { throw SQLSessionEvaluationError.invalidArguments(function: name) }
            let databaseName = suppliedResolution == nil
                ? (suppliedName ?? ActiveSQLSession.binding?.databaseName) : suppliedName
            guard let value = databaseName, !value.isEmpty else {
                throw SQLSessionEvaluationError.missingDatabaseIdentity
            }
            return try text(value)
        }
        guard let resolution else { throw SQLSessionEvaluationError.missingNamespaceAdmission }
        if name == "current_user" || name == "session_user" {
            guard arguments.isEmpty else { throw SQLSessionEvaluationError.invalidArguments(function: name) }
            guard let value = resolution.currentUser else { throw SQLSessionEvaluationError.unauthenticatedPrincipal }
            return try text(value)
        }
        let includeImplicit: Bool
        if name == "current_schemas" {
            guard arguments.count == 1 else { throw SQLSessionEvaluationError.invalidArguments(function: name) }
            if arguments[0].isNull { return (.primitive(.null), nil) }
            guard case .bool(let include) = arguments[0] else { throw SQLSessionEvaluationError.invalidArguments(function: name) }
            includeImplicit = include
        } else {
            guard arguments.isEmpty else { throw SQLSessionEvaluationError.invalidArguments(function: name) }
            includeImplicit = false
        }
        let capacity = resolution.searchPath.entries.count + 1
        let scratch = try workMeter.reserveIntermediate(bytes: UInt64(capacity) * 64, at: .expressionEvaluation)
        defer { scratch.release() }
        var schemas: [String] = []
        schemas.reserveCapacity(capacity)
        let catalog = resolution.snapshot.catalog
        func append(_ candidate: String) throws {
            try workMeter.consume(UInt64(schemas.count) + 1, at: .expressionEvaluation)
            for existing in schemas {
                try DatabaseByteProcessingMeter.consume(byteCount: UInt64(existing.utf8.count) + UInt64(candidate.utf8.count),
                    workMeter: workMeter, stage: .expressionEvaluation)
                if existing.utf8.elementsEqual(candidate.utf8) { return }
            }
            schemas.append(candidate)
        }
        var containsExplicitCatalog = false
        for entry in resolution.searchPath.entries {
            try workMeter.consume(at: .expressionEvaluation)
            let candidate: String?
            switch entry {
            case .currentUser: candidate = resolution.currentUser
            case .schema(let name): candidate = name
            }
            guard let candidate else { continue }
            var exists = candidate.utf8.elementsEqual("pg_catalog".utf8)
            if exists { containsExplicitCatalog = true }
            if !exists {
                for namespace in catalog.namespaces {
                    try DatabaseByteProcessingMeter.consume(byteCount: UInt64(namespace.name.utf8.count) + UInt64(candidate.utf8.count),
                        workMeter: workMeter, stage: .expressionEvaluation)
                    if namespace.name.utf8.elementsEqual(candidate.utf8) { exists = true; break }
                }
            }
            if exists { try append(candidate) }
        }
        if includeImplicit && !containsExplicitCatalog {
            try workMeter.consume(UInt64(schemas.count) + 1, at: .expressionEvaluation)
            schemas.insert("pg_catalog", at: 0)
        }
        if name == "current_schema" { return try text(schemas.first) }
        let limits = QueryValueLimits(
            maximumRetainedBytes: workMeter.budget.maximumIntermediateBytes,
            maximumTotalNodes: workMeter.budget.maximumWorkUnits,
            maximumNestingDepth: 2,
            maximumCollectionElements: workMeter.budget.maximumWorkUnits)
        return try SQLArrayStorage.makeArray(element: .text, rank: schemas.isEmpty ? 0 : 1,
            count: schemas.count, meter: workMeter, limits: limits,
            dimension: { _ in try SQLArrayStorage.dimension(length: schemas.count) },
            cell: { index, claim in
                try claim.reserveAdditional(bytes: 64 + UInt64(schemas[index].utf8.count), at: .expressionEvaluation)
                return .primitive(.string(schemas[index]))
            })
    }
}
