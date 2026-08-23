import DatabaseKit

/// Request-local proof that one Base member admitted the complete logical
/// read before any physical page or index resolution observed persisted data.
@_spi(DatabaseExecution)
public struct DatabaseReadAuthorizationAdmission: Sendable {
    fileprivate let container: DBContainer
    fileprivate let executionStorage: DatabaseExecutionStorage
    fileprivate let schemaGeneration: UInt64
    fileprivate let authorization: AuthorizationContext
    fileprivate let listAuthorizationByEntity: [String: IndexReadAuthorization]
    fileprivate let fieldsByEntity: [String: Set<String>]

    fileprivate func matches(
        context: DatabaseContext
    ) throws -> Bool {
        guard container === context.container else { return false }
        let currentStorage = try context.executionStorage()
        return executionStorage == currentStorage
            && schemaGeneration
                == context.container.acquireActiveSchemaLease().generation
            && authorization == context.authorization
    }

    package func coversList(
        entityName: String,
        authorization: IndexReadAuthorization,
        context: DatabaseContext
    ) throws -> Bool {
        try matches(context: context)
            && listAuthorizationByEntity[entityName] == authorization
    }

    package func coversFields(
        _ requested: [String: Set<String>],
        context: DatabaseContext
    ) throws -> Bool {
        guard try matches(context: context) else { return false }
        for (entity, fields) in requested {
            guard let admitted = fieldsByEntity[entity],
                  admitted.isSuperset(of: fields) else {
                return false
            }
        }
        return true
    }
}

package enum ActiveDatabaseReadAuthorizationAdmission {
    @TaskLocal package static var admission:
        DatabaseReadAuthorizationAdmission?
}

extension DatabaseContext {
    package func admitLogicalRead(
        listAuthorization: IndexReadAuthorization,
        fieldPlan: DatabaseFieldReadAuthorizationPlan,
        restrictingTo entityNames: Set<String>? = nil
    ) throws -> DatabaseReadAuthorizationAdmission {
        let fieldsByEntity: [String: Set<String>]
        if let entityNames {
            fieldsByEntity = fieldPlan.fieldsByEntity.filter {
                entityNames.contains($0.key)
            }
        } else {
            fieldsByEntity = fieldPlan.fieldsByEntity
        }
        for entityName in fieldsByEntity.keys.sorted() {
            try indexQueryContext.authorizeListAccess(
                entityName: entityName,
                authorization: listAuthorization
            )
        }
        try authorizeFieldReads(
            DatabaseFieldReadAuthorizationPlan(
                fieldsByEntity: fieldsByEntity
            )
        )
        return DatabaseReadAuthorizationAdmission(
            container: container,
            executionStorage: try executionStorage(),
            schemaGeneration: container.acquireActiveSchemaLease().generation,
            authorization: authorization,
            listAuthorizationByEntity: Dictionary(
                uniqueKeysWithValues: fieldsByEntity.keys.map {
                    ($0, listAuthorization)
                }
            ),
            fieldsByEntity: fieldsByEntity
        )
    }

    package func withReadAuthorizationAdmission<Result: Sendable>(
        _ admission: DatabaseReadAuthorizationAdmission,
        _ operation: @Sendable () async throws -> Result
    ) async throws -> Result {
        guard try admission.matches(context: self) else {
            throw DatabaseRuntimeError.internalError(
                "A read authorization admission cannot cross database contexts"
            )
        }
        return try await ActiveDatabaseReadAuthorizationAdmission.$admission
            .withValue(admission) {
                try await RequestFieldAuthorization.$fieldsByEntity.withValue(
                    admission.fieldsByEntity
                ) {
                    try await operation()
                }
            }
    }
}
