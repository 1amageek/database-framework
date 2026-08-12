import DatabaseKit
import DatabaseEngine
@testable import DatabaseOperations
import TestSupport

actor TestDatabaseJobAuthorizationValidator:
    DatabaseJobAuthorizationValidating {
    static let referenceValue = "test-authentication-record"

    private var records: [
        DatabaseJobAuthorizationReference: AuthorizationContext
    ]

    init(
        authorization: AuthorizationContext = TestBaseEnvironment.authorization
    ) throws {
        self.records = [
            try Self.reference(): authorization,
        ]
    }

    static func reference() throws -> DatabaseJobAuthorizationReference {
        try DatabaseJobAuthorizationReference(referenceValue)
    }

    func set(
        _ authorization: AuthorizationContext,
        for reference: DatabaseJobAuthorizationReference
    ) {
        records[reference] = authorization
    }

    func revoke(_ reference: DatabaseJobAuthorizationReference) {
        records.removeValue(forKey: reference)
    }

    func revalidate(
        _ reference: DatabaseJobAuthorizationReference
    ) async throws -> AuthorizationContext {
        guard let authorization = records[reference] else {
            throw DatabaseJobAuthorizationError.revalidationFailed
        }
        return authorization
    }
}

func testJobHostServices<Scheduler: DatabaseJobScheduler>(
    scheduler: Scheduler,
    authorization: AuthorizationContext = TestBaseEnvironment.authorization
) throws -> DatabaseOperationHostServices {
    let validator = try TestDatabaseJobAuthorizationValidator(
        authorization: authorization
    )
    return DatabaseOperationHostServices(
        jobScheduler: AnyDatabaseJobScheduler(scheduler),
        jobAuthorizationValidator: AnyDatabaseJobAuthorizationValidator(
            validator
        )
    )
}

func testJobHostServices<
    Scheduler: DatabaseJobScheduler,
    Generator: DatabaseUUIDGenerator
>(
    scheduler: Scheduler,
    identifierGenerator: Generator,
    authorization: AuthorizationContext = TestBaseEnvironment.authorization
) throws -> DatabaseOperationHostServices {
    let validator = try TestDatabaseJobAuthorizationValidator(
        authorization: authorization
    )
    return DatabaseOperationHostServices(
        jobScheduler: AnyDatabaseJobScheduler(scheduler),
        identifierGenerator: AnyDatabaseUUIDGenerator(identifierGenerator),
        jobAuthorizationValidator: AnyDatabaseJobAuthorizationValidator(
            validator
        )
    )
}
