#if SQLITE
import DatabaseKit
import DatabaseOperations
import TestSupport

struct SQLiteJobAuthorizationValidator:
    DatabaseJobAuthorizationValidating {
    static let referenceValue = "sqlite-test-authentication-record"

    func revalidate(
        _ reference: DatabaseJobAuthorizationReference
    ) async throws -> AuthorizationContext {
        guard reference.value == Self.referenceValue else {
            throw DatabaseJobAuthorizationError.revalidationFailed
        }
        return TestBaseEnvironment.authorization
    }

    static func reference() throws -> DatabaseJobAuthorizationReference {
        try DatabaseJobAuthorizationReference(referenceValue)
    }
}
#endif
