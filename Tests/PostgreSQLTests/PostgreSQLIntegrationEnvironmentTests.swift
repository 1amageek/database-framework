#if POSTGRESQL
import TestSupport
import Testing

@Suite("PostgreSQL Integration Environment")
struct PostgreSQLIntegrationEnvironmentTests {
    @Test("PostgreSQL integration endpoint is explicitly configured")
    func endpointIsConfigured() {
        #expect(
            PostgreSQLScenarioCoordinator.isConfigured,
            "Set POSTGRES_TEST_UNIX_SOCKET or POSTGRES_TEST_HOST before running PostgreSQLTests."
        )
    }
}
#endif
