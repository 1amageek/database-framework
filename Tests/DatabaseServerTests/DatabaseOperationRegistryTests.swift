import DatabaseServer
import DatabaseWire
import Testing

@Suite("Database operation registry")
struct DatabaseOperationRegistryTests {
    @Test("initialization fails when a required handler is missing")
    func missingRequiredHandlerFailsFast() {
        let handler = capabilityHandler(runtimeVersion: "test")

        do {
            _ = try DatabaseOperationRegistry(
                handlers: [AnyDatabaseOperationHandler(handler)],
                requiredOperations: [.capabilitiesDescribe, .schemaDescribe]
            )
            Issue.record("Expected a missing-handler error")
        } catch DatabaseOperationRegistryError.missing(let operations) {
            #expect(operations == [.schemaDescribe])
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("initialization rejects duplicate handlers")
    func duplicateHandlerFailsFast() {
        let first = capabilityHandler(runtimeVersion: "first")
        let second = capabilityHandler(runtimeVersion: "second")

        do {
            _ = try DatabaseOperationRegistry(
                handlers: [
                    AnyDatabaseOperationHandler(first),
                    AnyDatabaseOperationHandler(second),
                ],
                requiredOperations: [.capabilitiesDescribe]
            )
            Issue.record("Expected a duplicate-handler error")
        } catch DatabaseOperationRegistryError.duplicate(let operation) {
            #expect(operation == .capabilitiesDescribe)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    private func capabilityHandler(
        runtimeVersion: String
    ) -> DatabaseOperationRoute<CapabilitiesDescribeOperation> {
        DatabaseOperationRoute { _, _ in
            CapabilitiesDescribeOperation.Response(
                runtimeVersion: runtimeVersion,
                features: [],
                jobOperations: []
            )
        }
    }
}
