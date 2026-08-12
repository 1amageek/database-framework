import DatabaseKit
import DatabaseEngine
import DatabaseRuntime
@testable import DatabaseOperations
@_spi(DatabaseOperations) import DatabaseWire
import StorageKit
import TestSupport
import Testing

@Suite("Grant execution requirements")
struct GrantExecuteHandlerTests {
    @Test("Effective access reads only the authenticated principal")
    func effectiveAccessUsesReadAdmission() async throws {
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try DatabaseEndpointEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        DatabaseEndpointEntity.self
                    ),
                ]
            ),
            security: .testingDisabled
        )
        let handler = GrantExecuteHandler(
            coordinator: DatabaseTransactionalOperationCoordinator(
                stateStore: DatabaseMutationStateStore(container: container)
            )
        )
        let requirement = try handler.requirement(
            for: GrantExecuteOperation.Request(invocation: .effective)
        )

        #expect(requirement.access == .read)
        #expect(requirement.transaction == .read)

        let direct = try handler.requirement(
            for: GrantExecuteOperation.Request(
                invocation: .direct(subject: nil)
            )
        )
        #expect(direct.access == .administer)
        #expect(direct.transaction == .read)

        let grant = Security.Grant(
            subject: .principal("reader"),
            resource: .database,
            access: .read
        )
        let mutation = try handler.requirement(
            for: GrantExecuteOperation.Request(
                invocation: .grant(
                    grant,
                    expectedRevision: 0,
                    idempotencyKey: "grant-reader"
                )
            )
        )
        #expect(mutation.access == .administer)
        #expect(mutation.transaction == .write)
    }
}
