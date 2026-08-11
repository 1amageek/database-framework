#if !MultipleBases
import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseWireRuntime
import DatabaseFoundation
import DatabaseTypes
import DatabaseWire
import StorageKit
import StorageKitSystemClock
import Testing

@Persistable
private struct SingleRootEntity: SecurityPolicy {
    #Directory<SingleRootEntity>("single-root", "entities")

    var id: String = ""
    var value: String = ""

    static func permitsRead(
        of resource: borrowing SingleRootEntity,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        _ = resource
        return context.isAuthenticated
    }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        _ = query
        return context.isAuthenticated
    }

    static func permitsCreate(
        _ newResource: borrowing SingleRootEntity,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        _ = newResource
        return context.isAuthenticated
    }

    static func permitsUpdate(
        from resource: borrowing SingleRootEntity,
        to newResource: borrowing SingleRootEntity,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        _ = resource
        _ = newResource
        return context.isAuthenticated
    }

    static func permitsDelete(
        _ resource: borrowing SingleRootEntity,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        _ = resource
        return context.isAuthenticated
    }
}

@Suite("Single database data root", .serialized)
struct DatabaseSingleRootRuntimeTests {
    private static let authorization = AuthorizationContext.authenticated(
        Principal(identifier: "single-root-admin", roles: ["admin"])
    )

    @Test("local contexts persist through the database data root")
    func localContextPersistsData() async throws {
        let container = try await makeContainer()
        defer {
            Task { await container.shutdown() }
        }
        let context = container.newContext(authorization: Self.authorization)
        var entity = SingleRootEntity()
        entity.id = "entity-1"
        entity.value = "stored"

        try context.insert(entity)
        try await context.save()

        let stored = try await context.model(
            for: entity.id,
            as: SingleRootEntity.self
        )
        #expect(stored?.value == "stored")
        await container.shutdown()
    }

    @Test("runtime advertises only database-root operations")
    func capabilitiesExcludeMultipleBaseOperations() async throws {
        let container = try await makeContainer()
        let runtime = try await makeRuntime(container: container)

        let response = try await invoke(
            DatabaseOperations.capabilitiesDescribe,
            request: EmptyOperationPayload(),
            requestID: 1,
            target: .database,
            runtime: runtime
        )
        let identifiers = Set(response.features.map(\.identifier))
        #expect(identifiers.contains("query.execute"))
        #expect(identifiers.contains("mutation.execute"))
        #expect(!identifiers.contains("base.execute"))
        #expect(!identifiers.contains("composition.execute"))
        #expect(!identifiers.contains { $0.hasPrefix("composition.") })
        await container.shutdown()
    }

    @Test("Base targets fail without falling back to the database root")
    func baseTargetIsRejected() async throws {
        let container = try await makeContainer()
        let runtime = try await makeRuntime(container: container)
        let requestID: UInt64 = 2
        let frame = try DatabaseWireEncoder().encodeRequest(
            DatabaseOperations.capabilitiesDescribe,
            requestID: requestID,
            target: .base(try Base.ID("unavailable")),
            metadata: OperationRequestMetadata(),
            request: EmptyOperationPayload()
        )
        let response = try DatabaseWireDecoder().decodeResponse(
            DatabaseOperations.capabilitiesDescribe,
            from: try await runtime.execute(
                frame,
                context: DatabaseRequestExecutionContext(
                    authorization: Self.authorization
                )
            ),
            matching: requestID
        )

        guard case .failure(let error) = response else {
            Issue.record("Expected a typed target rejection")
            await container.shutdown()
            return
        }
        #expect(error.category == .invalidRequest)
        #expect(error.code == "TARGET_KIND_NOT_ACCEPTED")
        await container.shutdown()
    }

    @Test("effective Grants are evaluated on the database resource")
    func databaseGrantIsEffective() async throws {
        let container = try await makeContainer()
        let runtime = try await makeRuntime(container: container)
        let response = try await invoke(
            DatabaseOperations.grantExecute,
            request: GrantExecuteOperation.Request(invocation: .effective),
            requestID: 3,
            target: .database,
            runtime: runtime
        )

        guard case .effective(let effective) = response else {
            Issue.record("Expected an effective Grant response")
            await container.shutdown()
            return
        }
        #expect(effective.access == .all)
        #expect(effective.contributors.count == 1)
        #expect(effective.contributors[0].resource == .database)
        await container.shutdown()
    }

    @Test("read access can inspect only its own effective Grant")
    func readPrincipalCanInspectEffectiveGrant() async throws {
        let container = try await makeContainer()
        let runtime = try await makeRuntime(container: container)
        let reader = Principal(identifier: "single-root-reader", roles: [])
        let key = "grant-single-root-reader"
        _ = try await invoke(
            DatabaseOperations.grantExecute,
            request: GrantExecuteOperation.Request(
                invocation: .grant(
                    Security.Grant(
                        subject: .principal(reader.identifier),
                        resource: .database,
                        access: .read
                    ),
                    expectedRevision: 1,
                    idempotencyKey: key
                )
            ),
            requestID: 4,
            target: .database,
            metadata: OperationRequestMetadata(idempotencyKey: key),
            runtime: runtime
        )
        let response = try await invoke(
            DatabaseOperations.grantExecute,
            request: GrantExecuteOperation.Request(invocation: .effective),
            requestID: 5,
            target: .database,
            authorization: .authenticated(reader),
            runtime: runtime
        )

        guard case .effective(let effective) = response else {
            Issue.record("Expected an effective Grant response")
            await container.shutdown()
            return
        }
        #expect(effective.access == .read)
        #expect(effective.contributors.map(\.subject) == [
            .principal(reader.identifier),
        ])
        await container.shutdown()
    }

    private func makeContainer() async throws -> DBContainer {
        let engine = InMemoryEngine()
        let domainID = try DatabaseStorageDomain.ID("primary")
        let topology = DatabaseStorageTopology(
            controlDomain: try DatabaseStorageDomain(
                id: domainID,
                namespacePath: ["database", "single-root-tests"],
                storageEngine: engine
            )
        )
        return try await DBContainer.open(
            for: try Schema(
                entities: [try SingleRootEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(
                storageTopology: topology,
                monotonicClock: SystemStorageClock(),
                wallClock: RealtimeDatabaseWallClock()
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SingleRootEntity.self
                    ),
                ],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(SingleRootEntity.self),
                ]
            )
        )
    }

    private func makeRuntime(
        container: DBContainer
    ) async throws -> DatabaseOperationRuntime {
        try await DatabaseOperationRuntime(
            container: container,
            configuration: try DatabaseOperationRuntimeConfiguration(
                identity: DatabaseRuntimeIdentity(version: "single-root-test"),
                serviceFactory: AnyDatabaseOperationServiceFactory(
                    SingleRootServiceFactory()
                ),
                admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                    UnrestrictedDatabaseOperationAdmissionPolicy()
                ),
                clock: RealtimeDatabaseWallClock()
            )
        )
    }

    private func invoke<Request, Response>(
        _ operation: DatabaseOperation<Request, Response>,
        request: Request,
        requestID: UInt64,
        target: DatabaseOperationTarget,
        metadata: OperationRequestMetadata = OperationRequestMetadata(),
        authorization: AuthorizationContext = Self.authorization,
        runtime: DatabaseOperationRuntime
    ) async throws -> Response {
        let frame = try DatabaseWireEncoder().encodeRequest(
            operation,
            requestID: requestID,
            target: target,
            metadata: metadata,
            request: request
        )
        let response = try DatabaseWireDecoder().decodeResponse(
            operation,
            from: try await runtime.execute(
                frame,
                context: DatabaseRequestExecutionContext(
                    authorization: authorization
                )
            ),
            matching: requestID
        )
        switch response {
        case .success(let value):
            return value
        case .failure(let error):
            throw error
        }
    }
}

private final class SingleRootServiceFactory: DatabaseOperationServiceFactory {
    func makeServices(
        context: DatabaseOperationServiceContext
    ) async throws -> DatabaseOperationServices {
        let unavailable = SingleRootUnavailableService()
        #if GraphIndexes
        return DatabaseOperationServices(
            graphOperations: GraphOperationServices(
                statementExecutor: CanonicalDatabaseStatementMutationExecutor(
                    runtimeLimits: context.runtimeLimits
                ),
                algorithm: AnyDatabaseGraphAlgorithmService(unavailable),
                ontology: AnyDatabaseOntologyService(unavailable),
                shacl: AnyDatabaseSHACLService(unavailable)
            ),
            readCommandRegistry: try DatabaseReadCommandRegistry(commands: []),
            writeCommandRegistry: try DatabaseWriteCommandRegistry(commands: []),
            maintenanceService: AnyDatabaseMaintenanceService(unavailable),
            jobService: AnyDatabaseJobService(unavailable)
        )
        #else
        return DatabaseOperationServices(
            statementExecutor: CanonicalDatabaseStatementMutationExecutor(
                runtimeLimits: context.runtimeLimits
            ),
            readCommandRegistry: try DatabaseReadCommandRegistry(commands: []),
            writeCommandRegistry: try DatabaseWriteCommandRegistry(commands: []),
            maintenanceService: AnyDatabaseMaintenanceService(unavailable),
            jobService: AnyDatabaseJobService(unavailable)
        )
        #endif
    }
}

private struct SingleRootUnavailableService:
    DatabaseMaintenanceService,
    DatabaseJobService
{
    let jobOperations: [JobOperationIdentifier] = []

    func baseAdmission(
        for operation: JobOperationIdentifier
    ) throws -> DatabaseBaseAdmissionKind {
        _ = operation
        throw SingleRootUnavailableError()
    }

    func execute(
        _ request: MaintenanceExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> MaintenanceExecutionResult {
        _ = request
        _ = context
        throw SingleRootUnavailableError()
    }

    func start(
        _ request: JobStartOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobStartExecutionResult {
        _ = request
        _ = context
        throw SingleRootUnavailableError()
    }

    func status(
        _ request: JobStatusOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobStatusOperation.Response {
        _ = request
        _ = context
        throw SingleRootUnavailableError()
    }

    func result(
        _ request: JobResultOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobResultOperation.Response {
        _ = request
        _ = context
        throw SingleRootUnavailableError()
    }

    func cancel(
        _ request: JobCancelOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> JobCancellationExecutionResult {
        _ = request
        _ = context
        throw SingleRootUnavailableError()
    }

    func runScheduledWork() async throws {
        throw SingleRootUnavailableError()
    }

}

#if GraphIndexes
extension SingleRootUnavailableService:
    DatabaseGraphAlgorithmService,
    DatabaseOntologyService,
    DatabaseSHACLService {
    func execute(
        _ request: GraphAlgorithmOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> GraphAlgorithmOperation.Response {
        _ = request
        _ = context
        throw SingleRootUnavailableError()
    }

    func execute(
        _ request: OntologyExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> OntologyExecutionResult {
        _ = request
        _ = context
        throw SingleRootUnavailableError()
    }

    func execute(
        _ request: SHACLExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> SHACLExecutionResult {
        _ = request
        _ = context
        throw SingleRootUnavailableError()
    }
}
#endif

private struct SingleRootUnavailableError: Error {}
#endif
