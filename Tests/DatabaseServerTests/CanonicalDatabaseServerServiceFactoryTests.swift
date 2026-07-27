import DatabaseKit
import DatabaseEngine
import DatabaseRuntime
import DatabaseServer
import DatabaseWire
import StorageKit
import Testing

@Suite("Canonical database server service factory")
struct CanonicalDatabaseServerServiceFactoryTests {
    @Test("factory composes every canonical database service")
    func composesCanonicalServices() async throws {
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try DatabaseGraphSourceEdge.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [DatabaseGraphSourceEdge.self]
            ),
            security: .disabled
        )
        let stateStore = try await DatabaseMutationStateStore(
            container: container
        )
        let context = DatabaseServerServiceContext(
            container: container,
            stateStore: stateStore,
            coordinator: DatabaseTransactionalOperationCoordinator(
                stateStore: stateStore
            ),
            runtimeLimits: .default,
            wireLimits: .default
        )
        let platform = RejectingPlatformServices()
        let services = try await CanonicalDatabaseServerServiceFactory(
            maintenanceServiceFactory: platform,
            jobServiceFactory: platform
        ).makeServices(context: context)

        #expect(services.readCommandRegistry.identifiers.isEmpty)
        #expect(services.writeCommandRegistry.identifiers.isEmpty)
        await #expect(throws: UnexpectedPlatformInvocation.self) {
            try await services.jobService.runScheduledWork()
        }
    }

    private struct RejectingPlatformServices:
        DatabaseMaintenanceServiceFactory,
        DatabaseMaintenanceService,
        DatabaseJobServiceFactory,
        DatabaseJobService {
        var jobOperations: [JobOperationIdentifier] { [] }

        func makeMaintenanceService(
            context: DatabaseServerServiceContext
        ) async throws -> AnyDatabaseMaintenanceService {
            _ = context
            return AnyDatabaseMaintenanceService(self)
        }

        func makeJobService(
            context: DatabaseServerServiceContext
        ) async throws -> AnyDatabaseJobService {
            _ = context
            return AnyDatabaseJobService(self)
        }

        func execute(
            _ request: MaintenanceExecuteOperation.Request,
            context: DatabaseOperationContext
        ) async throws
            -> DatabasePreparedOperationResponse<MaintenanceExecuteOperation> {
            _ = request
            _ = context
            throw UnexpectedPlatformInvocation.unexpectedInvocation
        }

        func start(
            _ request: JobStartOperation.Request,
            context: DatabaseOperationContext
        ) async throws
            -> DatabasePreparedOperationResponse<JobStartOperation> {
            _ = request
            _ = context
            throw UnexpectedPlatformInvocation.unexpectedInvocation
        }

        func status(
            _ request: JobStatusOperation.Request,
            context: DatabaseOperationContext
        ) async throws -> JobStatusOperation.Response {
            _ = request
            _ = context
            throw UnexpectedPlatformInvocation.unexpectedInvocation
        }

        func result(
            _ request: JobResultOperation.Request,
            context: DatabaseOperationContext
        ) async throws -> JobResultOperation.Response {
            _ = request
            _ = context
            throw UnexpectedPlatformInvocation.unexpectedInvocation
        }

        func cancel(
            _ request: JobCancelOperation.Request,
            context: DatabaseOperationContext
        ) async throws
            -> DatabasePreparedOperationResponse<JobCancelOperation> {
            _ = request
            _ = context
            throw UnexpectedPlatformInvocation.unexpectedInvocation
        }

        func runScheduledWork() async throws {
            throw UnexpectedPlatformInvocation.unexpectedInvocation
        }
    }

    private enum UnexpectedPlatformInvocation: Error {
        case unexpectedInvocation
    }
}
