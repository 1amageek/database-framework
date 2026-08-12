import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine

public final class DatabaseOperationConfiguration: Sendable {
    public let identity: DatabaseOperationIdentity
    public let admissionPolicy: AnyDatabaseOperationAdmissionPolicy
    public let middlewares: [AnyDatabaseRequestMiddleware]
    public let runtimeLimits: DatabaseOperationLimits
    public let schemaRuntimeFactory: AnyDatabaseSchemaRuntimeFactory?
    #if DATABASE_OPERATIONS_GRAPH_INDEXES
    public let graphOperationLimits: GraphOperationLimits
    #endif
    private let serviceFactory: AnyDatabaseOperationServiceFactory

    public init(
        identity: DatabaseOperationIdentity,
        serviceFactory: AnyDatabaseOperationServiceFactory,
        admissionPolicy: AnyDatabaseOperationAdmissionPolicy,
        schemaRuntimeFactory: AnyDatabaseSchemaRuntimeFactory? = nil,
        middlewares: [AnyDatabaseRequestMiddleware] = [],
        runtimeLimits: DatabaseOperationLimits = .default
    ) throws(DatabaseOperationConfigurationError) {
        try runtimeLimits.validateConfiguration()
        self.identity = identity
        self.serviceFactory = serviceFactory
        self.admissionPolicy = admissionPolicy
        self.middlewares = middlewares
        self.runtimeLimits = runtimeLimits
        self.schemaRuntimeFactory = schemaRuntimeFactory
        #if DATABASE_OPERATIONS_GRAPH_INDEXES
        self.graphOperationLimits = .default
        #endif
    }

    #if DATABASE_OPERATIONS_GRAPH_INDEXES
    public init(
        identity: DatabaseOperationIdentity,
        serviceFactory: AnyDatabaseOperationServiceFactory,
        admissionPolicy: AnyDatabaseOperationAdmissionPolicy,
        graphOperationLimits: GraphOperationLimits,
        schemaRuntimeFactory: AnyDatabaseSchemaRuntimeFactory? = nil,
        middlewares: [AnyDatabaseRequestMiddleware] = [],
        runtimeLimits: DatabaseOperationLimits = .default
    ) throws(DatabaseOperationConfigurationError) {
        try runtimeLimits.validateConfiguration()
        self.identity = identity
        self.serviceFactory = serviceFactory
        self.admissionPolicy = admissionPolicy
        self.middlewares = middlewares
        self.runtimeLimits = runtimeLimits
        self.schemaRuntimeFactory = schemaRuntimeFactory
        self.graphOperationLimits = graphOperationLimits
    }
    #endif

    public func makeServices(
        context: DatabaseOperationServiceContext
    ) async throws -> DatabaseOperationServices {
        try await serviceFactory.makeServices(context: context)
    }
}
