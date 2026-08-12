import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
@_spi(DatabaseOperations) import DatabaseWire

/// Associates a closed DatabaseWire operation with its execution implementation.
///
/// `DatabaseOperationCatalog` owns the protocol descriptors and DatabaseWire
/// owns their binary representation. `DatabaseOperations` owns handler
/// selection and execution.
public protocol DatabaseOperationDeclaration: Sendable {
    associatedtype Request: Sendable
    associatedtype Response: Sendable

    static var operation: DatabaseOperation<Request, Response> { get }
}

extension CapabilitiesDescribeOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperationCatalog.capabilitiesDescribe
    }
}

extension SchemaDescribeOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperationCatalog.schemaDescribe
    }
}

extension SchemaExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperationCatalog.schemaExecute
    }
}

extension BaseExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperationCatalog.baseExecute
    }
}

extension CompositionExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperationCatalog.compositionExecute
    }
}

extension GrantExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperationCatalog.grantExecute
    }
}

extension QueryExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperationCatalog.queryExecute
    }
}

extension MutationExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperationCatalog.mutationExecute
    }
}

#if DATABASE_OPERATIONS_GRAPH_INDEXES
extension GraphAlgorithmOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperationCatalog.graphAlgorithm
    }
}

extension OntologyExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperationCatalog.ontologyExecute
    }
}

extension SHACLExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperationCatalog.shaclExecute
    }
}
#endif

extension CommandExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperationCatalog.commandExecute
    }
}

extension MaintenanceExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperationCatalog.maintenanceExecute
    }
}

extension JobStartOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperationCatalog.jobStart
    }
}

extension JobStatusOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperationCatalog.jobStatus
    }
}

extension JobResultOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperationCatalog.jobResult
    }
}

extension JobCancelOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperationCatalog.jobCancel
    }
}
