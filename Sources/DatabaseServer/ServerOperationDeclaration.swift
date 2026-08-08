@_spi(DatabaseServer) import DatabaseWire

/// Associates a closed DatabaseWire operation with its server implementation.
///
/// DatabaseWire owns the protocol identifiers and binary representations.
/// DatabaseServer owns only the handler selected for each operation.
public protocol ServerOperationDeclaration: Sendable {
    associatedtype Request: Sendable
    associatedtype Response: Sendable

    static var operation: DatabaseOperation<Request, Response> { get }
}

extension CapabilitiesDescribeOperation: ServerOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.capabilitiesDescribe
    }
}

extension SchemaDescribeOperation: ServerOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.schemaDescribe
    }
}

extension SchemaExecuteOperation: ServerOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.schemaExecute
    }
}

extension QueryExecuteOperation: ServerOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.queryExecute
    }
}

extension MutationExecuteOperation: ServerOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.mutationExecute
    }
}

#if DATABASE_SERVER_GRAPH_INDEXES
extension GraphAlgorithmOperation: ServerOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.graphAlgorithm
    }
}

extension OntologyExecuteOperation: ServerOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.ontologyExecute
    }
}

extension SHACLExecuteOperation: ServerOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.shaclExecute
    }
}
#endif

extension CommandExecuteOperation: ServerOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.commandExecute
    }
}

extension MaintenanceExecuteOperation: ServerOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.maintenanceExecute
    }
}

extension JobStartOperation: ServerOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.jobStart
    }
}

extension JobStatusOperation: ServerOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.jobStatus
    }
}

extension JobResultOperation: ServerOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.jobResult
    }
}

extension JobCancelOperation: ServerOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.jobCancel
    }
}
