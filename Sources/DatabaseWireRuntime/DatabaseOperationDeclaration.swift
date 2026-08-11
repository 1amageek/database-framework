@_spi(DatabaseWireRuntime) import DatabaseWire

/// Associates a closed DatabaseWire operation with its execution implementation.
///
/// DatabaseWire owns the protocol identifiers and binary representations.
/// DatabaseWireRuntime owns only the handler selected for each operation.
public protocol DatabaseOperationDeclaration: Sendable {
    associatedtype Request: Sendable
    associatedtype Response: Sendable

    static var operation: DatabaseOperation<Request, Response> { get }
}

extension CapabilitiesDescribeOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.capabilitiesDescribe
    }
}

extension SchemaDescribeOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.schemaDescribe
    }
}

extension SchemaExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.schemaExecute
    }
}

extension BaseExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.baseExecute
    }
}

extension CompositionExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.compositionExecute
    }
}

extension GrantExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.grantExecute
    }
}

extension QueryExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.queryExecute
    }
}

extension MutationExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.mutationExecute
    }
}

#if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
extension GraphAlgorithmOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.graphAlgorithm
    }
}

extension OntologyExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.ontologyExecute
    }
}

extension SHACLExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.shaclExecute
    }
}
#endif

extension CommandExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.commandExecute
    }
}

extension MaintenanceExecuteOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.maintenanceExecute
    }
}

extension JobStartOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.jobStart
    }
}

extension JobStatusOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.jobStatus
    }
}

extension JobResultOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.jobResult
    }
}

extension JobCancelOperation: DatabaseOperationDeclaration {
    public static var operation: DatabaseOperation<Request, Response> {
        DatabaseOperations.jobCancel
    }
}
