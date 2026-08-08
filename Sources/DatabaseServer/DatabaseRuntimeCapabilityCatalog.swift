@_spi(DatabaseServer) import DatabaseWire

enum DatabaseRuntimeCapabilityCatalog {
    static func operations(
        includesSchemaExecution: Bool
    ) -> [DatabaseOperationIdentifier] {
        var operations: [DatabaseOperationIdentifier] = [
            .capabilitiesDescribe,
            .schemaDescribe,
            .queryExecute,
            .mutationExecute,
            .commandExecute,
            .maintenanceExecute,
            .jobStart,
            .jobStatus,
            .jobResult,
            .jobCancel,
        ]
        if includesSchemaExecution {
            operations.append(.schemaExecute)
        }
        #if DATABASE_SERVER_GRAPH_INDEXES
        operations.append(contentsOf: [
            .graphAlgorithm,
            .ontologyExecute,
            .shaclExecute,
        ])
        #endif
        return operations.sorted { $0.rawValue < $1.rawValue }
    }

    static func features(
        includesSchemaExecution: Bool
    ) -> [CapabilitiesDescribeOperation.Feature] {
        operations(includesSchemaExecution: includesSchemaExecution).map {
        CapabilitiesDescribeOperation.Feature(
            identifier: identifier(for: $0),
            version: 1
        )
        }
    }

    private static func identifier(
        for operation: DatabaseOperationIdentifier
    ) -> String {
        switch operation {
        case .capabilitiesDescribe:
            "capabilities.describe"
        case .schemaDescribe:
            "schema.describe"
        case .schemaExecute:
            "schema.execute"
        case .queryExecute:
            "query.execute"
        case .mutationExecute:
            "mutation.execute"
        case .graphAlgorithm:
            "graph.algorithm"
        case .ontologyExecute:
            "ontology.execute"
        case .shaclExecute:
            "shacl.execute"
        case .commandExecute:
            "command.execute"
        case .maintenanceExecute:
            "maintenance.execute"
        case .jobStart:
            "job.start"
        case .jobStatus:
            "job.status"
        case .jobResult:
            "job.result"
        case .jobCancel:
            "job.cancel"
        }
    }
}
