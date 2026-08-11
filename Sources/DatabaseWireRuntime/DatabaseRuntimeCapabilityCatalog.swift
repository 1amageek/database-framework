@_spi(DatabaseWireRuntime) import DatabaseWire

enum DatabaseRuntimeCapabilityCatalog {
    static func operations(
        includesSchemaExecution: Bool,
        includesJobs: Bool
    ) -> [DatabaseOperationIdentifier] {
        var operations: [DatabaseOperationIdentifier] = [
            .capabilitiesDescribe,
            .schemaDescribe,
            .grantExecute,
            .queryExecute,
            .mutationExecute,
            .commandExecute,
            .maintenanceExecute,
        ]
        #if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
        operations.append(contentsOf: [
            .baseExecute,
            .compositionExecute,
        ])
        #endif
        if includesJobs {
            operations.append(contentsOf: [
                .jobStart,
                .jobStatus,
                .jobResult,
                .jobCancel,
            ])
        }
        if includesSchemaExecution {
            operations.append(.schemaExecute)
        }
        #if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
        operations.append(contentsOf: [
            .graphAlgorithm,
            .ontologyExecute,
            .shaclExecute,
        ])
        #endif
        return operations.sorted { $0.rawValue < $1.rawValue }
    }

    static func features(
        includesSchemaExecution: Bool,
        includesJobs: Bool,
        includesDurableQueryPaging: Bool = false,
        includesDurableCompositionPaging: Bool = false
    ) -> [CapabilitiesDescribeOperation.Feature] {
        var features = operations(
            includesSchemaExecution: includesSchemaExecution,
            includesJobs: includesJobs
        ).map {
            CapabilitiesDescribeOperation.Feature(
                identifier: identifier(for: $0),
                version: 1
            )
        }
        if includesDurableQueryPaging {
            features.append(
                CapabilitiesDescribeOperation.Feature(
                    identifier: "query.durable-paging",
                    version: 1
                )
            )
        }
        #if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
        features.append(contentsOf: [
            CapabilitiesDescribeOperation.Feature(
                identifier: "composition.query.scan-filter-project",
                version: 1
            ),
            CapabilitiesDescribeOperation.Feature(
                identifier: "composition.query.global-order-window",
                version: 1
            ),
            CapabilitiesDescribeOperation.Feature(
                identifier: "composition.query.distinct-provenance",
                version: 1
            ),
            CapabilitiesDescribeOperation.Feature(
                identifier: "composition.query.aggregate.count",
                version: 1
            ),
            CapabilitiesDescribeOperation.Feature(
                identifier: "composition.query.aggregate.sum",
                version: 1
            ),
            CapabilitiesDescribeOperation.Feature(
                identifier: "composition.query.aggregate.min",
                version: 1
            ),
            CapabilitiesDescribeOperation.Feature(
                identifier: "composition.query.aggregate.max",
                version: 1
            ),
            CapabilitiesDescribeOperation.Feature(
                identifier: "composition.query.aggregate.avg",
                version: 1
            ),
        ])
        #if DATABASE_WIRE_RUNTIME_VECTOR_INDEXES
        features.append(
            CapabilitiesDescribeOperation.Feature(
                identifier: "composition.query.vector",
                version: 1
            )
        )
        #endif
        #if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
        features.append(contentsOf: [
            CapabilitiesDescribeOperation.Feature(
                identifier: "composition.query.sparql-select",
                version: 1
            ),
            CapabilitiesDescribeOperation.Feature(
                identifier: "composition.query.sparql-ask",
                version: 1
            ),
        ])
        #endif
        if includesDurableCompositionPaging {
            features.append(
                CapabilitiesDescribeOperation.Feature(
                    identifier: "composition.query.durable-paging",
                    version: 1
                )
            )
            #if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
            features.append(
                CapabilitiesDescribeOperation.Feature(
                    identifier: "composition.query.sparql-rdf-union",
                    version: 1
                )
            )
            #endif
        }
        #endif
        return features.sorted { $0.identifier < $1.identifier }
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
        case .baseExecute:
            "base.execute"
        case .compositionExecute:
            "composition.execute"
        case .grantExecute:
            "grant.execute"
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
