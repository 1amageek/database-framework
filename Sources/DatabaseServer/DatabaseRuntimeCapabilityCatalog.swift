@_spi(DatabaseServer) import DatabaseWire

enum DatabaseRuntimeCapabilityCatalog {
    static let features = DatabaseOperationIdentifier.allCases
        .sorted { $0.rawValue < $1.rawValue }
        .map {
            CapabilitiesDescribeOperation.Feature(
                identifier: identifier(for: $0),
                version: 1
            )
        }

    private static func identifier(
        for operation: DatabaseOperationIdentifier
    ) -> String {
        switch operation {
        case .capabilitiesDescribe:
            "capabilities.describe"
        case .schemaDescribe:
            "schema.describe"
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
