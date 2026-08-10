#if SQLITE
import Database
import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseServer
import DatabaseServerFoundation
import DatabaseTypes
import DatabaseWire
import StorageKit
import TestSupport
import TestHeartbeat
import Testing

@Persistable
private struct SchemaDrivenSemanticGraphEdge {
    #Directory<SchemaDrivenSemanticGraphEdge>(
        "schema-driven-runtime",
        "semantic-graph-edges"
    )
    #Index(
        .propertyGraph(strategy: .adjacency),
        from: \SchemaDrivenSemanticGraphEdge.source,
        edge: \SchemaDrivenSemanticGraphEdge.label,
        to: \SchemaDrivenSemanticGraphEdge.target,
        name: "semantic_graph"
    )

    var id: String
    var source: String
    var label: String
    var target: String
}

@Persistable
@OWLClass(
    "urn:semantic:Person",
    individualIRIBase: "urn:semantic:person:",
    graph: "urn:semantic:data"
)
private struct SchemaDrivenSemanticPerson {
    #Directory<SchemaDrivenSemanticPerson>(
        "schema-driven-runtime",
        "semantic-people"
    )

    var id: String

    @OWLDataProperty("urn:semantic:name")
    var name: String?
}

@Suite("Schema-driven semantic runtime SQLite parity", .serialized, .heartbeat)
struct SchemaDrivenSemanticParitySQLiteTests {
    @Test("Graph, RDF, and SHACL execution match the compiled runtime")
    func graphRDFAndSHACLExecutionMatchCompiledRuntime() async throws {
        let schema = try Schema(
            entities: [
                try SchemaDrivenSemanticGraphEdge.schemaEntity,
                try SchemaDrivenSemanticPerson.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )
        let compiled = try await makeContainer(
            schema: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SchemaDrivenSemanticGraphEdge.self
                    ),
                    try DatabaseFrameworkRuntime.entity(
                        SchemaDrivenSemanticPerson.self
                    ),
                ]
            )
        )
        let schemaDriven = try await makeContainer(
            schema: schema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                schema: schema
            )
        )
        defer {
            await compiled.shutdown()
            await schemaDriven.shutdown()
        }

        let compiledRuntime = try await makeRuntime(container: compiled)
        let schemaDrivenRuntime = try await makeRuntime(container: schemaDriven)
        let models = try semanticModels()
        for container in [compiled, schemaDriven] {
            try await save(models, in: container)
        }

        let compiledPath = try await shortestPath(runtime: compiledRuntime)
        let schemaDrivenPath = try await shortestPath(runtime: schemaDrivenRuntime)
        #expect(compiledPath.nodes == schemaDrivenPath.nodes)
        #expect(compiledPath.edgeLabels == schemaDrivenPath.edgeLabels)
        #expect(compiledPath.nodes == [
            .identifier("A"),
            .identifier("B"),
            .identifier("C"),
            .identifier("D"),
        ])

        let compiledASK = try await personExists(runtime: compiledRuntime)
        let schemaDrivenASK = try await personExists(runtime: schemaDrivenRuntime)
        #expect(compiledASK)
        #expect(schemaDrivenASK == compiledASK)

        let rdfIndex = try #require(
            SchemaDrivenSemanticPerson.indexDescriptors.first {
                $0.kindIdentifier == "owl_class_rdf"
            }
        )
        let compiledValidation = try await validateMissingName(
            runtime: compiledRuntime,
            rdfIndex: rdfIndex.name
        )
        let schemaDrivenValidation = try await validateMissingName(
            runtime: schemaDrivenRuntime,
            rdfIndex: rdfIndex.name
        )
        #expect(!compiledValidation.conforms)
        #expect(
            compiledValidation.issues == schemaDrivenValidation.issues
        )
        #expect(compiledValidation.issues.count == 1)
        #expect(
            compiledValidation.issues.first?.focusNode
                == (try OWLIndividualIRIBuilder.term(
                    baseIRI: "urn:semantic:person:",
                    persistableType: SchemaDrivenSemanticPerson.persistableType,
                    identifier: "alice"
                ))
        )
        #expect(
            compiledValidation.issues.first?.code
                == "sh:MinCountConstraintComponent"
        )
    }

    private func makeContainer(
        schema: Schema,
        runtimeConfiguration: DatabaseRuntimeConfiguration
    ) async throws -> DBContainer {
        try await DBContainer.inMemory(
            for: schema,
            runtimeConfiguration: runtimeConfiguration,
            security: .testingDisabled
        )
    }

    private func makeRuntime(
        container: DBContainer
    ) async throws -> DatabaseServerRuntime {
        let identifierGenerator = RandomDatabaseUUIDGenerator()
        let serviceFactory = CanonicalDatabaseServerServiceFactory(
            maintenanceServiceFactory: DatabaseMaintenanceOperationServiceFactory(
                identifierGenerator: identifierGenerator
            ),
            jobServiceFactory: try DatabasePersistentJobServiceFactory(
                registry: DatabaseResumableOperationRegistry(operations: []),
                identifierGenerator: identifierGenerator,
                storageLimits: DatabasePersistentJobStorageLimits(
                    maximumStorageValueBytes: 1_048_576
                )
            )
        )
        return try await DatabaseServerRuntime(
            container: container,
            configuration: try DatabaseServerRuntimeConfiguration(
                identity: DatabaseRuntimeIdentity(version: "semantic-parity-test"),
                serviceFactory: AnyDatabaseServerServiceFactory(serviceFactory),
                admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                    UnrestrictedDatabaseOperationAdmissionPolicy()
                ),
                clock: RealtimeDatabaseWallClock()
            ),
            hostServices: DatabaseServerHostServices(
                jobScheduler: SemanticParityJobScheduler()
            )
        )
    }

    private func semanticModels() throws -> [PersistedModel] {
        let edges = [
            SchemaDrivenSemanticGraphEdge(
                id: "ab",
                source: "A",
                label: "link",
                target: "B"
            ),
            SchemaDrivenSemanticGraphEdge(
                id: "bc",
                source: "B",
                label: "link",
                target: "C"
            ),
            SchemaDrivenSemanticGraphEdge(
                id: "cd",
                source: "C",
                label: "link",
                target: "D"
            ),
        ]
        return try edges.map(PersistedModel.init) + [
            PersistedModel(
                SchemaDrivenSemanticPerson(id: "alice", name: nil)
            ),
        ]
    }

    private func save(
        _ models: [PersistedModel],
        in container: DBContainer
    ) async throws {
        try await container.testBaseContext().withTransaction(
            configuration: .batch
        ) { transaction in
            for model in models {
                try await transaction.savePersistedModel(
                    model,
                    precondition: .notExists
                )
            }
        }
    }

    private func shortestPath(
        runtime: DatabaseServerRuntime
    ) async throws -> MaterializedPath {
        let response = try await invoke(
            DatabaseOperations.graphAlgorithm,
            request: GraphAlgorithmOperation.Request(
                source: GraphAlgorithmOperation.Source(
                    index: "semantic_graph",
                    edgeLabel: .identifier("link")
                ),
                invocation: .shortestPath(
                    source: .identifier("A"),
                    target: .identifier("D"),
                    maximumDepth: 10,
                    bidirectional: false,
                    maximumNodes: 100
                ),
                page: GraphAlgorithmOperation.Page(limit: 10),
                budget: ExecutionBudget(maximumWorkUnits: 1_000)
            ),
            requestID: 1,
            runtime: runtime
        )
        guard case .path(let path) = response else {
            throw SemanticParityTestError.unexpectedGraphResponse
        }
        return MaterializedPath(
            nodes: try path.materializedNodes(maximumCount: 10),
            edgeLabels: try path.materializedEdgeLabels(maximumCount: 10)
        )
    }

    private func personExists(
        runtime: DatabaseServerRuntime
    ) async throws -> Bool {
        let response = try await invoke(
            DatabaseOperations.queryExecute,
            request: QueryExecuteOperation.Request(
                input: .ir(
                    .ask(
                        AskQuery(
                            pattern: .basic([
                                TriplePattern(
                                    subject: .variable("subject"),
                                    predicate: .iri(Self.rdfType),
                                    object: .iri(Self.personClass)
                                ),
                            ]),
                            dataset: .explicit(
                                defaultGraphs: [Self.dataGraph],
                                namedGraphs: []
                            )
                        )
                    )
                ),
                page: QueryExecuteOperation.Page(limit: 1),
                budget: ExecutionBudget(maximumWorkUnits: 1_000)
            ),
            requestID: 2,
            runtime: runtime
        )
        guard case .boolean(let value) = response else {
            throw SemanticParityTestError.unexpectedQueryResponse
        }
        return value.value
    }

    private func validateMissingName(
        runtime: DatabaseServerRuntime,
        rdfIndex: String
    ) async throws -> MaterializedValidation {
        _ = try await invoke(
            DatabaseOperations.shaclExecute,
            request: SHACLExecuteOperation.Request(
                invocation: .upsertShapes(
                    graph: Self.shapesGraph,
                    shapes: try missingNameShapes(),
                    expectedRevision: nil
                )
            ),
            requestID: 3,
            runtime: runtime,
            metadata: OperationRequestMetadata(
                idempotencyKey: "semantic-parity-shapes"
            )
        )
        let response = try await invoke(
            DatabaseOperations.shaclExecute,
            request: SHACLExecuteOperation.Request(
                invocation: .validate(
                    shapesGraph: Self.shapesGraph,
                    data: SHACLExecuteOperation.DataSource(
                        entity: SchemaDrivenSemanticPerson.persistableType,
                        index: rdfIndex,
                        graph: .named(
                            try RDFTerm.iri(validating: Self.dataGraph)
                        )
                    ),
                    focus: .targets,
                    entailment: .none
                ),
                page: QueryExecuteOperation.Page(limit: 10),
                budget: ExecutionBudget(maximumWorkUnits: 10_000)
            ),
            requestID: 4,
            runtime: runtime
        )
        guard case .validation(let report) = response else {
            throw SemanticParityTestError.unexpectedSHACLResponse
        }
        return MaterializedValidation(
            conforms: report.conforms,
            issues: try report.materializedIssues(
                maximumCount: report.issueCount
            )
        )
    }

    private func missingNameShapes() throws -> [RDFQuad] {
        let shape = try RDFTerm.iri(validating: "urn:semantic:PersonShape")
        let property = try RDFTerm.blankNode(identifier: "name-property")
        return [
            try quad(shape, Self.rdfType, .iri(validating: Self.shNodeShape)),
            try quad(shape, Self.shTargetClass, .iri(validating: Self.personClass)),
            try quad(shape, Self.shProperty, property),
            try quad(property, Self.rdfType, .iri(validating: Self.shPropertyShape)),
            try quad(property, Self.shPath, .iri(validating: Self.nameProperty)),
            try quad(
                property,
                Self.shMinCount,
                .literal(
                    try RDFLiteral(
                        lexicalForm: "1",
                        datatype: Self.xsdInteger
                    )
                )
            ),
        ]
    }

    private func quad(
        _ subject: RDFTerm,
        _ predicate: String,
        _ object: RDFTerm
    ) throws -> RDFQuad {
        try RDFQuad(
            validatingSubject: subject,
            predicate: try RDFTerm.iri(validating: predicate),
            object: object
        )
    }

    private func invoke<Request: Sendable, Response: Sendable>(
        _ operation: DatabaseOperation<Request, Response>,
        request: Request,
        requestID: UInt64,
        runtime: DatabaseServerRuntime,
        metadata: OperationRequestMetadata = OperationRequestMetadata()
    ) async throws -> Response {
        let encoder = DatabaseWireEncoder()
        let responseBytes = try await runtime.execute(
            try encoder.encodeRequest(
                operation,
                requestID: requestID,
                target: .base(try TestBaseEnvironment.id()),
                metadata: metadata,
                request: request
            ),
            context: DatabaseRequestExecutionContext(
                authorization: TestBaseEnvironment.authorization
            )
        )
        switch try DatabaseWireDecoder().decodeResponse(
            operation,
            from: responseBytes,
            matching: requestID
        ) {
        case .success(let response):
            return response
        case .failure(let error):
            throw error
        }
    }

    private struct MaterializedPath {
        let nodes: [GraphAlgorithmOperation.Term]
        let edgeLabels: [GraphAlgorithmOperation.Term]
    }

    private struct MaterializedValidation {
        let conforms: Bool
        let issues: [ValidationReport.Issue]
    }

    private static let rdfType =
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    private static let personClass = "urn:semantic:Person"
    private static let nameProperty = "urn:semantic:name"
    private static let dataGraph = "urn:semantic:data"
    private static let shapesGraph = "urn:semantic:shapes"
    private static let shNamespace = "http://www.w3.org/ns/shacl#"
    private static let shNodeShape = shNamespace + "NodeShape"
    private static let shPropertyShape = shNamespace + "PropertyShape"
    private static let shTargetClass = shNamespace + "targetClass"
    private static let shProperty = shNamespace + "property"
    private static let shPath = shNamespace + "path"
    private static let shMinCount = shNamespace + "minCount"
    private static let xsdInteger = "http://www.w3.org/2001/XMLSchema#integer"
}

private actor SemanticParityJobScheduler: DatabaseJobScheduler {
    func ensureWakeUp(noLaterThan timestamp: Timestamp) async throws {
        _ = timestamp
    }
}

private enum SemanticParityTestError: Error {
    case unexpectedGraphResponse
    case unexpectedQueryResponse
    case unexpectedSHACLResponse
}
#endif
