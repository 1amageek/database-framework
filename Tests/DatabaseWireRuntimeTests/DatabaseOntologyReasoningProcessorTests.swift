import DatabaseKit
import TestSupport
import DatabaseRuntime
import DatabaseEngine
import DatabaseWireRuntime
import DatabaseFoundation
import DatabaseTypes
import DatabaseWire
import OntologyIndex
import StorageKit
import Testing

@Suite("Database ontology reasoning processor", .serialized)
struct DatabaseOntologyReasoningProcessorTests {
    @Test("imports are indexed atomically and reasoning uses the closure")
    func importsAndReasoningUseClosure() async throws {
        let reasoningContext = try await makeOntologyReasoningContext()
        try await upsert(
            document: OntologyExecuteOperation.Document(
                ontology: "urn:base",
                axioms: [
                    try ontologyDeclaration("urn:base"),
                    try classDeclaration("urn:Person")
                ]
            ),
            key: "base-1",
            reasoningContext: reasoningContext
        )
        try await upsert(
            document: OntologyExecuteOperation.Document(
                ontology: "urn:calendar",
                imports: ["urn:base"],
                axioms: [
                    try ontologyDeclaration("urn:calendar"),
                    try classDeclaration("urn:Employee"),
                    try subclass("urn:Employee", of: "urn:Person"),
                    try individualDeclaration("urn:Alice"),
                    try type("urn:Alice", class: "urn:Employee")
                ]
            ),
            key: "calendar-1",
            reasoningContext: reasoningContext
        )

        let hierarchy = try await execute(
            OntologyExecuteOperation.Request(
                invocation: .hierarchy(
                    ontology: "urn:calendar",
                    resource: "urn:Employee",
                    resourceKind: .class,
                    direction: .ancestors,
                    maximumDepth: 4
                )
            ),
            key: nil,
            reasoningContext: reasoningContext
        )
        guard case .hierarchy(let page) = hierarchy else {
            Issue.record("Expected a hierarchy response")
            return
        }
        let entries = try page.materializedEntries(maximumCount: 16)
        #expect(
            entries.contains(
                OntologyExecuteOperation.HierarchyEntry(
                    resource: "urn:Person",
                    depth: 1
                )
            )
        )

        let reasoning = try await execute(
            OntologyExecuteOperation.Request(
                invocation: .reason(
                    ontology: "urn:calendar",
                    profile: .rdfs
                )
            ),
            key: nil,
            reasoningContext: reasoningContext
        )
        guard case .inference(let inference) = reasoning else {
            Issue.record("Expected an inference response")
            return
        }
        let inferredAxioms = try inference.materializedInferredAxioms(
            maximumCount: 1_024
        )
        #expect(
            inferredAxioms.contains(
                try rdfQuad(
                    subject: "urn:Alice",
                    predicate: Self.rdfType,
                    object: .iri(validating: "urn:Person"),
                    graph: "urn:calendar"
                )
            )
        )
    }

    @Test("reasoning preserves typed RDF literal identity")
    func reasoningPreservesLiteralIdentity() async throws {
        let reasoningContext = try await makeOntologyReasoningContext()
        let literal = RDFLiteral(
            lexicalForm: "urn:value",
            datatype: .xsdString
        )
        try await upsert(
            document: OntologyExecuteOperation.Document(
                ontology: "urn:calendar",
                axioms: [
                    try ontologyDeclaration("urn:calendar"),
                    try dataPropertyDeclaration("urn:title"),
                    try dataPropertyDeclaration("urn:label"),
                    try subproperty("urn:title", of: "urn:label"),
                    try individualDeclaration("urn:event:1"),
                    try rdfQuad(
                        subject: "urn:event:1",
                        predicate: "urn:title",
                        object: .literal(literal)
                    )
                ]
            ),
            key: "literal-1",
            reasoningContext: reasoningContext
        )

        let response = try await execute(
            OntologyExecuteOperation.Request(
                invocation: .reason(
                    ontology: "urn:calendar",
                    profile: .rdfs
                )
            ),
            key: nil,
            reasoningContext: reasoningContext
        )
        guard case .inference(let inference) = response else {
            Issue.record("Expected an inference response")
            return
        }
        let inferredAxioms = try inference.materializedInferredAxioms(
            maximumCount: 1_024
        )
        #expect(
            inferredAxioms.contains(
                try rdfQuad(
                    subject: "urn:event:1",
                    predicate: "urn:label",
                    object: .literal(literal),
                    graph: "urn:calendar"
                )
            )
        )
        #expect(
            !inferredAxioms.contains(
                try rdfQuad(
                    subject: "urn:event:1",
                    predicate: "urn:label",
                    object: .iri(validating: "urn:value"),
                    graph: "urn:calendar"
                )
            )
        )
    }

    @Test("invalid rule operands fail with a typed materialization error")
    func invalidRuleOperandFails() async throws {
        let reasoningContext = try await makeOntologyReasoningContext()
        try await upsert(
            document: OntologyExecuteOperation.Document(
                ontology: "urn:calendar",
                axioms: [
                    try ontologyDeclaration("urn:calendar"),
                    try individualDeclaration("urn:event:1"),
                    try rdfQuad(
                        subject: "urn:event:1",
                        predicate: Self.rdfType,
                        object: .literal(RDFLiteral(
                            lexicalForm: "urn:Event",
                            datatype: .xsdString
                        ))
                    )
                ]
            ),
            key: "invalid-rule-1",
            reasoningContext: reasoningContext
        )

        do {
            _ = try await execute(
                OntologyExecuteOperation.Request(
                    invocation: .reason(
                        ontology: "urn:calendar",
                        profile: .rdfs
                    )
                ),
                key: nil,
                reasoningContext: reasoningContext
            )
            Issue.record("Expected typed materialization failure")
        } catch let error as DatabaseOntologyProcessingError {
            #expect(
                error == .materialization(.expectedIRI(
                    rule: .caxSco,
                    position: .object,
                    actual: .literal
                ))
            )
        }
    }

    @Test("import updates rebuild dependents and invalidate continuations")
    func importUpdateRebuildsDependents() async throws {
        let reasoningContext = try await makeOntologyReasoningContext()
        try await upsert(
            document: OntologyExecuteOperation.Document(
                ontology: "urn:base",
                axioms: [
                    try ontologyDeclaration("urn:base"),
                    try classDeclaration("urn:Agent"),
                    try classDeclaration("urn:Person"),
                    try subclass("urn:Person", of: "urn:Agent")
                ]
            ),
            key: "base-1",
            reasoningContext: reasoningContext
        )
        try await upsert(
            document: OntologyExecuteOperation.Document(
                ontology: "urn:calendar",
                imports: ["urn:base"],
                axioms: [
                    try ontologyDeclaration("urn:calendar"),
                    try classDeclaration("urn:Employee"),
                    try subclass("urn:Employee", of: "urn:Person")
                ]
            ),
            key: "calendar-1",
            reasoningContext: reasoningContext
        )
        let first = try await execute(
            OntologyExecuteOperation.Request(
                invocation: .hierarchy(
                    ontology: "urn:calendar",
                    resource: "urn:Employee",
                    resourceKind: .class,
                    direction: .ancestors,
                    maximumDepth: 8
                ),
                page: QueryExecuteOperation.Page(limit: 1)
            ),
            key: nil,
            reasoningContext: reasoningContext
        )
        guard case .hierarchy(let firstPage) = first,
              let continuation = firstPage.continuation else {
            Issue.record("Expected a paged hierarchy response")
            return
        }

        try await upsert(
            document: OntologyExecuteOperation.Document(
                ontology: "urn:base",
                axioms: [
                    try ontologyDeclaration("urn:base"),
                    try classDeclaration("urn:Entity"),
                    try classDeclaration("urn:Agent"),
                    try classDeclaration("urn:Person"),
                    try subclass("urn:Person", of: "urn:Agent"),
                    try subclass("urn:Agent", of: "urn:Entity")
                ]
            ),
            expectedRevision: 1,
            key: "base-2",
            reasoningContext: reasoningContext
        )

        await #expect(throws: DatabaseOntologyProcessingError.self) {
            try await execute(
                OntologyExecuteOperation.Request(
                    invocation: .hierarchy(
                        ontology: "urn:calendar",
                        resource: "urn:Employee",
                        resourceKind: .class,
                        direction: .ancestors,
                        maximumDepth: 8
                    ),
                    page: QueryExecuteOperation.Page(
                        limit: 1,
                        continuation: continuation
                    )
                ),
                key: nil,
                reasoningContext: reasoningContext
            )
        }

        let refreshed = try await execute(
            OntologyExecuteOperation.Request(
                invocation: .hierarchy(
                    ontology: "urn:calendar",
                    resource: "urn:Employee",
                    resourceKind: .class,
                    direction: .ancestors,
                    maximumDepth: 8
                )
            ),
            key: nil,
            reasoningContext: reasoningContext
        )
        guard case .hierarchy(let refreshedPage) = refreshed else {
            Issue.record("Expected a refreshed hierarchy response")
            return
        }
        let refreshedEntries = try refreshedPage.materializedEntries(
            maximumCount: 16
        )
        #expect(
            refreshedEntries.contains(
                OntologyExecuteOperation.HierarchyEntry(
                    resource: "urn:Entity",
                    depth: 3
                )
            )
        )
    }

    @Test("import cycles and deletion of imported ontologies are rejected")
    func importIntegrityIsEnforced() async throws {
        let reasoningContext = try await makeOntologyReasoningContext()
        try await upsert(
            document: OntologyExecuteOperation.Document(
                ontology: "urn:base",
                axioms: [try ontologyDeclaration("urn:base")]
            ),
            key: "base-1",
            reasoningContext: reasoningContext
        )
        try await upsert(
            document: OntologyExecuteOperation.Document(
                ontology: "urn:calendar",
                imports: ["urn:base"],
                axioms: [try ontologyDeclaration("urn:calendar")]
            ),
            key: "calendar-1",
            reasoningContext: reasoningContext
        )

        await #expect(throws: DatabaseOntologyProcessingError.self) {
            try await upsert(
                document: OntologyExecuteOperation.Document(
                    ontology: "urn:base",
                    imports: ["urn:calendar"],
                    axioms: [try ontologyDeclaration("urn:base")]
                ),
                expectedRevision: 1,
                key: "base-cycle",
                reasoningContext: reasoningContext
            )
        }
        await #expect(throws: DatabaseOntologyProcessingError.self) {
            try await execute(
                OntologyExecuteOperation.Request(
                    invocation: .delete(
                        ontology: "urn:base",
                        expectedRevision: 1
                    )
                ),
                key: "base-delete",
                reasoningContext: reasoningContext
            )
        }
    }

    private func makeOntologyReasoningContext() async throws -> OntologyReasoningContext {
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [
                    try DatabaseEndpointEntity.schemaEntity,
                ],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration.testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DatabaseEndpointEntity.self)]
            ),
            security: .testingDisabled
        )
        let documentStore = try await DatabaseRDFDocumentStore(
            container: container,
            namespace: "ontology"
        )
        let processor = DatabaseOntologyReasoningProcessor(
            documentStore: documentStore,
            container: container,
            clock: AnyDatabaseWallClock(RealtimeDatabaseWallClock()),
            monotonicClock: TestProcessMonotonicClock()
        )
        let stateStore = DatabaseMutationStateStore(
            container: container
        )
        let service = CanonicalDatabaseOntologyService(
            store: documentStore,
            processor: processor,
            coordinator: DatabaseTransactionalOperationCoordinator(
                stateStore: stateStore
            )
        )
        return OntologyReasoningContext(container: container, service: service)
    }

    private func upsert(
        document: OntologyExecuteOperation.Document,
        expectedRevision: UInt64? = nil,
        key: String,
        reasoningContext: OntologyReasoningContext
    ) async throws {
        _ = try await execute(
            OntologyExecuteOperation.Request(
                invocation: .upsert(
                    document: document,
                    expectedRevision: expectedRevision
                )
            ),
            key: key,
            reasoningContext: reasoningContext
        )
    }

    private func execute(
        _ request: OntologyExecuteOperation.Request,
        key: String?,
        reasoningContext: OntologyReasoningContext
    ) async throws -> OntologyExecuteOperation.Response {
#if MultipleBases
        let acceptedTargets: DatabaseOperationTargetKinds = .base
#else
        let acceptedTargets: DatabaseOperationTargetKinds = .database
#endif
        let requirement = DatabaseOperationRequirement(
            acceptedTargets: acceptedTargets,
            access: .write,
            transaction: .write
        )
        return try await reasoningContext.container.testBaseContext()
            .withDataOperation {
                try await reasoningContext.service.execute(
                    request,
                    context: .testDataRoot(
                        container: reasoningContext.container,
                        requirement: requirement,
                        requestID: 1,
                        metadata: OperationRequestMetadata(idempotencyKey: key),
                        authorization: TestBaseEnvironment.authorization,
                        requestPayload: try DatabaseWireEncoder().encodeRequestPayload(
                            DatabaseOperations.ontologyExecute,
                            request: request
                        ),
                        dataContext: reasoningContext.container.testBaseContext(),
                        wireLimits: .default
                    )
                )
                .response
            }
    }

    private func ontologyDeclaration(_ ontology: String) throws -> RDFQuad {
        try type(ontology, class: Self.owlOntology)
    }

    private func classDeclaration(_ value: String) throws -> RDFQuad {
        try type(value, class: Self.owlClass)
    }

    private func individualDeclaration(_ value: String) throws -> RDFQuad {
        try type(value, class: Self.owlNamedIndividual)
    }

    private func dataPropertyDeclaration(_ value: String) throws -> RDFQuad {
        try type(value, class: Self.owlDatatypeProperty)
    }

    private func type(_ value: String, class classIRI: String) throws -> RDFQuad {
        try rdfQuad(
            subject: value,
            predicate: Self.rdfType,
            object: .iri(validating: classIRI)
        )
    }

    private func subclass(_ value: String, of parent: String) throws -> RDFQuad {
        try rdfQuad(
            subject: value,
            predicate: Self.rdfsSubClassOf,
            object: .iri(validating: parent)
        )
    }

    private func subproperty(_ value: String, of parent: String) throws -> RDFQuad {
        try rdfQuad(
            subject: value,
            predicate: Self.rdfsSubPropertyOf,
            object: .iri(validating: parent)
        )
    }

    private func rdfQuad(
        subject: String,
        predicate: String,
        object: RDFTerm,
        graph: String? = nil
    ) throws -> RDFQuad {
        RDFQuad(
            subject: .iri(try RDFIRI(subject)),
            predicate: try RDFPredicateIRI(predicate),
            object: object,
            graph: try graph.map(RDFGraphName.init(iri:))
        )
    }

    private struct OntologyReasoningContext: Sendable {
        let container: DBContainer
        let service: CanonicalDatabaseOntologyService
    }

    private static let rdfType =
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    private static let rdfsSubClassOf =
        "http://www.w3.org/2000/01/rdf-schema#subClassOf"
    private static let rdfsSubPropertyOf =
        "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"
    private static let owlOntology =
        "http://www.w3.org/2002/07/owl#Ontology"
    private static let owlClass =
        "http://www.w3.org/2002/07/owl#Class"
    private static let owlNamedIndividual =
        "http://www.w3.org/2002/07/owl#NamedIndividual"
    private static let owlDatatypeProperty =
        "http://www.w3.org/2002/07/owl#DatatypeProperty"
}
