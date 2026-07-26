import Core
import DatabaseRuntime
import DatabaseEngine
import DatabaseServer
import DatabaseValue
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
        #expect(
            page.entries.contains(
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
        #expect(
            inference.inferredAxioms.contains(
                try DatabaseRDFQuad(
                    subject: .iri("urn:Alice"),
                    predicate: .iri(Self.rdfType),
                    object: .iri("urn:Person"),
                    graph: .iri("urn:calendar")
                )
            )
        )
    }

    @Test("reasoning preserves typed RDF literal identity")
    func reasoningPreservesLiteralIdentity() async throws {
        let reasoningContext = try await makeOntologyReasoningContext()
        let literal = DatabaseRDFLiteral(
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
                    try DatabaseRDFQuad(
                        subject: .iri("urn:event:1"),
                        predicate: .iri("urn:title"),
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
        #expect(
            inference.inferredAxioms.contains(
                try DatabaseRDFQuad(
                    subject: .iri("urn:event:1"),
                    predicate: .iri("urn:label"),
                    object: .literal(literal),
                    graph: .iri("urn:calendar")
                )
            )
        )
        #expect(
            !inference.inferredAxioms.contains(
                try DatabaseRDFQuad(
                    subject: .iri("urn:event:1"),
                    predicate: .iri("urn:label"),
                    object: .iri("urn:value"),
                    graph: .iri("urn:calendar")
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
                    try DatabaseRDFQuad(
                        subject: .iri("urn:event:1"),
                        predicate: .iri(Self.rdfType),
                        object: .literal(DatabaseRDFLiteral(
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
        #expect(
            refreshedPage.entries.contains(
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
            configuration: DBConfiguration(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [DatabaseEndpointEntity.self]
            ),
            security: .disabled
        )
        let documentStore = try await DatabaseRDFDocumentStore(
            container: container,
            namespace: "ontology"
        )
        let ontologySubspace = try await container.engine.withTransaction {
            transaction in
            try await container.engine.directoryService.createOrOpen(
                path: ["database-framework", "ontology-index"],
                transaction: transaction
            )
        }
        let processor = DatabaseOntologyReasoningProcessor(
            documentStore: documentStore,
            ontologyStore: OntologyStore(
                subspace: OntologySubspace(base: ontologySubspace)
            )
        )
        let stateStore = try await DatabaseMutationStateStore(
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
        try await reasoningContext.service.execute(
            request,
            context: DatabaseOperationContext(
                container: reasoningContext.container,
                requestID: 1,
                metadata: DatabaseRequestMetadata(idempotencyKey: key),
                requestPayload: try DatabaseEnvelopeCodec.encode(request)
            )
        ).response
    }

    private func ontologyDeclaration(_ ontology: String) throws -> DatabaseRDFQuad {
        try type(ontology, class: Self.owlOntology)
    }

    private func classDeclaration(_ value: String) throws -> DatabaseRDFQuad {
        try type(value, class: Self.owlClass)
    }

    private func individualDeclaration(_ value: String) throws -> DatabaseRDFQuad {
        try type(value, class: Self.owlNamedIndividual)
    }

    private func dataPropertyDeclaration(_ value: String) throws -> DatabaseRDFQuad {
        try type(value, class: Self.owlDatatypeProperty)
    }

    private func type(_ value: String, class classIRI: String) throws -> DatabaseRDFQuad {
        try DatabaseRDFQuad(
            subject: .iri(value),
            predicate: .iri(Self.rdfType),
            object: .iri(classIRI)
        )
    }

    private func subclass(_ value: String, of parent: String) throws -> DatabaseRDFQuad {
        try DatabaseRDFQuad(
            subject: .iri(value),
            predicate: .iri(Self.rdfsSubClassOf),
            object: .iri(parent)
        )
    }

    private func subproperty(_ value: String, of parent: String) throws -> DatabaseRDFQuad {
        try DatabaseRDFQuad(
            subject: .iri(value),
            predicate: .iri(Self.rdfsSubPropertyOf),
            object: .iri(parent)
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
