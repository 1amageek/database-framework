import DatabaseKit
import TestSupport
import DatabaseRuntime
import DatabaseEngine
import DatabaseOperations
import DatabaseTypes
import DatabaseWire
import GraphIndex
import OntologyIndex
import StorageKit
import Testing

@Suite("Database SHACL validation processor", .serialized)
struct DatabaseSHACLValidationProcessorTests {
    @Test("canonical RDF shapes validate a named graph")
    func canonicalShapesValidateNamedGraph() async throws {
        let validationContext = try await makeSHACLValidationContext()
        try await insertMissingNamePeople(validationContext: validationContext)
        try await upsertShapes(try shapes(), key: "shapes-1", validationContext: validationContext)

        let response = try await validate(
            page: QueryExecuteOperation.Page(limit: 10),
            validationContext: validationContext
        )

        #expect(response.conforms == false)
        #expect(response.issues.count == 2)
        #expect(
            Set(response.issues.compactMap(\.focusNode)) == Set([
                try RDFTerm.iri(validating: "urn:Dave"),
                try RDFTerm.iri(validating: "urn:Eve"),
            ])
        )
        for issue in response.issues {
            #expect(issue.severity == ValidationReport.Severity.violation)
            #expect(issue.code == "sh:MinCountConstraintComponent")
            #expect(
                issue.path
                    == .predicate(try RDFPredicateIRI("urn:name"))
            )
            #expect(
                issue.sourceShape
                    == (try RDFTerm.blankNode(identifier: "name-property"))
            )
        }
    }

    @Test("pagination is bound to the active data snapshot")
    func paginationTracksSnapshotFingerprint() async throws {
        let validationContext = try await makeSHACLValidationContext()
        try await insertMissingNamePeople(validationContext: validationContext)
        try await upsertShapes(try shapes(), key: "shapes-1", validationContext: validationContext)

        let first = try await validate(
            page: QueryExecuteOperation.Page(limit: 1),
            validationContext: validationContext
        )
        guard let continuation = first.continuation else {
            Issue.record("Expected a validation continuation")
            return
        }
        let second = try await validate(
            page: QueryExecuteOperation.Page(
                limit: 1,
                continuation: continuation
            ),
            validationContext: validationContext
        )
        #expect(second.issues.count == 1)
        #expect(second.continuation == nil)
        #expect(first.issues[0].focusNode != second.issues[0].focusNode)

        await validationContext.resolver.updateSnapshotFingerprint([2])
        await #expect(throws: DatabaseSHACLValidationError.self) {
            try await validate(
                page: QueryExecuteOperation.Page(
                    limit: 1,
                    continuation: continuation
                ),
                validationContext: validationContext
            )
        }
    }

    @Test("an empty entity selection does not expand to shape targets")
    func emptyEntitySelectionRemainsEmpty() async throws {
        let validationContext = try await makeSHACLValidationContext()
        try await insertMissingNamePeople(validationContext: validationContext)
        try await upsertShapes(
            try shapes(),
            key: "shapes-empty",
            validationContext: validationContext
        )

        let response = try await validate(
            page: QueryExecuteOperation.Page(limit: 10),
            focus: .entities([]),
            validationContext: validationContext
        )

        #expect(response.conforms)
        #expect(response.issues.isEmpty)
        #expect(response.continuation == nil)
    }

    @Test("OWL entailment applies ontology hierarchy to data targets and class constraints")
    func owlEntailmentAppliesToStoredData() async throws {
        var ontology = OWLOntology(
            iri: "urn:test:shacl-ontology",
            classes: [
                OWLClass(iri: "urn:Person"),
                OWLClass(iri: "urn:Employee")
            ]
        )
        ontology.axioms = [
            .subClassOf(
                sub: .named("urn:Employee"),
                sup: .named("urn:Person")
            )
        ]
        let validationContext = try await makeSHACLValidationContext(
            entailmentContext: OWLGraphEntailment(
                reasoner: OWLReasoner(
                    ontology: ontology,
                    clock: TestProcessMonotonicClock()
                )
            )
        )
        try await insertEntailedPeople(
            validationContext: validationContext
        )
        try await upsertShapes(
            try entailedShapes(),
            key: "entailed-shapes",
            validationContext: validationContext
        )

        let response = try await validate(
            page: QueryExecuteOperation.Page(limit: 10),
            entailment: .owl(ontology: ontology.iri),
            validationContext: validationContext
        )

        #expect(response.conforms == false)
        #expect(response.issues.count == 1)
        #expect(
            response.issues.first?.focusNode
                == (try RDFTerm.iri(validating: "urn:Dave"))
        )
        #expect(
            response.issues.first?.code
                == "sh:MinCountConstraintComponent"
        )
    }

    @Test("RDFS entailment applies subclass, domain, and subproperty closure")
    func rdfsEntailmentAppliesToValidationQueries() async throws {
        let dataQuads = try rdfsData()
        let workBudget = SHACLValidationWorkBudget(
            budget: ExecutionBudget(maximumWorkUnits: 1_000),
            monotonicClock: TestProcessMonotonicClock()
        )
        let entailment = try RDFSGraphEntailment(
            quads: dataQuads,
            budget: workBudget
        )
        let validationContext = try await makeSHACLValidationContext(
            entailmentContext: entailment,
            ontologyContext: entailment.ontologyContext
        )
        try await insert(
            dataQuads,
            validationContext: validationContext
        )
        try await upsertShapes(
            try rdfsShapes(),
            key: "rdfs-shapes",
            validationContext: validationContext
        )

        let response = try await validate(
            page: QueryExecuteOperation.Page(limit: 10),
            entailment: .rdfs,
            validationContext: validationContext
        )

        #expect(response.conforms == false)
        #expect(response.issues.count == 2)
        #expect(
            Set(response.issues.compactMap { $0.focusNode })
                == [try RDFTerm.iri(validating: "urn:Dave")]
        )
        #expect(
            response.issues.allSatisfy {
                $0.code == "sh:MinCountConstraintComponent"
            }
        )
    }

    @Test("invalid shapes roll back the canonical document")
    func invalidShapesRollbackCanonicalDocument() async throws {
        let validationContext = try await makeSHACLValidationContext()
        let unsupported = try shapes() + [
            try quad(
                try RDFTerm.iri(validating: "urn:PersonShape"),
                Self.shNamespace + "sparql",
                try RDFTerm.blankNode(identifier: "constraint")
            )
        ]

        await #expect(throws: DatabaseSHACLValidationError.self) {
            try await upsertShapes(
                unsupported,
                key: "invalid-shapes",
                validationContext: validationContext
            )
        }
        await #expect(throws: DatabaseRDFDocumentStoreError.self) {
            try await validationContext.service.execute(
                SHACLExecuteOperation.Request(
                    invocation: .describeShapes(graph: Self.shapesGraph)
                ),
                context: context(container: validationContext.container)
            )
        }
    }

    private func makeSHACLValidationContext(
        entailmentContext: (any SHACLEntailmentContext)? = nil,
        ontologyContext: OntologyContext? = nil
    ) async throws -> SHACLValidationContext {
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try DatabaseSHACLStatement.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration.testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DatabaseSHACLStatement.self)]
            ),
            security: .testingDisabled
        )
        guard let descriptor = try DatabaseSHACLStatement.indexDescriptors.first(
            where: {
                $0.kindIdentifier
                    == IndexDefinition.rdfDataset.identifier
            }
        ) else {
            throw SHACLValidationSetupError.missingRDFDatasetIndex
        }
        let readableIndex = try await container.testBaseContext()
            .indexQueryContext.withReadableIndex(
                named: descriptor.name,
                kindIdentifier: descriptor.kind.identifier,
                for: DatabaseSHACLStatement.self
            ) { index, _ in
                index
            }
        guard let readableIndex else {
            throw SHACLValidationSetupError.missingRDFDatasetIndex
        }
        let data = SHACLExecuteOperation.DataSource(
            entity: DatabaseSHACLStatement.persistableType,
            index: descriptor.name,
            partitions: FieldObject(),
            graph: .named(try RDFTerm.iri(validating: Self.dataGraph))
        )
        let source = RDFDatasetSource(
            entityName: DatabaseSHACLStatement.persistableType,
            indexName: descriptor.name,
            indexSubspace: readableIndex.subspace,
            coverage: .dataset
        )
        let executor = SPARQLQueryExecutor(
            database: container.engine,
            monotonicClock: container.monotonicClock,
            wallClock: FixedTestWallClock(),
            sources: [source],
            ontologyContext: ontologyContext
        )
        let resolver = MutableSnapshotSHACLDataSourceResolver(
            executor: executor,
            dataGraph: .named(try RDFGraphName(iri: Self.dataGraph)),
            entailmentContext: entailmentContext,
            snapshotFingerprint: [1]
        )
        let store = try await DatabaseRDFDocumentStore(
            container: container,
            namespace: "shacl"
        )
        let processor = DatabaseSHACLValidationProcessor(
            documentStore: store,
            dataSourceResolver: resolver
        )
        let stateStore = DatabaseMutationStateStore(
            container: container
        )
        let service = CanonicalDatabaseSHACLService(
            store: store,
            processor: processor,
            coordinator: DatabaseTransactionalOperationCoordinator(
                stateStore: stateStore
            )
        )
        return SHACLValidationContext(
            container: container,
            resolver: resolver,
            service: service,
            data: data
        )
    }

    private func insertMissingNamePeople(validationContext: SHACLValidationContext) async throws {
        let context = validationContext.container.testBaseContext()
        for (index, person) in ["urn:Dave", "urn:Eve"].enumerated() {
            let statement = DatabaseSHACLStatement(
                id: "person-\(index)",
                subject: try .iri(validating: person),
                predicate: try .iri(validating: Self.rdfType),
                object: try .iri(validating: "urn:Person"),
                graph: try .iri(validating: Self.dataGraph)
            )
            try context.insert(statement)
        }
        try await context.save()
    }

    private func insertEntailedPeople(
        validationContext: SHACLValidationContext
    ) async throws {
        let statements = [
            ("dave-type", "urn:Dave", Self.rdfType, "urn:Employee"),
            ("alice-type", "urn:Alice", Self.rdfType, "urn:Person"),
            ("alice-name", "urn:Alice", "urn:name", "Alice"),
            ("alice-knows", "urn:Alice", "urn:knows", "urn:Bob"),
            ("bob-type", "urn:Bob", Self.rdfType, "urn:Employee"),
            ("bob-name", "urn:Bob", "urn:name", "Bob")
        ]
        let context = validationContext.container.testBaseContext()
        for (id, subject, predicate, object) in statements {
            let objectTerm: RDFTerm
            if predicate == "urn:name" {
                objectTerm = .string(object)
            } else {
                objectTerm = try .iri(validating: object)
            }
            try context.insert(
                DatabaseSHACLStatement(
                    id: id,
                    subject: try .iri(validating: subject),
                    predicate: try .iri(validating: predicate),
                    object: objectTerm,
                    graph: try .iri(validating: Self.dataGraph)
                )
            )
        }
        try await context.save()
    }

    private func insert(
        _ quads: [RDFQuad],
        validationContext: SHACLValidationContext
    ) async throws {
        let context = validationContext.container.testBaseContext()
        for (offset, quad) in quads.enumerated() {
            try context.insert(
                DatabaseSHACLStatement(
                    id: "rdfs-\(offset)",
                    subject: quad.subject.term,
                    predicate: quad.predicate.term,
                    object: quad.object,
                    graph: try .iri(validating: Self.dataGraph)
                )
            )
        }
        try await context.save()
    }

    private func upsertShapes(
        _ shapes: [RDFQuad],
        key: String,
        validationContext: SHACLValidationContext
    ) async throws {
        let request = SHACLExecuteOperation.Request(
            invocation: .upsertShapes(
                graph: Self.shapesGraph,
                shapes: shapes,
                expectedRevision: nil
            )
        )
        let baseContext = validationContext.container.testBaseContext()
        let requestPayload = try DatabaseWireEncoder().encodeRequestPayload(
            DatabaseOperationCatalog.shaclExecute,
            request: request
        )
        _ = try await baseContext.withDataOperation {
            try await validationContext.service.execute(
                request,
                context: context(
                    container: validationContext.container,
                    requestID: 1,
                    metadata: OperationRequestMetadata(idempotencyKey: key),
                    requestPayload: requestPayload
                )
            )
        }
    }

    private func validate(
        page: QueryExecuteOperation.Page,
        focus: SHACLExecuteOperation.Focus = .targets,
        entailment: SHACLExecuteOperation.Entailment = .none,
        validationContext: SHACLValidationContext
    ) async throws -> MaterializedValidationReport {
        let request = SHACLExecuteOperation.Request(
                invocation: .validate(
                    shapesGraph: Self.shapesGraph,
                    data: validationContext.data,
                    focus: focus,
                    entailment: entailment
                ),
                page: page
            )
        let response = try await validationContext.container.testBaseContext()
            .withDataOperation {
                try await validationContext.service.execute(
                    request,
                    context: context(container: validationContext.container)
                ).response
            }
        guard case .validation(let report) = response else {
            throw SHACLValidationSetupError.unexpectedResponse
        }
        return MaterializedValidationReport(
            conforms: report.conforms,
            issues: try report.materializedIssues(
                maximumCount: report.issueCount
            ),
            continuation: report.continuation
        )
    }

    private func shapes() throws -> [RDFQuad] {
        let shape = try RDFTerm.iri(validating: "urn:PersonShape")
        let property = try RDFTerm.blankNode(identifier: "name-property")
        return [
            try quad(
                shape,
                Self.rdfType,
                try RDFTerm.iri(validating: Self.shNodeShape)
            ),
            try quad(
                shape,
                Self.shTargetClass,
                try RDFTerm.iri(validating: "urn:Person")
            ),
            try quad(shape, Self.shProperty, property),
            try quad(
                property,
                Self.rdfType,
                try RDFTerm.iri(validating: Self.shPropertyShape)
            ),
            try quad(
                property,
                Self.shPath,
                try RDFTerm.iri(validating: "urn:name")
            ),
            try quad(
                property,
                Self.shMinCount,
                .literal(
                    try RDFLiteral(
                        lexicalForm: "1",
                        datatype: Self.xsdInteger
                    )
                )
            )
        ]
    }

    private func entailedShapes() throws -> [RDFQuad] {
        let aliceShape = try RDFTerm.iri(validating: "urn:AliceShape")
        let knowsProperty = try RDFTerm.blankNode(
            identifier: "knows-property"
        )
        return try shapes() + [
            try quad(
                aliceShape,
                Self.rdfType,
                try RDFTerm.iri(validating: Self.shNodeShape)
            ),
            try quad(
                aliceShape,
                Self.shTargetNode,
                try RDFTerm.iri(validating: "urn:Alice")
            ),
            try quad(aliceShape, Self.shProperty, knowsProperty),
            try quad(
                knowsProperty,
                Self.rdfType,
                try RDFTerm.iri(validating: Self.shPropertyShape)
            ),
            try quad(
                knowsProperty,
                Self.shPath,
                try RDFTerm.iri(validating: "urn:knows")
            ),
            try quad(
                knowsProperty,
                Self.shClass,
                try RDFTerm.iri(validating: "urn:Person")
            )
        ]
    }

    private func rdfsData() throws -> [RDFQuad] {
        [
            try quad(
                try RDFTerm.iri(validating: "urn:Employee"),
                Self.rdfsSubClassOf,
                try RDFTerm.iri(validating: "urn:Person")
            ),
            try quad(
                try RDFTerm.iri(validating: "urn:manages"),
                Self.rdfsSubPropertyOf,
                try RDFTerm.iri(validating: "urn:knows")
            ),
            try quad(
                try RDFTerm.iri(validating: "urn:worksFor"),
                Self.rdfsDomain,
                try RDFTerm.iri(validating: "urn:Employee")
            ),
            try quad(
                try RDFTerm.iri(validating: "urn:Dave"),
                "urn:worksFor",
                try RDFTerm.iri(validating: "urn:Acme")
            ),
            try quad(
                try RDFTerm.iri(validating: "urn:Dave"),
                "urn:manages",
                try RDFTerm.iri(validating: "urn:Bob")
            )
        ]
    }

    private func rdfsShapes() throws -> [RDFQuad] {
        let personShape = try RDFTerm.iri(validating: "urn:PersonShape")
        let personName = try RDFTerm.blankNode(identifier: "person-name")
        let subjectShape = try RDFTerm.iri(
            validating: "urn:KnowsSubjectShape"
        )
        let subjectName = try RDFTerm.blankNode(identifier: "subject-name")
        return [
            try quad(
                personShape,
                Self.rdfType,
                try RDFTerm.iri(validating: Self.shNodeShape)
            ),
            try quad(
                personShape,
                Self.shTargetClass,
                try RDFTerm.iri(validating: "urn:Person")
            ),
            try quad(personShape, Self.shProperty, personName),
            try quad(
                personName,
                Self.rdfType,
                try RDFTerm.iri(validating: Self.shPropertyShape)
            ),
            try quad(
                personName,
                Self.shPath,
                try RDFTerm.iri(validating: "urn:name")
            ),
            try quad(
                personName,
                Self.shMinCount,
                .literal(
                    try RDFLiteral(
                        lexicalForm: "1",
                        datatype: Self.xsdInteger
                    )
                )
            ),
            try quad(
                subjectShape,
                Self.rdfType,
                try RDFTerm.iri(validating: Self.shNodeShape)
            ),
            try quad(
                subjectShape,
                Self.shTargetSubjectsOf,
                try RDFTerm.iri(validating: "urn:knows")
            ),
            try quad(subjectShape, Self.shProperty, subjectName),
            try quad(
                subjectName,
                Self.rdfType,
                try RDFTerm.iri(validating: Self.shPropertyShape)
            ),
            try quad(
                subjectName,
                Self.shPath,
                try RDFTerm.iri(validating: "urn:name")
            ),
            try quad(
                subjectName,
                Self.shMinCount,
                .literal(
                    try RDFLiteral(
                        lexicalForm: "1",
                        datatype: Self.xsdInteger
                    )
                )
            )
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

    private func context(
        container: DBContainer,
        requestID: UInt64 = 2,
        metadata: OperationRequestMetadata = OperationRequestMetadata(),
        requestPayload: ByteString = []
    ) -> DatabaseOperationContext {
        let baseContext = container.testBaseContext()
#if MultipleBases
        return DatabaseOperationContext(
            container: container,
            target: .base(baseContext.baseID),
            baseContext: baseContext,
            composition: nil,
            requirement: .canonical(for: .shaclExecute),
            requestID: requestID,
            metadata: metadata,
            authorization: TestBaseEnvironment.authorization,
            requestPayload: requestPayload,
            wireLimits: .default
        )
#else
        _ = baseContext
        return DatabaseOperationContext(
            container: container,
            target: .database,
            requirement: .canonical(for: .shaclExecute),
            requestID: requestID,
            metadata: metadata,
            authorization: TestBaseEnvironment.authorization,
            requestPayload: requestPayload,
            wireLimits: .default
        )
#endif
    }

    private struct SHACLValidationContext: Sendable {
        let container: DBContainer
        let resolver: MutableSnapshotSHACLDataSourceResolver
        let service: CanonicalDatabaseSHACLService
        let data: SHACLExecuteOperation.DataSource
    }

    private struct MaterializedValidationReport {
        let conforms: Bool
        let issues: [ValidationReport.Issue]
        let continuation: ByteString?
    }

    private enum SHACLValidationSetupError: Error {
        case missingRDFDatasetIndex
        case unexpectedResponse
    }

    private static let dataGraph = "urn:data"
    private static let shapesGraph = "urn:calendar-shapes"
    private static let rdfNamespace =
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    private static let shNamespace = "http://www.w3.org/ns/shacl#"
    private static let rdfType = rdfNamespace + "type"
    private static let shNodeShape = shNamespace + "NodeShape"
    private static let shPropertyShape = shNamespace + "PropertyShape"
    private static let shTargetClass = shNamespace + "targetClass"
    private static let shTargetNode = shNamespace + "targetNode"
    private static let shTargetSubjectsOf =
        shNamespace + "targetSubjectsOf"
    private static let shProperty = shNamespace + "property"
    private static let shPath = shNamespace + "path"
    private static let shClass = shNamespace + "class"
    private static let shMinCount = shNamespace + "minCount"
    private static let xsdInteger =
        "http://www.w3.org/2001/XMLSchema#integer"
    private static let rdfsSubClassOf =
        "http://www.w3.org/2000/01/rdf-schema#subClassOf"
    private static let rdfsSubPropertyOf =
        "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"
    private static let rdfsDomain =
        "http://www.w3.org/2000/01/rdf-schema#domain"
}
