import Core
import DatabaseRuntime
import DatabaseEngine
import DatabaseServer
import DatabaseValue
import DatabaseWire
import Graph
import GraphIndex
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
        #expect(Set(response.issues.compactMap(\.focusNode)) == [
            .iri("urn:Dave"),
            .iri("urn:Eve")
        ])
        for issue in response.issues {
            #expect(issue.severity == .violation)
            #expect(issue.code == "sh:MinCountConstraintComponent")
            #expect(issue.path == .predicate("urn:name"))
            #expect(issue.sourceShape == .blankNode("name-property"))
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

    @Test("an empty record selection does not expand to shape targets")
    func emptyRecordSelectionRemainsEmpty() async throws {
        let validationContext = try await makeSHACLValidationContext()
        try await insertMissingNamePeople(validationContext: validationContext)
        try await upsertShapes(
            try shapes(),
            key: "shapes-empty",
            validationContext: validationContext
        )

        let response = try await validate(
            page: QueryExecuteOperation.Page(limit: 10),
            focus: .records([]),
            validationContext: validationContext
        )

        #expect(response.conforms)
        #expect(response.issues.isEmpty)
        #expect(response.continuation == nil)
    }

    @Test("invalid shapes roll back the canonical document")
    func invalidShapesRollbackCanonicalDocument() async throws {
        let validationContext = try await makeSHACLValidationContext()
        let unsupported = try shapes() + [
            try quad(
                .iri("urn:PersonShape"),
                Self.shNamespace + "sparql",
                .blankNode("constraint")
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

    private func makeSHACLValidationContext() async throws -> SHACLValidationContext {
        let container = try await DBContainer.open(
            for: Schema(
                [DatabaseSHACLStatement.self],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
        guard let descriptor = DatabaseSHACLStatement.indexDescriptors.first(
            where: {
                $0.kindIdentifier ==
                    RDFQuadIndexKind<DatabaseSHACLStatement>.identifier
            }
        ) else {
            throw SHACLValidationSetupError.missingRDFDatasetIndex
        }
        let typeSubspace = try await container.newContext()
            .indexQueryContext.indexSubspace(
                for: DatabaseSHACLStatement.self
            )
        let data = SHACLExecuteOperation.DataSource(
            entity: DatabaseSHACLStatement.persistableType,
            index: descriptor.name,
            partitions: [],
            graph: .named(.iri(Self.dataGraph))
        )
        let source = RDFDatasetSource(
            entityName: DatabaseSHACLStatement.persistableType,
            indexName: descriptor.name,
            indexSubspace: typeSubspace.subspace(descriptor.name),
            coverage: .dataset
        )
        let executor = SPARQLQueryExecutor(
            database: container.engine,
            sources: [source]
        )
        let resolver = MutableSnapshotSHACLDataSourceResolver(
            executor: executor,
            graphScope: .named(try RDFGraphName(iri: Self.dataGraph)),
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
        let stateStore = try await DatabaseMutationStateStore(
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
        let context = validationContext.container.newContext()
        for (index, person) in ["urn:Dave", "urn:Eve"].enumerated() {
            var statement = DatabaseSHACLStatement()
            statement.id = "person-\(index)"
            statement.subject = .iri(person)
            statement.predicate = .iri(Self.rdfType)
            statement.object = .iri("urn:Person")
            statement.graph = .iri(Self.dataGraph)
            try context.insert(statement)
        }
        try await context.save()
    }

    private func upsertShapes(
        _ shapes: [DatabaseRDFQuad],
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
        _ = try await validationContext.service.execute(
            request,
            context: DatabaseOperationContext(
                container: validationContext.container,
                requestID: 1,
                metadata: DatabaseRequestMetadata(idempotencyKey: key),
                requestPayload: try DatabaseEnvelopeCodec.encode(request)
            )
        )
    }

    private func validate(
        page: QueryExecuteOperation.Page,
        focus: SHACLExecuteOperation.Focus = .targets,
        validationContext: SHACLValidationContext
    ) async throws -> DatabaseValidationReport {
        let response = try await validationContext.service.execute(
            SHACLExecuteOperation.Request(
                invocation: .validate(
                    shapesGraph: Self.shapesGraph,
                    data: validationContext.data,
                    focus: focus,
                    entailment: .none
                ),
                page: page
            ),
            context: context(container: validationContext.container)
        ).response
        guard case .validation(let report) = response else {
            throw SHACLValidationSetupError.unexpectedResponse
        }
        return report
    }

    private func shapes() throws -> [DatabaseRDFQuad] {
        let shape = DatabaseRDFTerm.iri("urn:PersonShape")
        let property = DatabaseRDFTerm.blankNode("name-property")
        return [
            try quad(shape, Self.rdfType, .iri(Self.shNodeShape)),
            try quad(shape, Self.shTargetClass, .iri("urn:Person")),
            try quad(shape, Self.shProperty, property),
            try quad(property, Self.rdfType, .iri(Self.shPropertyShape)),
            try quad(property, Self.shPath, .iri("urn:name")),
            try quad(
                property,
                Self.shMinCount,
                .literal(
                    try DatabaseRDFLiteral(
                        lexicalForm: "1",
                        datatype: Self.xsdInteger
                    )
                )
            )
        ]
    }

    private func quad(
        _ subject: DatabaseRDFTerm,
        _ predicate: String,
        _ object: DatabaseRDFTerm
    ) throws -> DatabaseRDFQuad {
        try DatabaseRDFQuad(
            subject: subject,
            predicate: .iri(predicate),
            object: object
        )
    }

    private func context(container: DBContainer) -> DatabaseOperationContext {
        DatabaseOperationContext(
            container: container,
            requestID: 2,
            metadata: DatabaseRequestMetadata(),
            requestPayload: []
        )
    }

    private struct SHACLValidationContext: Sendable {
        let container: DBContainer
        let resolver: MutableSnapshotSHACLDataSourceResolver
        let service: CanonicalDatabaseSHACLService
        let data: SHACLExecuteOperation.DataSource
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
    private static let shProperty = shNamespace + "property"
    private static let shPath = shNamespace + "path"
    private static let shMinCount = shNamespace + "minCount"
    private static let xsdInteger =
        "http://www.w3.org/2001/XMLSchema#integer"
}
