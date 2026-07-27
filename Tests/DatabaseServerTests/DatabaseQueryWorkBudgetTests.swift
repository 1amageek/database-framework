import DatabaseKit
import DatabaseEngine
import DatabaseRuntime
import DatabaseServer
import DatabaseTypes
import DatabaseWire
import DatabaseKit
import StorageKit
import Testing

@Suite("Canonical query work budget", .serialized)
struct DatabaseQueryWorkBudgetTests {
    @Test("Direct QueryIR uses the configured structural limits")
    func directQueryIRUsesConfiguredStructuralLimits() async throws {
        let container = try await makeContainer()
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef("id"))),
                ProjectionItem(.column(ColumnRef("title"))),
            ]),
            source: .table(TableRef(DatabaseEndpointEntity.persistableType))
        )
        let request = QueryExecuteOperation.Request(
            input: .ir(.select(query)),
            page: QueryExecuteOperation.Page(limit: 1)
        )
        let handler = QueryExecuteHandler(
            runtimeLimits: try DatabaseRuntimeLimits(
                maximumRows: 10_000,
                maximumWorkUnits: 1_000_000,
                maximumTimeoutMilliseconds: 30_000,
                queryStructuralLimits: QueryStructuralLimits(
                    maximumCollectionElements: 1
                )
            )
        )

        do {
            _ = try await handler.handle(
                request,
                context: try operationContext(container: container, request: request)
            )
            Issue.record("Expected the QueryIR collection limit to reject the query")
        } catch QueryParameterBindingError.invalidStructure(let error) {
            #expect(
                error == .resourceLimitExceeded(
                    resource: .collectionElements,
                    actual: 2,
                    maximum: 1
                )
            )
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("Parameter payloads are rejected before recursive binding")
    func parameterPayloadIsValidatedBeforeBinding() async throws {
        let container = try await makeContainer()
        var value = FieldValue.object(FieldObject())
        for _ in 0..<7 {
            value = .array([value])
        }
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.parameter(.position(1)), alias: "value"),
            ]),
            source: .table(TableRef(DatabaseEndpointEntity.persistableType))
        )
        let request = QueryExecuteOperation.Request(
            input: .ir(.select(query)),
            parameters: [
                QueryParameter(
                    position: 1,
                    name: "value",
                    value: value
                ),
            ],
            page: QueryExecuteOperation.Page(limit: 1)
        )
        let handler = QueryExecuteHandler(
            runtimeLimits: try DatabaseRuntimeLimits(
                maximumRows: 10_000,
                maximumWorkUnits: 1_000_000,
                maximumTimeoutMilliseconds: 30_000,
                queryStructuralLimits: QueryStructuralLimits(
                    maximumNestingDepth: 6
                )
            )
        )

        do {
            _ = try await handler.handle(
                request,
                context: try operationContext(container: container, request: request)
            )
            Issue.record("Expected parameter preflight to reject recursive binding")
        } catch QueryParameterBindingError.invalidStructure(let error) {
            #expect(
                error == .resourceLimitExceeded(
                    resource: .nestingDepth,
                    actual: 7,
                    maximum: 6
                )
            )
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("SQL text parsing uses the configured structural limits")
    func sqlTextUsesConfiguredStructuralLimits() async throws {
        let container = try await makeContainer()
        let request = QueryExecuteOperation.Request(
            input: .text(
                language: .sql,
                statement: "SELECT id, title FROM DatabaseEndpointEntity"
            ),
            page: QueryExecuteOperation.Page(limit: 1)
        )
        let handler = QueryExecuteHandler(
            runtimeLimits: try DatabaseRuntimeLimits(
                maximumRows: 10_000,
                maximumWorkUnits: 1_000_000,
                maximumTimeoutMilliseconds: 30_000,
                queryStructuralLimits: QueryStructuralLimits(
                    maximumCollectionElements: 1
                )
            )
        )

        do {
            _ = try await handler.handle(
                request,
                context: try operationContext(container: container, request: request)
            )
            Issue.record("Expected the parser collection limit to reject the query")
        } catch let error as QueryStructuralValidationError {
            #expect(
                error == .resourceLimitExceeded(
                    resource: .collectionElements,
                    actual: 2,
                    maximum: 1
                )
            )
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("Configured structural limits reach the SPARQL plan compiler")
    func configuredStructuralLimitsReachSPARQLCompiler() async throws {
        let container = try await makeContainer()
        var expression = Expression.literal(.bool(true))
        for _ in 0..<62 {
            expression = .not(expression)
        }
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(expression, alias: "value"),
            ]),
            source: .graphPattern(.basic([]))
        )
        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .nestingDepth,
                actual: 65,
                maximum: 64
            )
        ) {
            try QueryStructuralValidator.validate(query)
        }
        let structuralLimits = QueryStructuralLimits(maximumNestingDepth: 65)
        try QueryStructuralValidator.validate(
            query,
            limits: structuralLimits
        )
        let request = QueryExecuteOperation.Request(
            input: .ir(.select(query)),
            page: QueryExecuteOperation.Page(limit: 1)
        )
        let response = try await QueryExecuteHandler(
            runtimeLimits: try DatabaseRuntimeLimits(
                maximumRows: 10_000,
                maximumWorkUnits: 1_000_000,
                maximumTimeoutMilliseconds: 30_000,
                queryStructuralLimits: structuralLimits
            )
        ).handle(
            request,
            context: DatabaseOperationContext(
                container: container,
                requestID: 1,
                metadata: OperationRequestMetadata(),
                requestPayload: []
            )
        )

        guard case .rows(let page) = response else {
            Issue.record("Expected a row page")
            return
        }
        #expect(page.rowCount == 1)
    }

    @Test("Direct QueryIR cannot hide an RDF blank node inside Literal")
    func directQueryIRRejectsNonCanonicalBlankNode() async throws {
        let container = try await makeContainer()
        let query = SelectQuery(
            projection: .all,
            source: .graphPattern(
                .basic([
                    TriplePattern(
                        subject: .variable("subject"),
                        predicate: .iri("urn:predicate"),
                        object: .literal(
                            .rdfTerm(
                                try RDFTerm.blankNode(identifier: "hidden")
                            )
                        )
                    )
                ])
            )
        )
        let request = QueryExecuteOperation.Request(
            input: .ir(.select(query)),
            page: QueryExecuteOperation.Page(limit: 1)
        )

        do {
            _ = try await QueryExecuteHandler().handle(
                request,
                context: DatabaseOperationContext(
                    container: container,
                    requestID: 1,
                    metadata: OperationRequestMetadata(),
                    requestPayload: try DatabaseWireEncoder()
                        .encodeRequestPayload(
                            DatabaseOperations.queryExecute,
                            request: request
                        )
                )
            )
            Issue.record("Expected semantic validation to reject the query")
        } catch let error as SPARQLSemanticValidationError {
            #expect(error == .nonCanonicalTermLiteral)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("table scans fail with a typed work limit before returning a partial page")
    func tableScanIsBounded() async throws {
        let container = try await makeContainer()

        do {
            _ = try await execute(
                container: container,
                pageLimit: 2,
                budget: ExecutionBudget(
                    maximumRows: 2,
                    maximumWorkUnits: 1,
                    timeoutMilliseconds: 1_000
                )
            )
            Issue.record("Expected the table scan to exhaust its work budget")
        } catch is DatabaseWorkLimitError {
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("table queries return only the requested page under one shared meter")
    func tablePageUsesSharedMeter() async throws {
        let container = try await makeContainer()
        let response = try await execute(
            container: container,
            pageLimit: 2,
            budget: ExecutionBudget(
                maximumRows: 2,
                maximumWorkUnits: 1_000,
                timeoutMilliseconds: 1_000
            )
        )

        guard case .rows(let page) = response else {
            Issue.record("Expected a row page")
            return
        }
        #expect(page.rowCount == 2)
        #expect(page.continuation != nil)
    }

    private func makeContainer() async throws -> DBContainer {
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try DatabaseEndpointEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(
                backend: .custom(InMemoryEngine())
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [DatabaseEndpointEntity.self]
            ),
            security: .disabled
        )
        let context = container.newContext()
        for index in 0..<3 {
            var entity = DatabaseEndpointEntity()
            entity.id = "entity-\(index)"
            entity.title = "Title \(index)"
            entity.priority = Int64(index)
            try context.insert(entity)
        }
        try await context.save()
        return container
    }

    private func execute(
        container: DBContainer,
        pageLimit: UInt32,
        budget: ExecutionBudget
    ) async throws -> QueryExecuteOperation.Response {
        let request = QueryExecuteOperation.Request(
            input: .ir(
                .select(
                    SelectQuery(
                        projection: .all,
                        source: .table(
                            TableRef(
                                DatabaseEndpointEntity.persistableType
                            )
                        )
                    )
                )
            ),
            page: QueryExecuteOperation.Page(limit: pageLimit),
            budget: budget
        )
        return try await QueryExecuteHandler().handle(
            request,
            context: try operationContext(container: container, request: request)
        )
    }

    private func operationContext(
        container: DBContainer,
        request: QueryExecuteOperation.Request
    ) throws -> DatabaseOperationContext {
        DatabaseOperationContext(
            container: container,
            requestID: 1,
            metadata: OperationRequestMetadata(),
            requestPayload: try DatabaseWireEncoder().encodeRequestPayload(
                DatabaseOperations.queryExecute,
                request: request
            )
        )
    }
}
