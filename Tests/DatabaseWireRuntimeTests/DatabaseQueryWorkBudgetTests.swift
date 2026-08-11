import DatabaseKit
import TestSupport
import DatabaseEngine
import DatabaseFoundation
import DatabaseRuntime
import DatabaseWireRuntime
import DatabaseTypes
import DatabaseWire
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
            _ = try await handle(request, using: handler, container: container)
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
            _ = try await handle(request, using: handler, container: container)
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
            _ = try await handle(request, using: handler, container: container)
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
        let handler = QueryExecuteHandler(
            runtimeLimits: try DatabaseRuntimeLimits(
                maximumRows: 10_000,
                maximumWorkUnits: 1_000_000,
                maximumTimeoutMilliseconds: 30_000,
                queryStructuralLimits: structuralLimits
            )
        )
        let response = try await handle(
            request,
            using: handler,
            container: container
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
            _ = try await handle(
                request,
                using: QueryExecuteHandler(),
                container: container
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

    @Test("stable table paging scans only the visible page and one lookahead row")
    func stableTablePageDoesNotRescanTheWholeTable() async throws {
        let container = try await makeContainer(seedCount: 100)
        let context = container.testBaseContext()
        let query = SelectQuery(
            projection: .all,
            source: .table(
                TableRef(DatabaseEndpointEntity.persistableType)
            )
        )
        let budget = ExecutionBudget(
            maximumRows: 2,
            // Includes canonical query fingerprinting and row projection,
            // but remains below the 100 storage rows the previous
            // continuation path re-scanned for every page.
            maximumWorkUnits: 80,
            timeoutMilliseconds: 1_000
        )
        let pages = try await context.executeCanonicalRead { transaction in
            let first = try await context.query(
                query,
                execution: ReadExecutionContext(
                    options: ReadExecutionOptions(
                        pageSize: 2,
                        budget: budget,
                        continuationSnapshotIsStable: true
                    ),
                    monotonicClock: container.monotonicClock
                ),
                transaction: transaction
            )
            let continuation = try #require(first.continuation)
            let second = try await context.query(
                query,
                execution: ReadExecutionContext(
                    options: ReadExecutionOptions(
                        pageSize: 2,
                        continuation: continuation,
                        budget: budget,
                        continuationSnapshotIsStable: true
                    ),
                    monotonicClock: container.monotonicClock
                ),
                transaction: transaction
            )
            return (first, second)
        }

        #expect(pages.0.rows.count == 2)
        #expect(pages.0.continuation != nil)
        #expect(pages.1.rows.count == 2)
        #expect(pages.1.continuation != nil)
    }

    @Test("Qualified equality joins use bounded hash candidates")
    func qualifiedEqualityJoinUsesBoundedHashCandidates() async throws {
        let container = try await makeContainer(seedCount: 100)
        let entity = DatabaseEndpointEntity.persistableType
        let query = SelectQuery(
            projection: .all,
            source: .join(
                JoinClause(
                    type: .inner,
                    left: .table(
                        TableRef(table: entity, alias: "left_entity")
                    ),
                    right: .table(
                        TableRef(table: entity, alias: "right_entity")
                    ),
                    condition: .on(
                        .equal(
                            .column(
                                ColumnRef(
                                    table: "left_entity",
                                    column: "id"
                                )
                            ),
                            .column(
                                ColumnRef(
                                    table: "right_entity",
                                    column: "id"
                                )
                            )
                        )
                    )
                )
            )
        )
        let request = QueryExecuteOperation.Request(
            input: .ir(.select(query)),
            page: QueryExecuteOperation.Page(limit: 100),
            budget: ExecutionBudget(
                maximumRows: 100,
                // A 100-by-100 nested-loop join requires at least 10,000
                // candidate checks. Keep this below that threshold while
                // allowing source decoding, hash construction, projection,
                // and final page materialization to share the same meter.
                maximumWorkUnits: 5_000,
                timeoutMilliseconds: 1_000
            )
        )

        let response = try await handle(
            request,
            using: QueryExecuteHandler(),
            container: container
        )

        guard case .rows(let page) = response else {
            Issue.record("Expected a row page")
            return
        }
        #expect(page.rowCount == 100)
        #expect(page.continuation == nil)
    }

    @Test("Outer page size does not truncate a nested relational source")
    func outerPageSizeDoesNotTruncateNestedSource() async throws {
        let container = try await makeContainer(seedCount: 100)
        let nested = SelectQuery(
            projection: .all,
            source: .table(
                TableRef(DatabaseEndpointEntity.persistableType)
            )
        )
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(
                    .aggregate(.count(nil, distinct: false)),
                    alias: "count"
                )
            ]),
            source: .subquery(nested, alias: "entities")
        )
        let response = try await container.testBaseContext().query(
            query,
            options: ReadExecutionOptions(
                pageSize: 1,
                budget: ExecutionBudget(
                    maximumRows: 1,
                    maximumWorkUnits: 5_000,
                    timeoutMilliseconds: 1_000
                )
            )
        )

        let row = try #require(response.rows.first)
        #expect(response.rows.count == 1)
        #expect(row.fields["count"] == .int64(100))
    }

    private func makeContainer(seedCount: Int = 3) async throws -> DBContainer {
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try DatabaseEndpointEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration.testing(
                storageEngine: InMemoryEngine()
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DatabaseEndpointEntity.self)]
            ),
            security: .testingDisabled
        )
        let context = container.testBaseContext()
        for index in 0..<seedCount {
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
        let baseContext = container.testBaseContext()
        return try await baseContext.withDataOperation {
            let snapshotStore = DatabaseQuerySnapshotStore(
                container: container,
                clock: AnyDatabaseWallClock(RealtimeDatabaseWallClock()),
                identifierGenerator: AnyDatabaseUUIDGenerator(
                    RandomDatabaseUUIDGenerator()
                ),
                scheduler: AnyDatabaseJobScheduler(
                    WorkBudgetSnapshotScheduler()
                ),
                wireLimits: .default
            )
            return try await QueryExecuteHandler(
                runtimeLimits: .default,
                querySnapshotStore: snapshotStore
            ).handle(
                request,
                context: try operationContext(
                    container: container,
                    request: request,
                    baseContext: baseContext
                )
            )
        }
    }

    private func handle(
        _ request: QueryExecuteOperation.Request,
        using handler: QueryExecuteHandler,
        container: DBContainer
    ) async throws -> QueryExecuteOperation.Response {
        let baseContext = container.testBaseContext()
        return try await baseContext.withDataOperation {
            try await handler.handle(
                request,
                context: try operationContext(
                    container: container,
                    request: request,
                    baseContext: baseContext
                )
            )
        }
    }

    private func operationContext(
        container: DBContainer,
        request: QueryExecuteOperation.Request,
        baseContext: DatabaseContext? = nil
    ) throws -> DatabaseOperationContext {
        let baseContext = baseContext ?? container.testBaseContext()
#if MultipleBases
        return DatabaseOperationContext(
            container: container,
            target: .base(baseContext.baseID),
            baseContext: baseContext,
            composition: nil,
            requirement: .canonical(for: .queryExecute),
            requestID: 1,
            metadata: OperationRequestMetadata(),
            authorization: TestBaseEnvironment.authorization,
            requestPayload: [],
            wireLimits: .default
        )
#else
        _ = baseContext
        return DatabaseOperationContext(
            container: container,
            target: .database,
            requirement: .canonical(for: .queryExecute),
            requestID: 1,
            metadata: OperationRequestMetadata(),
            authorization: TestBaseEnvironment.authorization,
            requestPayload: [],
            wireLimits: .default
        )
#endif
    }
}

private actor WorkBudgetSnapshotScheduler: DatabaseJobScheduler {
    func ensureWakeUp(noLaterThan deadline: Timestamp) async throws {
        _ = deadline
    }
}
