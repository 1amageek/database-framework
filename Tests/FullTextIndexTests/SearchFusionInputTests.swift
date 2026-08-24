import DatabaseKit
import StorageKit
import TestSupport
import Testing

@testable import DatabaseEngine
@testable import FullTextIndex

@Persistable
private struct SearchFusionInputItem {
    #Index(
        .text(
            name: "search_fusion_body",
            fields: [\SearchFusionInputItem.body],
            mode: .fullText(tokenizer: .simple)
        )
    )

    var id: String
    var body: String
}

@Persistable
private struct SearchFusionInputWithoutPositionsItem {
    #Index(
        .text(
            name: "search_fusion_without_positions_body",
            fields: [\SearchFusionInputWithoutPositionsItem.body],
            mode: .fullText(tokenizer: .simple, storePositions: false)
        )
    )

    var id: String
    var body: String
}

@Persistable
private struct SearchFusionIntegerIDItem {
    #Index(
        .text(
            name: "search_fusion_integer_body",
            fields: [\SearchFusionIntegerIDItem.body],
            mode: .fullText(tokenizer: .simple)
        )
    )

    var id: Int64
    var body: String
}

@Suite("Full-text Fusion input")
struct SearchFusionInputTests {
    @Test("Search requires an exact single-field physical index")
    func requiresExactFieldMatch() {
        let input = Search(SearchFusionInputItem.fields.body)
            .terms(["swift"])
            .fusionInput

        guard case .index(let source) = input.operation else {
            Issue.record("Search must lower to an index operation")
            return
        }
        #expect(source.selection == .matching(
            type: .text(.fullText),
            fields: [SearchFusionInputItem.fields.body.identity],
            fieldMatch: .exact
        ))
    }

    @Test("Search lowers parameters score direction and limit to QueryIR")
    func lowersToCanonicalInput() {
        let input = Search(SearchFusionInputItem.fields.body)
            .index(named: "article_body")
            .terms(["swift", "database"], mode: .any)
            .bm25(k1: 1.4, b: 0.6)
            .limit(12)
            .fusionInput

        #expect(input.scoring == .annotation(
            name: "score",
            order: .higherIsBetter
        ))
        #expect(input.limit == 12)
        guard case .index(let source) = input.operation else {
            Issue.record("Search must lower to an index operation")
            return
        }
        #expect(source.selection == .named(
            name: "article_body",
            type: .text(.fullText)
        ))
        #expect(source.parameters[FullTextReadParameter.fieldName]
            == .string("body"))
        #expect(source.parameters[FullTextReadParameter.terms] == .array([
            .string("swift"),
            .string("database"),
        ]))
        #expect(source.parameters[FullTextReadParameter.matchMode]
            == .string("any"))
        #expect(source.parameters[FullTextReadParameter.bm25K1]
            == .float64(Double(Float(1.4))))
        #expect(source.parameters[FullTextReadParameter.bm25B]
            == .float64(Double(Float(0.6))))
    }

    @Test("Full-text Fusion parameters distinguish omission from invalid values")
    func rejectsMalformedOptionalAndUnknownParameters() throws {
        let entity = try SearchFusionInputItem.schemaEntity
        let descriptor = try #require(entity.indexes.first)
        let input = Search(SearchFusionInputItem.fields.body)
            .terms(["swift"])
            .fusionInput
        guard case .index(let source) = input.operation else {
            Issue.record("Search must lower to an index operation")
            return
        }
        let executor = FullTextFusionIndexReadExecutor()

        var omittedParameters = source.parameters
        omittedParameters.removeValue(forKey: FullTextReadParameter.bm25K1)
        omittedParameters.removeValue(forKey: FullTextReadParameter.bm25B)
        try executor.validate(
            validationRequest(
                source: FusionIndexSource(
                    selection: source.selection,
                    referencedFields: source.referencedFields,
                    parameters: omittedParameters
                ),
                scoring: input.scoring,
                descriptor: descriptor
            )
        )

        for (name, value) in invalidParameterValues() {
            var invalidParameters = source.parameters
            invalidParameters[name] = value
            #expect {
                try executor.validate(
                    validationRequest(
                        source: FusionIndexSource(
                            selection: source.selection,
                            referencedFields: source.referencedFields,
                            parameters: invalidParameters
                        ),
                        scoring: input.scoring,
                        descriptor: descriptor
                    )
                )
            } throws: { error in
                error as? FusionExecutionError == .invalidIndexInput(
                    indexType: .text(.fullText),
                    parameter: name
                )
            }
        }

        let withoutPositionsEntity = try SearchFusionInputWithoutPositionsItem
            .schemaEntity
        let withoutPositionsDescriptor = try #require(
            withoutPositionsEntity.indexes.first
        )
        let phraseInput = Search(
            SearchFusionInputWithoutPositionsItem.fields.body
        )
            .terms(["swift database"], mode: .phrase)
            .fusionInput
        guard case .index(let phraseSource) = phraseInput.operation else {
            Issue.record("Phrase search must lower to an index operation")
            return
        }
        #expect {
            try executor.validate(
                validationRequest(
                    source: phraseSource,
                    scoring: phraseInput.scoring,
                    descriptor: withoutPositionsDescriptor
                )
            )
        } throws: { error in
            error as? FusionExecutionError == .invalidIndexInput(
                indexType: .text(.fullText),
                parameter: FullTextReadParameter.matchMode
            )
        }
    }

    @Test("Malformed full-text Fusion parameters fail before storage admission")
    func malformedParametersFailBeforeStorageAdmission() async throws {
        let entity = try SearchFusionInputItem.schemaEntity
        let provider = FullTextIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            SearchFusionInputItem.self
        )
        try FullTextReadExecutors.register(with: &entityRuntime)
        try entityRuntime.register(provider)
        let executor = FullTextFusionIndexReadExecutor()
        let engine = TransactionCountingInMemoryEngine()
        let container = try await DBContainer.open(
            for: try Schema(entities: [entity]),
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fulltext-fusion-preflight-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider),
                ],
                fusionIndexReadExecutors: [executor],
                entityRuntimes: [entityRuntime.registration()]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let baselineTransactions = engine.transactionCount
        let input = Search(SearchFusionInputItem.fields.body)
            .terms(["swift"])
            .fusionInput
        guard case .index(let source) = input.operation else {
            Issue.record("Search must lower to an index operation")
            return
        }

        for (name, value) in invalidParameterValues() {
            var parameters = source.parameters
            parameters[name] = value
            let invalidSource = FusionIndexSource(
                selection: source.selection,
                referencedFields: source.referencedFields,
                parameters: parameters
            )
            let query = SelectQuery(
                projection: .all,
                source: .table(TableRef(SearchFusionInputItem.persistableType)),
                accessPath: .fusion(
                    FusionSource(stages: [
                        FusionStageSource(inputs: [
                            FusionInput(
                                operation: .index(invalidSource),
                                scoring: input.scoring
                            ),
                        ]),
                    ])
                )
            )
            await #expect {
                _ = try await container.testBaseContext().query(query)
            } throws: { error in
                error as? FusionExecutionError == .invalidIndexInput(
                    indexType: .text(.fullText),
                    parameter: name
                )
            }
            #expect(engine.transactionCount == baselineTransactions)
        }
    }

    @Test("Successful multi-stage Fusion uses one storage transaction")
    func successfulMultiStageFusionUsesOneTransaction() async throws {
        let entity = try SearchFusionInputItem.schemaEntity
        let provider = FullTextIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            SearchFusionInputItem.self
        )
        try FullTextReadExecutors.register(with: &entityRuntime)
        try entityRuntime.register(provider)
        let engine = TransactionCountingInMemoryEngine()
        let container = try await DBContainer.open(
            for: try Schema(entities: [entity]),
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fulltext-fusion-transaction-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider),
                ],
                fusionIndexReadExecutors: [
                    FullTextFusionIndexReadExecutor(),
                ],
                entityRuntimes: [entityRuntime.registration()]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        for item in [
            SearchFusionInputItem(id: "both", body: "swift database"),
            SearchFusionInputItem(id: "swift", body: "swift only"),
        ] {
            try context.insert(item)
        }
        try await context.save()
        let baselineTransactions = engine.transactionCount

        let response = try await context.execute(
            FusionQuery<SearchFusionInputItem> {
                Search(SearchFusionInputItem.fields.body).terms(["swift"])
                Search(SearchFusionInputItem.fields.body).terms(["database"])
            }
        )

        #expect(response.results.map(\.item.id) == ["both"])
        #expect(engine.transactionCount == baselineTransactions + 1)
    }

    @Test("Observed corrupt postings fail before non-match filtering")
    func corruptNonMatchingPostingFailsExplicitly() async throws {
        let entity = try SearchFusionInputItem.schemaEntity
        let provider = FullTextIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            SearchFusionInputItem.self
        )
        try FullTextReadExecutors.register(with: &entityRuntime)
        try entityRuntime.register(provider)
        let engine = InMemoryEngine()
        let container = try await DBContainer.open(
            for: try Schema(entities: [entity]),
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fulltext-fusion-corruption-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider),
                ],
                fusionIndexReadExecutors: [
                    FullTextFusionIndexReadExecutor(),
                ],
                entityRuntimes: [entityRuntime.registration()]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(
            SearchFusionInputItem(id: "corrupt", body: "foo bar")
        )
        try await context.save()

        try await container.withTestBaseTransaction { transaction in
            let readable = try #require(
                try await context.indexQueryContext.readableIndex(
                    named: "search_fusion_body",
                    indexType: .text(.fullText),
                    for: SearchFusionInputItem.self,
                    transaction: transaction
                )
            )
            let postingKey = FullTextStorageLayout.terms(
                in: readable.subspace
            ).subspace("bar").pack(Tuple("corrupt"))
            try transaction.setValue(
                Tuple(Int64(1), Int64(100)).pack(),
                for: postingKey
            )
        }

        let unrestricted = FusionQuery<SearchFusionInputItem> {
            Search(SearchFusionInputItem.fields.body)
                .terms(["foo bar"], mode: .phrase)
        }
        await expectCorruptedFullTextIndex {
            _ = try await context.execute(unrestricted)
        }

        let restricted = FusionQuery<SearchFusionInputItem> {
            Search(SearchFusionInputItem.fields.body).terms(["foo"])
            Search(SearchFusionInputItem.fields.body)
                .terms(["foo bar"], mode: .phrase)
        }
        await expectCorruptedFullTextIndex {
            _ = try await context.execute(restricted)
        }
    }

    @Test("Zero document-frequency tombstones preserve search semantics")
    func zeroDocumentFrequencyTombstoneIsAnOrdinaryMissingTerm() async throws {
        let entity = try SearchFusionInputItem.schemaEntity
        let provider = FullTextIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            SearchFusionInputItem.self
        )
        try FullTextReadExecutors.register(with: &entityRuntime)
        try entityRuntime.register(provider)
        let container = try await DBContainer.open(
            for: try Schema(entities: [entity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fulltext-fusion-zero-df-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider),
                ],
                fusionIndexReadExecutors: [
                    FullTextFusionIndexReadExecutor(),
                ],
                entityRuntimes: [entityRuntime.registration()]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let removed = SearchFusionInputItem(id: "removed", body: "foo")
        try context.insert(removed)
        try context.insert(SearchFusionInputItem(id: "kept", body: "bar"))
        try await context.save()
        try context.delete(removed)
        try await context.save()

        let any = try await context.execute(
            FusionQuery<SearchFusionInputItem> {
                Search(SearchFusionInputItem.fields.body)
                    .terms(["foo", "bar"], mode: .any)
            }
        )
        #expect(any.results.map(\.item.id) == ["kept"])

        let all = try await context.execute(
            FusionQuery<SearchFusionInputItem> {
                Search(SearchFusionInputItem.fields.body)
                    .terms(["foo", "bar"], mode: .all)
            }
        )
        #expect(all.results.isEmpty)
    }

    @Test("Empty indexed documents form a valid zero-length corpus")
    func emptyIndexedDocumentIsNotCorruptCorpusStatistics() async throws {
        let entity = try SearchFusionInputItem.schemaEntity
        let provider = FullTextIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            SearchFusionInputItem.self
        )
        try FullTextReadExecutors.register(with: &entityRuntime)
        try entityRuntime.register(provider)
        let container = try await DBContainer.open(
            for: try Schema(entities: [entity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fulltext-fusion-empty-corpus-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider),
                ],
                fusionIndexReadExecutors: [
                    FullTextFusionIndexReadExecutor(),
                ],
                entityRuntimes: [entityRuntime.registration()]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(SearchFusionInputItem(id: "empty", body: ""))
        try await context.save()

        let response = try await context.execute(
            FusionQuery<SearchFusionInputItem> {
                Search(SearchFusionInputItem.fields.body).terms(["missing"])
            }
        )

        #expect(response.results.isEmpty)
    }

    @Test("Non-canonical posting identity fails before semantic filtering")
    func nonCanonicalPostingIdentityCannotBecomeEmptySuccess() async throws {
        let entity = try SearchFusionIntegerIDItem.schemaEntity
        let provider = FullTextIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            SearchFusionIntegerIDItem.self
        )
        try FullTextReadExecutors.register(with: &entityRuntime)
        try entityRuntime.register(provider)
        let engine = InMemoryEngine()
        let container = try await DBContainer.open(
            for: try Schema(entities: [entity]),
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fulltext-fusion-noncanonical-id-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider),
                ],
                fusionIndexReadExecutors: [
                    FullTextFusionIndexReadExecutor(),
                ],
                entityRuntimes: [entityRuntime.registration()]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(SearchFusionIntegerIDItem(id: 1, body: "foo"))
        try await context.save()

        try await container.withTestBaseTransaction { transaction in
            let readable = try #require(
                try await context.indexQueryContext.readableIndex(
                    named: "search_fusion_integer_body",
                    indexType: .text(.fullText),
                    for: SearchFusionIntegerIDItem.self,
                    transaction: transaction
                )
            )
            let term = FullTextStorageLayout.terms(
                in: readable.subspace
            ).subspace("foo")
            let canonicalKey = term.pack(Tuple(Int64(1)))
            let posting = try #require(
                try await transaction.getValue(for: canonicalKey)
            )
            try transaction.clear(key: canonicalKey)
            let nonCanonicalIdentifier = ByteString([0x16, 0x00, 0x01])
            try transaction.setValue(
                posting,
                for: term.prefix.appending(
                    contentsOf: nonCanonicalIdentifier
                )
            )
        }

        await expectCorruptedFullTextIndex {
            _ = try await context.execute(
                FusionQuery<SearchFusionIntegerIDItem> {
                    Search(SearchFusionIntegerIDItem.fields.body)
                        .terms(["foo", "missing"], mode: .all)
                }
            )
        }
    }

    @Test("Top-K replacement admits the new key before releasing the old key")
    func topKReplacementAccountsForOwnershipOverlap() throws {
        let oldKey = ByteString(repeating: 0x41, count: 64)
        let newKey = ByteString(repeating: 0x42, count: 512)
        let baselineBytes: UInt64
        do {
            let meter = topKMeter(maximumIntermediateBytes: 4_096)
            do {
                var topK = try FullTextFusionTopK(
                    limit: 1,
                    workMeter: meter
                )
                try topK.consider(primaryKey: oldKey, score: 1)
                baselineBytes = meter.retainedIntermediateBytes
            }
            #expect(meter.retainedIntermediateBytes == 0)
        }

        let insufficientMeter = topKMeter(
            maximumIntermediateBytes: baselineBytes
                + UInt64(newKey.count) - 1
        )
        do {
            var topK = try FullTextFusionTopK(
                limit: 1,
                workMeter: insufficientMeter
            )
            try topK.consider(primaryKey: oldKey, score: 1)
            #expect {
                try topK.consider(primaryKey: newKey, score: 2)
            } throws: { error in
                error is DatabaseWorkLimitError
            }
            #expect(
                insufficientMeter.retainedIntermediateBytes == baselineBytes
            )
        }
        #expect(insufficientMeter.retainedIntermediateBytes == 0)

        let exactMeter = topKMeter(
            maximumIntermediateBytes: baselineBytes + UInt64(newKey.count)
        )
        do {
            var topK = try FullTextFusionTopK(
                limit: 1,
                workMeter: exactMeter
            )
            try topK.consider(primaryKey: oldKey, score: 1)
            try topK.consider(primaryKey: newKey, score: 2)
            #expect(
                exactMeter.peakIntermediateBytes
                    == baselineBytes + UInt64(newKey.count)
            )
            #expect(
                exactMeter.retainedIntermediateBytes
                    == baselineBytes - UInt64(oldKey.count)
                        + UInt64(newKey.count)
            )
        }
        #expect(exactMeter.retainedIntermediateBytes == 0)
    }

    private func validationRequest(
        source: FusionIndexSource,
        scoring: FusionScoring?,
        descriptor: IndexDescriptor
    ) -> FusionIndexValidationRequest {
        FusionIndexValidationRequest(
            source: source,
            scoring: scoring,
            descriptor: descriptor
        )
    }

    private func invalidParameterValues() -> [(String, FieldValue)] {
        [
            (FullTextReadParameter.bm25K1, .string("invalid")),
            (FullTextReadParameter.bm25K1, .float64(.nan)),
            (
                FullTextReadParameter.bm25K1,
                .float64(Double.greatestFiniteMagnitude)
            ),
            (FullTextReadParameter.bm25B, .null),
            (FullTextReadParameter.bm25B, .float64(1.1)),
            ("unknown", .bool(true)),
        ]
    }

    private func topKMeter(
        maximumIntermediateBytes: UInt64
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 10,
                maximumWorkUnits: 10_000,
                maximumIntermediateRows: 10,
                maximumIntermediateBytes: maximumIntermediateBytes,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    private func expectCorruptedFullTextIndex(
        _ operation: () async throws -> Void
    ) async {
        do {
            try await operation()
            Issue.record("Corrupt full-text storage must not become a miss")
        } catch {
            #expect(
                error as? FusionExecutionError
                    == .corruptedIndex(.text(.fullText))
            )
        }
    }
}
