import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing

@testable import DatabaseEngine
@testable import GraphIndex

@Persistable
private struct ConnectedFusionResultItem {
    var id: String
    var vertexID: String?
    var eligible: Bool
}

@Persistable
private struct ConnectedFusionEdge {
    #Index(
        .graph(
            name: "property_graph",
            definition: .property(
                source: \ConnectedFusionEdge.source,
                label: .field(\ConnectedFusionEdge.label),
                target: \ConnectedFusionEdge.target,
                graph: nil,
                strategy: .adjacency
            )
        )
    )

    var id: String
    var source: String
    var label: String
    var target: String
}

@Suite("Connected Fusion input")
struct ConnectedFusionInputTests {
    @Test("Connected preserves cross-entity traversal semantics in QueryIR")
    func lowersToCanonicalInput() throws {
        let partitions = try FieldObject([
            (key: "tenant", value: .string("a")),
        ])
        let input = Connected(
            ConnectedFusionResultItem.fields.vertexID,
            from: "origin",
            through: ConnectedFusionEdge.self,
            indexNamed: "property_graph",
            partitions: partitions
        )
        .via("follows")
        .direction(.both)
        .hops(3)
        .limit(9)
        .fusionInput

        #expect(input.scoring == .annotation(
            name: "hops",
            order: .lowerIsBetter
        ))
        #expect(input.limit == 9)
        guard case .connected(let source) = input.operation else {
            Issue.record("Connected must lower to a connected operation")
            return
        }
        #expect(source.edgeEntity == ConnectedFusionEdge.persistableType)
        #expect(source.edgePartitions == partitions)
        #expect(source.selection == .named(
            name: "property_graph",
            type: .graph(.property)
        ))
        #expect(source.resultField.name == "vertexID")
        #expect(source.origin == "origin")
        #expect(source.edgeLabel == "follows")
        #expect(source.direction == .both)
        #expect(source.maximumHops == 3)
    }

    @Test("Connected executes shortest-hop traversal in every direction")
    func executesShortestHopTraversal() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try await insertFixtures(into: context)

        let outgoing = try await context.execute(
            FusionQuery<ConnectedFusionResultItem> {
                Connected(
                    ConnectedFusionResultItem.fields.vertexID,
                    from: "alice",
                    through: ConnectedFusionEdge.self,
                    indexNamed: "property_graph"
                )
                .via("follows")
                .hops(2)
            }
        )
        #expect(outgoing.results.map(\.item.id) == ["bob", "carol", "dave"])

        let incoming = try await context.execute(
            FusionQuery<ConnectedFusionResultItem> {
                Connected(
                    ConnectedFusionResultItem.fields.vertexID,
                    to: "alice",
                    through: ConnectedFusionEdge.self,
                    indexNamed: "property_graph"
                )
                .via("follows")
            }
        )
        #expect(incoming.results.map(\.item.id) == ["eve"])

        let both = try await context.execute(
            FusionQuery<ConnectedFusionResultItem> {
                Connected(
                    ConnectedFusionResultItem.fields.vertexID,
                    from: "alice",
                    through: ConnectedFusionEdge.self,
                    indexNamed: "property_graph"
                )
                .via("follows")
                .direction(.both)
            }
        )
        #expect(both.results.map(\.item.id) == ["bob", "carol", "eve"])
    }

    @Test("Connected restricts result candidates before applying its limit")
    func restrictsCandidatesBeforeLimit() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try await insertFixtures(into: context)

        let eligible = try Filter(
            ConnectedFusionResultItem.fields.eligible,
            equals: true
        )
        let result = try await context.execute(
            FusionQuery<ConnectedFusionResultItem> {
                eligible
                Connected(
                    ConnectedFusionResultItem.fields.vertexID,
                    from: "alice",
                    through: ConnectedFusionEdge.self,
                    indexNamed: "property_graph"
                )
                .via("follows")
                .hops(3)
                .limit(1)
            }
        )

        #expect(result.results.map(\.item.id) == ["carol"])
    }

    @Test("Connected resolves every equal-hop tie before applying its limit")
    func resolvesEqualHopTiesBeforeLimit() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try await insertFixtures(into: context)
        try context.insert(
            ConnectedFusionResultItem(
                id: "z-bob",
                vertexID: "bob",
                eligible: true
            )
        )
        try await context.save()

        let result = try await context.execute(
            FusionQuery<ConnectedFusionResultItem> {
                Connected(
                    ConnectedFusionResultItem.fields.vertexID,
                    from: "alice",
                    through: ConnectedFusionEdge.self,
                    indexNamed: "property_graph"
                )
                .hops(1)
                .limit(2)
            }
        )

        #expect(result.results.map(\.item.id) == ["bob", "carol"])
    }

    @Test("Connected rejects a non-string result field before physical I/O")
    func rejectsInvalidResultField() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let invalid = FusionConnectedSource(
            edgeEntity: ConnectedFusionEdge.persistableType,
            selection: .named(
                name: "property_graph",
                type: .graph(.property)
            ),
            resultField: ConnectedFusionResultItem.fields.eligible.identity,
            origin: "alice"
        )

        await #expect {
            _ = try await context.execute(
                FusionQuery<ConnectedFusionResultItem> {
                    RawConnectedFusionInput(invalid)
                }
            )
        } throws: { error in
            error as? FusionExecutionError == .invalidIndexInput(
                indexType: .graph(.property),
                parameter: "resultField"
            )
        }
    }

    @Test("Malformed property-graph keys fail as index corruption")
    func malformedGraphKeyFailsAsCorruption() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try await insertFixtures(into: context)

        try await container.withTestBaseTransaction { transaction in
            let readable = try #require(
                try await context.indexQueryContext.readableIndex(
                    named: "property_graph",
                    indexType: .graph(.property),
                    for: ConnectedFusionEdge.self,
                    transaction: transaction
                )
            )
            try transaction.setValue(
                ByteString(),
                for: readable.subspace.subspace(Int64(0)).pack(
                    Tuple("alice", "follows")
                )
            )
        }

        await #expect {
            _ = try await context.execute(
                FusionQuery<ConnectedFusionResultItem> {
                    Connected(
                        ConnectedFusionResultItem.fields.vertexID,
                        from: "alice",
                        through: ConnectedFusionEdge.self,
                        indexNamed: "property_graph"
                    )
                    .via("follows")
                }
            )
        } throws: { error in
            error as? FusionExecutionError == .corruptedIndex(
                .graph(.property)
            )
        }
    }

    private func insertFixtures(
        into context: DatabaseContext
    ) async throws {
        for item in [
            ConnectedFusionResultItem(
                id: "bob",
                vertexID: "bob",
                eligible: false
            ),
            ConnectedFusionResultItem(
                id: "carol",
                vertexID: "carol",
                eligible: true
            ),
            ConnectedFusionResultItem(
                id: "dave",
                vertexID: "dave",
                eligible: true
            ),
            ConnectedFusionResultItem(
                id: "eve",
                vertexID: "eve",
                eligible: true
            ),
            ConnectedFusionResultItem(
                id: "missing",
                vertexID: nil,
                eligible: true
            ),
        ] {
            try context.insert(item)
        }
        for edge in [
            ConnectedFusionEdge(
                id: "alice-bob",
                source: "alice",
                label: "follows",
                target: "bob"
            ),
            ConnectedFusionEdge(
                id: "alice-carol",
                source: "alice",
                label: "follows",
                target: "carol"
            ),
            ConnectedFusionEdge(
                id: "bob-carol",
                source: "bob",
                label: "follows",
                target: "carol"
            ),
            ConnectedFusionEdge(
                id: "carol-dave",
                source: "carol",
                label: "follows",
                target: "dave"
            ),
            ConnectedFusionEdge(
                id: "eve-alice",
                source: "eve",
                label: "follows",
                target: "alice"
            ),
            ConnectedFusionEdge(
                id: "alice-ignored",
                source: "alice",
                label: "blocks",
                target: "ignored"
            ),
        ] {
            try context.insert(edge)
        }
        try await context.save()
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(
            testing: try Schema(entities: [
                try ConnectedFusionResultItem.schemaEntity,
                try ConnectedFusionEdge.schemaEntity,
            ]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "connected-fusion-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        ConnectedFusionResultItem.self
                    ),
                    try DatabaseFrameworkRuntime.entity(
                        ConnectedFusionEdge.self
                    ),
                ]
            ),
            security: .testingDisabled
        )
    }
}

private struct RawConnectedFusionInput: FusionQueryInput {
    typealias Item = ConnectedFusionResultItem

    let source: FusionConnectedSource

    init(_ source: FusionConnectedSource) {
        self.source = source
    }

    var fusionInput: FusionInput {
        FusionInput(
            operation: .connected(source),
            scoring: .annotation(name: "hops", order: .lowerIsBetter)
        )
    }
}
