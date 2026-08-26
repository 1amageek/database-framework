#if MultiBase
import DatabaseKit
import DatabaseRuntime
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine
@_spi(DatabaseExecution) @testable import GraphIndex

private enum CompositionSPARQLAuthorizationProbeError: Error {
    case missingAuthorizationEvidence
}

private struct CompositionSPARQLAuthorizationProbe:
    SPARQLSourceExecutor
{
    func executeInTransaction(
        session: DatabaseReadSession,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedQueryRows {
        guard session.transaction.authorization != nil else {
            throw CompositionSPARQLAuthorizationProbeError
                .missingAuthorizationEvidence
        }
        let rows = try DatabaseRetainedQueryRowsBuilder(
            workMeter: options.workMeter,
            stage: .resultMaterialization
        )
        return rows.finish()
    }

    func executeAskInTransaction(
        session: DatabaseReadSession,
        askQuery: AskQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> Bool {
        throw CanonicalReadError.unsupportedSource(
            "Only SELECT is used by the Composition authorization probe"
        )
    }

    func executeConstructInTransaction(
        session: DatabaseReadSession,
        constructQuery: ConstructQuery,
        nodeNamespace: GraphResultNodeNamespace,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedRDFGraph {
        throw CanonicalReadError.unsupportedSource(
            "Only SELECT is used by the Composition authorization probe"
        )
    }

    func executeDescribeInTransaction(
        session: DatabaseReadSession,
        describeQuery: DescribeQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedRDFGraph {
        throw CanonicalReadError.unsupportedSource(
            "Only SELECT is used by the Composition authorization probe"
        )
    }
}

@Suite("Composition SPARQL authorization")
struct CompositionSPARQLAuthorizationTests {
    @Persistable
    struct Anchor {
        var id: String = ""
    }

    private struct Fixture: Sendable {
        let container: DBContainer
        let baseID: Base.ID
        let readerAuthorization: AuthorizationContext
    }

    @Test("Composition dispatch seals SPARQL authorization evidence")
    func compositionDispatchSealsSPARQLAuthorization() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let source = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(bases: [fixture.baseID])
        let readContext = ReadExecutionContext(
            monotonicClock: fixture.container.monotonicClock
        )

        try await CompositionSPARQLQueryPlanner(
            structuralLimits: readContext.queryStructuralLimits
        ).execute(
            SelectQuery(
                projection: .all,
                source: .graphPattern(.basic([]))
            ),
            source: source,
            graphPartitions: FieldObject(),
            pageSize: 1,
            readContext: readContext
        ) { _ in true }
    }

    private func makeFixture() async throws -> Fixture {
        let domainID = try DatabaseStorageDomain.ID(
            "composition-sparql-authorization"
        )
        let placementID = try Base.Placement.ID(
            "composition-sparql-authorization"
        )
        let baseID = try Base.ID("composition-sparql-authorization")
        let owner = Principal(identifier: "composition-sparql-owner")
        let reader = Principal(identifier: "composition-sparql-reader")
        let engine = InMemoryEngine()
        let topology = try DatabaseStorageTopology(
            controlDomainID: domainID,
            domains: [
                try DatabaseStorageDomain(
                    id: domainID,
                    namespacePath: [
                        "tests", "composition-sparql-authorization"
                    ],
                    storageEngine: engine
                )
            ],
            placements: [
                try DatabaseStoragePlacement(
                    id: placementID,
                    domainID: domainID,
                    path: ["bases"]
                )
            ],
            defaultPlacementID: placementID
        )
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try Anchor.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(
                name: "composition-sparql-authorization",
                storageTopology: topology,
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock()
            ),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "graph-index-tests",
                    revision: 1
                ),
                sparqlSourceExecutor: CompositionSPARQLAuthorizationProbe(),
                entityRuntimes: [
                    try EntityRuntimeDefinition(Anchor.self).registration()
                ]
            ),
            security: .testingDisabled
        )
        do {
            _ = try await container.provisionBase(
                baseID,
                placementID: placementID,
                initialGrants: [
                    Security.Grant(
                        subject: .principal(owner.identifier),
                        resource: .base(baseID),
                        access: .all
                    ),
                    Security.Grant(
                        subject: .principal(reader.identifier),
                        resource: .base(baseID),
                        access: .read
                    ),
                ],
                expectedRevision: 0
            )
        } catch {
            await container.shutdown()
            throw error
        }
        return Fixture(
            container: container,
            baseID: baseID,
            readerAuthorization: .authenticated(reader)
        )
    }
}
#endif
