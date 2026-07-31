import DatabaseKit
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import GraphIndex
import OntologyIndex
import StorageKit

public struct SchemaDatabaseSHACLDataSourceResolver:
    DatabaseSHACLDataSourceResolver {
    private let container: DBContainer
    private let stateStore: DatabaseMutationStateStore
    private let ontologyStore: OntologyStore

    public init(
        container: DBContainer,
        stateStore: DatabaseMutationStateStore,
        ontologyStore: OntologyStore
    ) {
        self.container = container
        self.stateStore = stateStore
        self.ontologyStore = ontologyStore
    }

    public func resolve(
        data: SHACLExecuteOperation.DataSource,
        focus: SHACLExecuteOperation.Focus,
        entailment: SHACLExecuteOperation.Entailment,
        workBudget: SHACLValidationWorkBudget,
        transaction: any TransactionAccess
    ) async throws -> DatabaseSHACLResolvedDataSource {
        let resolved = try await resolveSource(
            data,
            transaction: transaction
        )
        let executor = SPARQLQueryExecutor(
            database: container.engine,
            wallClock: container.wallClock,
            sources: resolved.source.map { [$0] } ?? []
        )
        let entailmentResolution = try await DatabaseSHACLEntailmentResolver(
            ontologyStore: ontologyStore,
            monotonicClock: container.monotonicClock
        ).resolve(
            entailment,
            executor: executor,
            graphScope: resolved.graphScope,
            workBudget: workBudget,
            transaction: transaction
        )
        let selectedFocusNodes = try await resolveFocusNodes(
            focus,
            data: data,
            entity: resolved.entity,
            selection: resolved.selection,
            workBudget: workBudget,
            transaction: transaction
        )
        try workBudget.consume()
        let logicalVersion = try await stateStore.currentLogicalVersion(
            transaction: transaction
        )

        return DatabaseSHACLResolvedDataSource(
            data: data,
            focus: focus,
            entailment: entailment,
            executor: executor.withOntology(
                entailmentResolution.ontologyContext
            ),
            graphScope: resolved.graphScope,
            entailmentContext: entailmentResolution.entailmentContext,
            selectedFocusNodes: selectedFocusNodes,
            snapshotFingerprint: Self.bigEndianBytes(logicalVersion)
        )
    }

    private func resolveSource(
        _ data: SHACLExecuteOperation.DataSource,
        transaction: any TransactionAccess
    ) async throws -> ResolvedSource {
        guard let entity = container.schema.entity(named: data.entity) else {
            throw DatabaseSHACLDataSourceError.schemaEntityNotFound(data.entity)
        }
        guard let descriptor = entity.indexDescriptors.first(
            where: { $0.name == data.index }
        ) else {
            throw DatabaseSHACLDataSourceError.indexNotFound(
                entity: data.entity,
                index: data.index
            )
        }
        guard let selection = try RDFDatasetIndexSelection(
            descriptor: descriptor
        ) else {
            throw DatabaseSHACLDataSourceError.indexIsNotRDFDataset(
                entity: data.entity,
                index: data.index
            )
        }
        let coverage = try selection.metadata.graphScope.sourceCoverage
        let graphScope = try resolveGraphScope(
            data.graph,
            coverage: coverage,
            entity: data.entity,
            index: data.index
        )
        let indexSubspace: Subspace?
        do {
            indexSubspace = try await IndexQueryContext(
                context: container.newContext()
            ).readableIndex(
                named: data.index,
                kindIdentifier: descriptor.kindIdentifier,
                forEntityName: data.entity,
                partitions: data.partitions,
                transaction: transaction
            )?.subspace
        } catch CanonicalReadError.invalidPartition(_, let reason) {
            throw DatabaseSHACLDataSourceError.invalidPartition(
                entity: data.entity,
                reason: reason
            )
        }
        return ResolvedSource(
            entity: entity,
            selection: selection,
            source: indexSubspace.map {
                RDFDatasetSource(
                    entityName: data.entity,
                    indexName: data.index,
                    indexSubspace: $0,
                    coverage: coverage,
                    storedFieldNames: selection.storedFieldNames
                )
            },
            graphScope: graphScope
        )
    }

    private func resolveGraphScope(
        _ graph: SHACLExecuteOperation.DataGraph,
        coverage: RDFDatasetSourceCoverage,
        entity: String,
        index: String
    ) throws -> SHACLDataGraphScope {
        switch graph {
        case .defaultGraph:
            guard coverage == .defaultGraph || coverage == .dataset else {
                throw DatabaseSHACLDataSourceError.graphNotCovered(
                    entity: entity,
                    index: index
                )
            }
            return .defaultGraph
        case .named(let term):
            let graphName: RDFGraphName
            do {
                graphName = try RDFGraphName(term)
            } catch {
                throw DatabaseSHACLDataSourceError.invalidGraphName(term)
            }
            guard coverage == .dataset || coverage == .namedGraph(graphName) else {
                throw DatabaseSHACLDataSourceError.graphNotCovered(
                    entity: entity,
                    index: index
                )
            }
            return .named(graphName)
        }
    }

    private func resolveFocusNodes(
        _ focus: SHACLExecuteOperation.Focus,
        data: SHACLExecuteOperation.DataSource,
        entity: Schema.Entity,
        selection: RDFDatasetIndexSelection,
        workBudget: SHACLValidationWorkBudget,
        transaction: any TransactionAccess
    ) async throws -> [RDFTerm]? {
        switch focus {
        case .targets:
            return nil
        case .nodes(let nodes):
            return Array(Set(nodes)).sorted()
        case .entities(let identities):
            guard container.runtimeConfiguration.entityRuntimes
                .registration(named: entity.name) != nil else {
                throw DatabaseSHACLDataSourceError.schemaEntityNotFound(data.entity)
            }
            let expectedPartition: [String]
            do {
                expectedPartition = try CanonicalPartitionBinding
                    .makeAnyBinding(
                        for: entity,
                        partitions: data.partitions
                    )?.resolve() ?? []
            } catch CanonicalReadError.invalidPartition(_, let reason) {
                throw DatabaseSHACLDataSourceError.invalidPartition(
                    entity: data.entity,
                    reason: reason
                )
            }
            let databaseTransaction = DatabaseTransaction(
                storageAccess: transaction,
                container: container
            )
            var subjects = Set<RDFTerm>()
            for identity in identities {
                try workBudget.consume()
                guard identity.entity == data.entity else {
                    throw DatabaseSHACLDataSourceError.focusEntityMismatch(
                        expected: data.entity,
                        actual: identity.entity
                    )
                }
                let resolved = try ResolvedEntityReference.resolve(
                    identity,
                    container: container
                )
                guard resolved.partitionPath == expectedPartition else {
                    throw DatabaseSHACLDataSourceError
                        .focusPartitionMismatch(identity)
                }
                guard let entity = try await databaseTransaction
                    .loadPersistedModel(
                    entity: data.entity,
                    id: resolved.id,
                    partition: resolved.partition
                    ) else {
                    throw DatabaseSHACLDataSourceError.focusEntityNotFound(identity)
                }
                let fields = try DatabaseEntityProjection.fieldObject(
                    for: entity
                )
                guard let value = fields[
                    selection.metadata.subjectFieldName
                ],
                case .rdfTerm(let subject) = value else {
                    throw DatabaseSHACLDataSourceError.focusSubjectMissing(
                        entity: identity,
                        field: selection.metadata.subjectFieldName
                    )
                }
                subjects.insert(subject)
            }
            return subjects.sorted()
        }
    }

    private static func bigEndianBytes(_ value: UInt64) -> ByteString {
        var encoded = value.bigEndian
        return ByteString(
            withUnsafeBytes(of: &encoded) { Array($0) }
        )
    }

    private struct ResolvedSource {
        let entity: Schema.Entity
        let selection: RDFDatasetIndexSelection
        let source: RDFDatasetSource?
        let graphScope: SHACLDataGraphScope
    }
}
