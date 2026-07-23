import Core
import DatabaseEngine
import DatabaseValue
import DatabaseWire
import Graph
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
        transaction: any Transaction
    ) async throws -> DatabaseSHACLResolvedDataSource {
        try validate(entailment: entailment)
        let resolved = try await resolveSource(
            data,
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
            executor: SPARQLQueryExecutor(
                database: container.engine,
                sources: resolved.source.map { [$0] } ?? []
            ),
            graphScope: resolved.graphScope,
            selectedFocusNodes: selectedFocusNodes,
            snapshotFingerprint: Self.bigEndianBytes(logicalVersion)
        )
    }

    private func validate(
        entailment: SHACLExecuteOperation.Entailment
    ) throws {
        switch entailment {
        case .none:
            return
        case .rdfs, .owl:
            _ = ontologyStore
            throw DatabaseSHACLDataSourceError.unsupportedEntailment(
                entailment
            )
        }
    }

    private func resolveSource(
        _ data: SHACLExecuteOperation.DataSource,
        transaction: any Transaction
    ) async throws -> ResolvedSource {
        guard let entity = container.schema.entity(named: data.entity) else {
            throw DatabaseSHACLDataSourceError.entityNotFound(data.entity)
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
            ).readableIndexSubspace(
                named: data.index,
                forEntityName: data.entity,
                partitions: data.partitions,
                transaction: transaction
            )
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
                    coverage: coverage
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
        transaction: any Transaction
    ) async throws -> [DatabaseRDFTerm]? {
        switch focus {
        case .targets:
            return nil
        case .nodes(let nodes):
            return Array(Set(nodes)).sorted()
        case .records(let identities):
            guard let persistableType = entity.persistableType else {
                throw DatabaseSHACLDataSourceError.entityNotFound(data.entity)
            }
            let expectedPartition: [String]
            do {
                expectedPartition = try CanonicalPartitionBinding
                    .makeAnyBinding(
                        for: persistableType,
                        partitions: data.partitions
                    )?.resolve() ?? []
            } catch CanonicalReadError.invalidPartition(_, let reason) {
                throw DatabaseSHACLDataSourceError.invalidPartition(
                    entity: data.entity,
                    reason: reason
                )
            }
            let persistence = container.newContext().makePersistenceHandler()
            var subjects = Set<DatabaseRDFTerm>()
            for identity in identities {
                try workBudget.consume()
                guard identity.entity == data.entity else {
                    throw DatabaseSHACLDataSourceError.recordEntityMismatch(
                        expected: data.entity,
                        actual: identity.entity
                    )
                }
                let resolved = try DatabaseResolvedRecordIdentity.resolve(
                    identity,
                    container: container
                )
                guard resolved.partitionPath == expectedPartition else {
                    throw DatabaseSHACLDataSourceError
                        .recordPartitionMismatch(identity)
                }
                guard let record = try await persistence.load(
                    data.entity,
                    id: resolved.id,
                    partition: resolved.partition,
                    transaction: transaction
                ) else {
                    throw DatabaseSHACLDataSourceError.recordNotFound(identity)
                }
                let fields = try DatabaseRecordProjection.fields(for: record)
                guard let value = fields.first(
                    where: {
                        $0.name == selection.metadata.subjectFieldName
                    }
                )?.value,
                case .rdfTerm(let subject) = value else {
                    throw DatabaseSHACLDataSourceError.recordSubjectMissing(
                        record: identity,
                        field: selection.metadata.subjectFieldName
                    )
                }
                subjects.insert(subject)
            }
            return subjects.sorted()
        }
    }

    private static func bigEndianBytes(_ value: UInt64) -> DatabaseBytes {
        var encoded = value.bigEndian
        return DatabaseBytes(
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
