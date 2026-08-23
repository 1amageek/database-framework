#if DATABASE_MULTI_BASE
@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Base-local RDF graph form accepted by the Composition planner.
@_spi(DatabaseExecution)
public enum CompositionRDFStatement: Sendable {
    case construct(ConstructQuery)
    case describe(DescribeQuery)
}

@_spi(DatabaseExecution)
public struct CompositionRDFMetadata: Sendable {
    public let composition: CompositionResolution
    public let basePlacementGenerations: [Base.ID: UInt64]
    public let schemaGeneration: UInt64
    public let consistency: DatabaseKit.DatabaseReadConsistency

    public init(
        composition: CompositionResolution,
        basePlacementGenerations: [Base.ID: UInt64],
        schemaGeneration: UInt64,
        consistency: DatabaseKit.DatabaseReadConsistency
    ) {
        self.composition = composition
        self.basePlacementGenerations = basePlacementGenerations
        self.schemaGeneration = schemaGeneration
        self.consistency = consistency
    }
}

@_spi(DatabaseExecution)
public struct CompositionRDFResult: Sendable {
    public let quad: RDFQuad
    public let origin: CompositionOrigin

    public init(quad: RDFQuad, origin: CompositionOrigin) {
        self.quad = quad
        self.origin = origin
    }
}

@_spi(DatabaseExecution)
public enum CompositionRDFQueryEvent: Sendable {
    case began(CompositionRDFMetadata)
    case quad(CompositionRDFResult)
}

@_spi(DatabaseExecution)
public struct CompositionAskResult: Sendable {
    public let value: Bool
    public let metadata: CompositionRDFMetadata
    public let origin: CompositionOrigin

    public init(
        value: Bool,
        metadata: CompositionRDFMetadata,
        origin: CompositionOrigin
    ) {
        self.value = value
        self.metadata = metadata
        self.origin = origin
    }
}

/// Owns Composition semantics for RDF graph forms and ASK. Wire paging and
/// durable remote continuation remain host-adapter responsibilities.
@_spi(DatabaseExecution)
public struct CompositionRDFQueryPlanner: Sendable {
    private enum Field {
        static let subject = "subject"
        static let predicate = "predicate"
        static let object = "object"
        static let graph = "graph"
    }

    public init() {}

    public func execute(
        _ statement: CompositionRDFStatement,
        source: CompositionDataSource,
        graphPartitions: FieldObject,
        nodeNamespace: GraphResultNodeNamespace? = nil,
        readContext: ReadExecutionContext,
        emit: @Sendable @escaping (
            CompositionRDFQueryEvent
        ) async throws -> Bool
    ) async throws {
        try CompositionSPARQLPlanValidator.validate(statement)
        guard let executor = source.container.runtimeConfiguration
            .logicalSourceExecutors.sparqlExecutor else {
            throw CanonicalReadError.unsupportedSource(
                "SPARQL source executor is not registered"
            )
        }
        let workspace = CompositionDistinctWorkspace.create(
            maximumIntermediateBytes: readContext.options.budget
                .maximumIntermediateBytes,
            workMeter: readContext.workMeter
        )
        do {
            try await source.withReadSnapshot { snapshot in
                let authorization = try Self.authorization(for: statement)
                let admissions = try await makeReadAdmissions(
                    authorization: authorization,
                    source: source,
                    snapshot: snapshot
                )
                let metadata = CompositionRDFMetadata(
                    composition: snapshot.resolution,
                    basePlacementGenerations: snapshot
                        .basePlacementGenerations,
                    schemaGeneration: source.container.schemaGeneration,
                    consistency: .federated(try await snapshot.readPoints())
                )
                guard try await emit(.began(metadata)) else { return }
                var nextSequence: UInt64 = 0
                for member in snapshot.members {
                    guard let admission = admissions[member.baseID] else {
                        throw CompositionQueryError.workspaceCorrupted
                    }
                    let memberSequenceStart = nextSequence
                    nextSequence = try await source.withMemberContext(
                        member,
                        in: snapshot
                    ) { [memberSequenceStart] databaseContext, transaction in
                        try await databaseContext
                            .withReadAuthorizationAdmission(admission) {
                                let graph: DatabaseRetainedRDFGraph
                                switch statement {
                                case .construct(let query):
                                    guard let nodeNamespace else {
                                        throw CompositionQueryError
                                            .invalidExecutionConfiguration(
                                                "CONSTRUCT requires a deterministic graph-result node namespace"
                                            )
                                    }
                                    graph = try await executor
                                        .executeConstructInTransaction(
                                            context: databaseContext,
                                            constructQuery: query,
                                            nodeNamespace: nodeNamespace,
                                            options: readContext,
                                            partitions: graphPartitions,
                                            transaction: transaction
                                        )
                                case .describe(let query):
                                    graph = try await executor
                                        .executeDescribeInTransaction(
                                            context: databaseContext,
                                            describeQuery: query,
                                            options: readContext,
                                            partitions: graphPartitions,
                                            transaction: transaction
                                        )
                                }
                                var memberSequence = memberSequenceStart
                                for index in 0..<graph.count {
                                    let quad = try graph.withElement(at: index) {
                                        value in
                                        try CompositionRDFIdentity
                                            .qualifyBlankNodes(
                                                in: copy value,
                                                baseID: member.baseID
                                            )
                                    }
                                    let currentSequence = memberSequence
                                    let next = memberSequence
                                        .addingReportingOverflow(1)
                                    guard !next.overflow else {
                                        throw CompositionQueryError
                                            .workspaceCorrupted
                                    }
                                    memberSequence = next.partialValue
                                    try await workspace.insert(
                                        try Self.row(from: quad),
                                        origin: .source(member.baseID),
                                        sequence: currentSequence
                                    )
                                }
                                return memberSequence
                            }
                    }
                }
                try await workspace.forEachResult(batchSize: 64) { result in
                    try await emit(
                        .quad(
                            CompositionRDFResult(
                                quad: try Self.quad(from: result.row),
                                origin: result.origin
                            )
                        )
                    )
                }
            }
            await workspace.removeAll()
        } catch {
            let operationError = error
            await workspace.removeAll()
            throw operationError
        }
    }

    public func executeAsk(
        _ query: AskQuery,
        source: CompositionDataSource,
        graphPartitions: FieldObject,
        readContext: ReadExecutionContext
    ) async throws -> CompositionAskResult {
        try CompositionSPARQLPlanValidator.validate(query)
        guard let executor = source.container.runtimeConfiguration
            .logicalSourceExecutors.sparqlExecutor else {
            throw CanonicalReadError.unsupportedSource(
                "SPARQL source executor is not registered"
            )
        }
        return try await source.withReadSnapshot { snapshot in
            let admissions = try await makeReadAdmissions(
                authorization: try IndexReadAuthorization(
                    modifiers: query.modifiers
                ),
                source: source,
                snapshot: snapshot
            )
            var matchingBases: [Base.ID] = []
            matchingBases.reserveCapacity(snapshot.members.count)
            for member in snapshot.members {
                guard let admission = admissions[member.baseID] else {
                    throw CompositionQueryError.workspaceCorrupted
                }
                let matched = try await source.withMemberContext(
                    member,
                    in: snapshot
                ) { databaseContext, transaction in
                    try await databaseContext
                        .withReadAuthorizationAdmission(admission) {
                            try await executor.executeAskInTransaction(
                                context: databaseContext,
                                askQuery: query,
                                options: readContext,
                                partitions: graphPartitions,
                                transaction: transaction
                            )
                        }
                }
                if matched { matchingBases.append(member.baseID) }
            }
            let allBases = snapshot.resolution.bases
            return CompositionAskResult(
                value: !matchingBases.isEmpty,
                metadata: CompositionRDFMetadata(
                    composition: snapshot.resolution,
                    basePlacementGenerations: snapshot
                        .basePlacementGenerations,
                    schemaGeneration: source.container.schemaGeneration,
                    consistency: .federated(try await snapshot.readPoints())
                ),
                origin: .derived(
                    contributors: matchingBases.isEmpty
                        ? allBases
                        : matchingBases
                )
            )
        }
    }

    private static func authorization(
        for statement: CompositionRDFStatement
    ) throws -> IndexReadAuthorization {
        switch statement {
        case .construct(let query):
            return try IndexReadAuthorization(modifiers: query.modifiers)
        case .describe(let query):
            return try IndexReadAuthorization(modifiers: query.modifiers)
        }
    }

    private func makeReadAdmissions(
        authorization: IndexReadAuthorization,
        source: CompositionDataSource,
        snapshot: DatabaseCompositionReadSnapshot
    ) async throws -> [Base.ID: DatabaseReadAuthorizationAdmission] {
        var admissions: [Base.ID: DatabaseReadAuthorizationAdmission] = [:]
        admissions.reserveCapacity(snapshot.members.count)
        for member in snapshot.members {
            admissions[member.baseID] = try await source.withMemberContext(
                member,
                in: snapshot
            ) { context, _ in
                let fieldPlan = DatabaseFieldReadAuthorizationPlan
                    .rdfDataset(schema: context.container.schema)
                return try context.admitLogicalRead(
                    listAuthorization: authorization,
                    fieldPlan: fieldPlan,
                    restrictingTo: Set(fieldPlan.fieldsByEntity.keys)
                )
            }
        }
        return admissions
    }

    private static func row(from quad: RDFQuad) throws -> QueryRow {
        QueryRow(
            fields: [
                Field.subject: .rdfTerm(quad.subject.term),
                Field.predicate: .rdfTerm(quad.predicate.term),
                Field.object: .rdfTerm(quad.object),
                Field.graph: quad.graph.map { .rdfTerm($0.term) } ?? .null,
            ]
        )
    }

    private static func quad(from row: QueryRow) throws -> RDFQuad {
        guard case .rdfTerm(let subjectTerm)? = row.fields[Field.subject],
              case .rdfTerm(let predicateTerm)? = row.fields[Field.predicate],
              case .rdfTerm(let object)? = row.fields[Field.object] else {
            throw CompositionQueryError.workspaceCorrupted
        }
        let subject: RDFSubject
        switch subjectTerm {
        case .iri(let iri):
            subject = .iri(iri)
        case .blankNode(let identifier):
            subject = .blankNode(identifier)
        case .literal, .tripleTerm:
            throw CompositionQueryError.workspaceCorrupted
        }
        guard case .iri(let predicateIRI) = predicateTerm else {
            throw CompositionQueryError.workspaceCorrupted
        }
        let graph: RDFGraphName?
        switch row.fields[Field.graph] {
        case .null?:
            graph = nil
        case .rdfTerm(let term)?:
            do { graph = try RDFGraphName(term) }
            catch { throw CompositionQueryError.workspaceCorrupted }
        default:
            throw CompositionQueryError.workspaceCorrupted
        }
        return RDFQuad(
            subject: subject,
            predicate: RDFPredicateIRI(predicateIRI),
            object: object,
            graph: graph
        )
    }
}
#endif
