import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
#if DATABASE_OPERATIONS_MULTIPLE_BASES
#if DATABASE_OPERATIONS_GRAPH_INDEXES
import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import Synchronization

/// Adapts exact row-identity spill storage to RDF quads without introducing a
/// second durability, collision, or contributor-merging implementation.
final class DatabaseCompositionRDFDistinctSpill: Sendable {
    struct Result: Sendable {
        let quad: RDFQuad
        let origin: CompositionOrigin
    }

    private enum Field {
        static let subject = "subject"
        static let predicate = "predicate"
        static let object = "object"
        static let graph = "graph"
    }

    private let spill: DatabaseCompositionDistinctSpill
    private let sequence = Mutex<UInt64>(0)

    init(
        snapshotStore: DatabaseQuerySnapshotStore,
        reservation: DatabaseQuerySnapshotStore.WriteReservation,
        maximumIntermediateBytes: UInt64,
        workMeter: DatabaseWorkMeter
    ) {
        self.spill = DatabaseCompositionDistinctSpill(
            snapshotStore: snapshotStore,
            reservation: reservation,
            maximumIntermediateBytes: maximumIntermediateBytes,
            workMeter: workMeter
        )
    }

    var payloadByteCount: UInt64 {
        get async { await spill.payloadByteCount }
    }

    func insert(
        _ quad: RDFQuad,
        origin: CompositionOrigin
    ) async throws {
        let currentSequence = try sequence.withLock { value in
            let current = value
            let incremented = value.addingReportingOverflow(1)
            guard !incremented.overflow else {
                throw DatabaseQueryExecutionError
                    .querySnapshotCorrupted
            }
            value = incremented.partialValue
            return current
        }
        try await spill.insert(
            DatabaseEngine.QueryRow(
                fields: [
                    Field.subject: .rdfTerm(quad.subject.term),
                    Field.predicate: .rdfTerm(quad.predicate.term),
                    Field.object: .rdfTerm(quad.object),
                    Field.graph: quad.graph.map {
                        .rdfTerm($0.term)
                    } ?? .null,
                ]
            ),
            origin: origin,
            sequence: currentSequence
        )
    }

    func forEachResult(
        batchSize: Int,
        _ body: @Sendable (Result) async throws -> Bool
    ) async throws {
        try await spill.forEachResult(batchSize: batchSize) { result in
            try await body(
                Result(
                    quad: try Self.quad(from: result.row),
                    origin: result.origin
                )
            )
        }
    }

    private static func quad(
        from row: DatabaseEngine.QueryRow
    ) throws -> RDFQuad {
        guard case .rdfTerm(let subjectTerm)? = row.fields[Field.subject],
              case .rdfTerm(let predicateTerm)? = row.fields[Field.predicate],
              case .rdfTerm(let object)? = row.fields[Field.object]
        else {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
        }
        let subject: RDFSubject
        switch subjectTerm {
        case .iri(let iri):
            subject = .iri(iri)
        case .blankNode(let identifier):
            subject = .blankNode(identifier)
        case .literal, .tripleTerm:
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
        }
        guard case .iri(let predicateIRI) = predicateTerm else {
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
        }
        let graph: RDFGraphName?
        switch row.fields[Field.graph] {
        case .null?:
            graph = nil
        case .rdfTerm(let term)?:
            do {
                graph = try RDFGraphName(term)
            } catch {
                throw DatabaseQueryExecutionError
                    .querySnapshotCorrupted
            }
        default:
            throw DatabaseQueryExecutionError.querySnapshotCorrupted
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

#endif
