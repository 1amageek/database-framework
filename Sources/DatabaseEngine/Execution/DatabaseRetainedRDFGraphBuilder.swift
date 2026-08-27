import DatabaseKit

/// Public admitted builder for custom SPARQL runtime implementations.
public struct DatabaseRetainedRDFGraphBuilder: ~Copyable {
    private var storage: DatabaseRetainedArrayBuilder<RDFQuad>
    private let footprintMeter: DatabaseRDFQuadFootprintMeter
    package let workMeter: DatabaseWorkMeter

    package var producerFootprintMeter: DatabaseRDFQuadFootprintMeter {
        footprintMeter
    }

    public init(
        workMeter: DatabaseWorkMeter,
        expectedCount: Int = 0
    ) throws {
        let footprintMeter = try DatabaseRDFQuadFootprintMeter.make(
            workMeter: workMeter,
            stage: .resultMaterialization
        )
        do {
            self.storage = try DatabaseRetainedArrayBuilder(
                workMeter: workMeter,
                stage: .resultMaterialization,
                layout: try DatabaseRetainedArrayLayout.validated(
                    containerByteCount: 64,
                    elementCapacitySlotByteCount: 64,
                    sharedOwnerByteCount: 64,
                    appendAdmissionByteCount: 64
                ),
                expectedCount: expectedCount
            )
            self.footprintMeter = footprintMeter
            self.workMeter = workMeter
        } catch {
            footprintMeter.shutdown()
            throw error
        }
    }

    public mutating func append(
        _ quad: consuming RDFQuad
    ) throws {
        try workMeter.consume(at: .resultMaterialization)
        let footprint = try footprintMeter.footprint(of: quad)
        let admission = try storage.prepareAppend(
            footprint: footprint,
            at: .resultMaterialization
        )
        storage.append(consume quad, using: consume admission)
    }

    /// Admits a borrowed scanner result before copying it into owned output.
    package mutating func appendBorrowed(
        _ quad: borrowing RDFQuad
    ) throws {
        try workMeter.consume(at: .resultMaterialization)
        let footprint = try footprintMeter.footprint(of: quad)
        try storage.append(
            footprint: footprint,
            at: .resultMaterialization,
            make: { copy quad }
        )
    }

    /// Transfers one purpose-bound produced quad after its destination has
    /// admitted the exact retained footprint. The producer claim remains live
    /// through the destination copy and is released when this call returns.
    package mutating func appendProduced(
        _ produced: consuming DatabaseQueryScopedRDFQuad
    ) throws {
        guard produced.workMeter === workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        try workMeter.consume(at: .resultMaterialization)
        try produced.withQuad { quad in
            try storage.append(
                footprint: produced.footprint,
                at: .resultMaterialization,
                make: { copy quad }
            )
        }
    }

    /// Admits the destination footprint before constructing the RDFQuad.
    package mutating func append(
        subject: RDFSubject,
        predicate: RDFPredicateIRI,
        object: RDFTerm,
        graph: RDFGraphName? = nil
    ) throws {
        try workMeter.consume(at: .resultMaterialization)
        let footprint = try footprintMeter.footprint(
            subject: subject,
            predicate: predicate,
            object: object,
            graph: graph
        )
        try storage.append(
            footprint: footprint,
            at: .resultMaterialization,
            make: {
                RDFQuad(
                    subject: subject,
                    predicate: predicate,
                    object: object,
                    graph: graph
                )
            }
        )
    }

    public consuming func finish() -> DatabaseRetainedRDFGraph {
        footprintMeter.shutdown()
        return DatabaseRetainedRDFGraph(storage: storage.finish())
    }
}
