import DatabaseKit

/// Public admitted builder for custom SPARQL runtime implementations.
public struct DatabaseRetainedRDFGraphBuilder: ~Copyable {
    private var storage: DatabaseRetainedArrayBuilder<RDFQuad>
    private let footprintMeter: DatabaseRDFQuadFootprintMeter
    package let workMeter: DatabaseWorkMeter

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

    public consuming func finish() -> DatabaseRetainedRDFGraph {
        footprintMeter.shutdown()
        return DatabaseRetainedRDFGraph(storage: storage.finish())
    }
}
