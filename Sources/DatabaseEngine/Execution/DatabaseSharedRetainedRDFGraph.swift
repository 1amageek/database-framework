import DatabaseKit

/// Copyable request-accounted RDF graph ownership for asynchronous internal
/// stages that run after the source transaction has ended.
@_spi(DatabaseExecution)
public struct DatabaseSharedRetainedRDFGraph: Sendable {
    private let storage: DatabaseSharedRetainedArray<RDFQuad>

    package init(storage: DatabaseSharedRetainedArray<RDFQuad>) {
        self.storage = storage
    }

    public var count: Int { storage.count }
    public var isEmpty: Bool { storage.isEmpty }

    /// Creates a separately admitted page while the shared full-result owner
    /// remains retained. Admission precedes every quad copy.
    public func retainedPage(
        _ range: Range<Int>,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseRetainedRDFGraphPage {
        precondition(
            range.lowerBound >= 0 && range.upperBound <= storage.count
        )
        guard storage.workMeter === workMeter else {
            throw CanonicalReadError.executorWorkMeterMismatch(
                sourceName: "retained RDF continuation graph"
            )
        }
        let footprintMeter = try DatabaseRDFQuadFootprintMeter.make(
            workMeter: workMeter,
            stage: .resultMaterialization
        )
        defer { footprintMeter.shutdown() }
        var builder = try DatabaseRetainedArrayBuilder<RDFQuad>(
            workMeter: workMeter,
            stage: .resultMaterialization,
            layout: try DatabaseRetainedArrayLayout.validated(
                containerByteCount: 64,
                elementCapacitySlotByteCount: 64,
                sharedOwnerByteCount: 64,
                appendAdmissionByteCount: 64
            ),
            expectedCount: range.count
        )
        for index in range {
            try storage.withElement(at: index) { quad in
                let admission = try builder.prepareAppend(
                    footprint: try footprintMeter.footprint(of: quad),
                    at: .resultMaterialization
                )
                builder.append(copy quad, using: consume admission)
            }
        }
        return DatabaseRetainedRDFGraphPage(
            storage: try builder.finish().moveToSharedOwnership(
                at: .resultMaterialization
            )
        )
    }
}
