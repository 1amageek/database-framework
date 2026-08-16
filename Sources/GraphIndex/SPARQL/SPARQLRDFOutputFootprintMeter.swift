import DatabaseKit
import DatabaseEngine
import DatabaseTypes

/// Measures retained RDF output before the corresponding owner is created.
final class SPARQLRDFOutputFootprintMeter {
    private static let termPayloadByteCount: UInt64 = 32

    private let valueMeter: SPARQLBindingFootprintMeter

    private init(valueMeter: SPARQLBindingFootprintMeter) {
        self.valueMeter = valueMeter
    }

    static func make(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> SPARQLRDFOutputFootprintMeter {
        SPARQLRDFOutputFootprintMeter(
            valueMeter: try SPARQLBindingFootprintMeter.make(
                workMeter: workMeter,
                stage: stage
            )
        )
    }

    static func termArrayLayout() throws -> DatabaseRetainedArrayLayout {
        try DatabaseRetainedArrayLayout.validated(
            containerByteCount: 64,
            elementCapacitySlotByteCount: 48,
            sharedOwnerByteCount: 64,
            appendAdmissionByteCount: 64
        )
    }

    func footprint(
        of term: borrowing RDFTerm
    ) throws -> DatabaseIntermediateFootprint {
        try DatabaseIntermediateFootprint(
            rows: 1,
            bytes: Self.termPayloadByteCount
        ).adding(rdfPayloadFootprint(of: term))
    }

    func shutdown() {
        valueMeter.shutdown()
    }

    private func rdfPayloadFootprint(
        of term: borrowing RDFTerm
    ) throws -> DatabaseIntermediateFootprint {
        try valueMeter.footprint(of: .rdfTerm(copy term))
    }
}
