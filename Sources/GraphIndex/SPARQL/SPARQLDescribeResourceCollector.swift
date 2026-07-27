import DatabaseKit
import DatabaseEngine
import DatabaseTypes

/// Deduplicates DESCRIBE resources while retaining their stable first order.
struct SPARQLDescribeResourceCollector: ~Copyable {
    private let seen: SPARQLRetainedFieldValueSet
    private var storage: DatabaseRetainedArrayBuilder<RDFTerm>
    private let footprintMeter: SPARQLRDFOutputFootprintMeter

    private init(
        seen: SPARQLRetainedFieldValueSet,
        storage: consuming DatabaseRetainedArrayBuilder<RDFTerm>,
        footprintMeter: SPARQLRDFOutputFootprintMeter
    ) {
        self.seen = seen
        self.storage = consume storage
        self.footprintMeter = footprintMeter
    }

    static func make(
        workMeter: DatabaseWorkMeter
    ) throws -> SPARQLDescribeResourceCollector {
        let seen = try SPARQLRetainedFieldValueSet.make(
            workMeter: workMeter,
            stage: .deduplication
        )
        let footprintMeter = try SPARQLRDFOutputFootprintMeter.make(
            workMeter: workMeter,
            stage: .deduplication
        )
        do {
            return SPARQLDescribeResourceCollector(
                seen: seen,
                storage: try DatabaseRetainedArrayBuilder(
                    workMeter: workMeter,
                    stage: .deduplication,
                    layout: try SPARQLRDFOutputFootprintMeter
                        .termArrayLayout()
                ),
                footprintMeter: footprintMeter
            )
        } catch {
            footprintMeter.shutdown()
            throw error
        }
    }

    mutating func insert(
        _ term: borrowing RDFTerm,
        workMeter: DatabaseWorkMeter
    ) throws {
        try workMeter.consume(at: .deduplication)
        guard try seen.insert(.rdfTerm(copy term)) else { return }
        let footprint = try footprintMeter.footprint(of: term)
        let admission = try storage.prepareAppend(
            footprint: footprint,
            at: .deduplication
        )
        storage.append(copy term, using: consume admission)
    }

    consuming func finish() -> DatabaseRetainedBuffer<RDFTerm> {
        footprintMeter.shutdown()
        return storage.finish()
    }
}
