#if MultiBase
@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import Synchronization
import TestSupport
import Testing
@testable import GraphIndex

@Suite("Composition RDF identity admission")
struct CompositionRDFIdentityAdmissionTests {
    @Test("Qualified blank-node rows are admitted before construction")
    func qualifiedBlankNodeRowIsAdmittedBeforeConstruction() throws {
        let baseID = try Base.ID("qualified-base")
        let row = QueryRow(
            fields: [
                "subject": .rdfTerm(
                    .blankNode(try RDFBlankNodeIdentifier("subject"))
                ),
                "object": .array([
                    .rdfTerm(
                        .blankNode(try RDFBlankNodeIdentifier("nested"))
                    ),
                ]),
            ]
        )
        let generousMeter = makeMeter(maximumBytes: 1 * 1_024 * 1_024)
        let footprint = try CanonicalRelationalFootprintMeter.footprint(
            of: row,
            prefixingRDFBlankNodeIdentifiersWith:
                CompositionRDFIdentity.qualificationPrefix(baseID: baseID),
            workMeter: generousMeter,
            stage: .resultMaterialization
        )
        let qualified = try CompositionRDFIdentity.qualifyBlankNodes(
            in: row,
            baseID: baseID
        )
        let observed = try CanonicalRelationalFootprintMeter.footprint(
            of: qualified,
            workMeter: generousMeter,
            stage: .resultMaterialization
        )
        #expect(footprint == observed)
        #expect(footprint.bytes > 0)

        let constrainedMeter = makeMeter(
            maximumBytes: footprint.bytes - 1
        )
        let producerWasInvoked = Mutex(false)
        #expect(throws: DatabaseWorkLimitError.self) {
            _ = try DatabaseQueryScopedQueryRow.producing(
                exactFootprint: footprint,
                workMeter: constrainedMeter,
                stage: .resultMaterialization
            ) {
                producerWasInvoked.withLock { $0 = true }
                return try CompositionRDFIdentity.qualifyBlankNodes(
                    in: row,
                    baseID: baseID
                )
            }
        }
        #expect(!producerWasInvoked.withLock { $0 })
        #expect(constrainedMeter.retainedIntermediateRows == 0)
        #expect(constrainedMeter.retainedIntermediateBytes == 0)
    }

    private func makeMeter(maximumBytes: UInt64) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 100,
                maximumWorkUnits: 100_000,
                maximumIntermediateRows: 16,
                maximumIntermediateBytes: maximumBytes,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }
}
#endif
