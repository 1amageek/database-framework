import DatabaseServer
import Testing

@Suite("Graph operation limits")
struct GraphOperationLimitsTests {
    @Test("Default and positive SPARQL LOAD limits are valid")
    func acceptsPositiveLoadDocumentLimits() throws {
        #expect(GraphOperationLimits.default.maximumLoadDocumentBytes > 0)
        let limits = try GraphOperationLimits(maximumLoadDocumentBytes: 1)
        #expect(limits.maximumLoadDocumentBytes == 1)
    }

    @Test("SPARQL LOAD byte limits must be positive")
    func rejectsNonPositiveLoadDocumentLimits() {
        for maximum in [0, -1] {
            #expect(
                throws:
                    GraphOperationLimitsError
                        .nonPositiveMaximumLoadDocumentBytes
            ) {
                _ = try GraphOperationLimits(
                    maximumLoadDocumentBytes: maximum
                )
            }
        }
    }
}
