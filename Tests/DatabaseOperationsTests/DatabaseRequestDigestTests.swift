import DatabaseWire
import Testing
@testable import DatabaseOperations

@Suite("Database request digest")
struct DatabaseRequestDigestTests {
    @Test("Digests bind both operation and payload")
    func bindsOperationAndPayload() {
        let query = DatabaseRequestDigest.compute(
            operation: .queryExecute,
            payload: [1, 2, 3]
        )
        let mutation = DatabaseRequestDigest.compute(
            operation: .mutationExecute,
            payload: [1, 2, 3]
        )
        let differentPayload = DatabaseRequestDigest.compute(
            operation: .queryExecute,
            payload: [1, 2, 4]
        )

        #expect(query.count == DatabaseRequestDigest.byteCount)
        #expect(query != mutation)
        #expect(query != differentPayload)
    }
}
