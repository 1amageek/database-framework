import DatabaseTypes
import StorageKit
import Testing
@testable import VersionIndex

@Suite("Index limit boundaries")
struct IndexLimitBoundaryTests {
    @Test("Version history limit zero performs a zero-row read")
    func versionLimitZeroIsEmpty() async throws {
        let engine = InMemoryEngine()
        let subspace = Subspace(prefix: Tuple("limit", "version").pack())
        let key = subspace.pack(Tuple("identifier")).appending(
            contentsOf: ByteString([
                0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
            ])
        )
        do {
            try await engine.withTransaction { transaction in
                try transaction.setValue(
                    ByteString([
                        0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0,
                        1,
                    ]),
                    for: key
                )
            }
            let rows = try await engine.withTransaction { transaction in
                try await VersionIndexReader(subspace: subspace).history(
                    primaryKey: ["identifier"],
                    limit: 0,
                    transaction: transaction
                )
            }
            #expect(rows.isEmpty)
        } catch {
            await engine.shutdown()
            throw error
        }
        await engine.shutdown()
    }
}
