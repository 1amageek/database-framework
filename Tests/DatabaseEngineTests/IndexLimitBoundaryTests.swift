import DatabaseTypes
import DatabaseKit
import StorageKit
import Testing
@testable import PermutedIndex
@testable import VersionIndex

@Suite("Index limit boundaries")
struct IndexLimitBoundaryTests {
    @Test("Permuted index limit zero performs a zero-row read")
    func permutedLimitZeroIsEmpty() async throws {
        let engine = InMemoryEngine()
        let subspace = Subspace(prefix: Tuple("limit", "permuted").pack())
        do {
            try await engine.withTransaction { transaction in
                try transaction.setValue(
                    [],
                    for: subspace.pack(Tuple("value", "identifier"))
                )
            }
            let rows = try await engine.withTransaction { transaction in
                try await PermutedIndexReader(
                    permutation: Permutation(indices: [0]),
                    subspace: subspace
                ).entries(
                    transaction: transaction,
                    limit: 0
                )
            }
            #expect(rows.isEmpty)
        } catch {
            await engine.shutdown()
            throw error
        }
        await engine.shutdown()
    }

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
