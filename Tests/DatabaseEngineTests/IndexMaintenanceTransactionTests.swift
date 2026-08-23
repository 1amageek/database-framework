import DatabaseTypes
import StorageKit
import Testing
@testable import DatabaseEngine

private actor EscapedIndexMaintenanceAccess {
    private var access: (any IndexMaintenanceTransactionAccess)?

    func store(_ access: any IndexMaintenanceTransactionAccess) {
        self.access = access
    }

    func read(_ key: ByteString) async throws -> ByteString? {
        guard let access else { return nil }
        return try await access.getValue(for: key, snapshot: true)
    }
}

@Suite("Index maintenance transaction capability")
struct IndexMaintenanceTransactionTests {
    @Test("Maintenance reads and writes remain inside one index")
    func confinesReadsWritesAndConflicts() async throws {
        let engine = InMemoryEngine()
        let index = Subspace(prefix: Tuple("indexes", "allowed").pack())
        let neighbor = Subspace(prefix: Tuple("indexes", "neighbor").pack())
        let allowedKey = index.pack(Tuple("entry"))
        let neighborKey = neighbor.pack(Tuple("entry"))
        let range = index.range()
        let neighborRange = neighbor.range()

        _ = try await engine.withTransaction { transaction in
            try await withIndexMaintenanceTransaction(
                transaction: transaction,
                indexSubspace: index
            ) { access in
                try access.setValue([0x01], for: allowedKey)
                #expect(
                    try await access.getValue(
                        for: allowedKey,
                        snapshot: false
                    ) == [0x01]
                )
                try access.addConflictRange(
                    beginKey: range.begin,
                    endKey: range.end,
                    type: .write
                )
                #expect(throws: DatabaseReadTransactionError.keyOutsideDataRoot) {
                    try access.setValue([0x02], for: neighborKey)
                }
                #expect(throws: DatabaseReadTransactionError.rangeOutsideDataRoot) {
                    try access.addConflictRange(
                        beginKey: range.begin,
                        endKey: neighborRange.end,
                        type: .write
                    )
                }
            }
        }
    }

    @Test("Maintenance cannot acquire transaction controls")
    func rejectsTransactionControls() async throws {
        let engine = InMemoryEngine()
        let index = Subspace(prefix: Tuple("indexes", "controlled").pack())

        try await engine.withTransaction { transaction in
            try await withIndexMaintenanceTransaction(
                transaction: transaction,
                indexSubspace: index
            ) { access in
                await #expect(
                    throws: DatabaseReadTransactionError
                        .transactionControlUnavailable
                ) {
                    _ = try await access.getReadVersion()
                }
                #expect(
                    throws: DatabaseReadTransactionError
                        .transactionControlUnavailable
                ) {
                    try access.setReadVersion(1)
                }
                #expect(
                    throws: DatabaseReadTransactionError
                        .transactionControlUnavailable
                ) {
                    try access.setOption(
                        forOption: .timeout(milliseconds: 1)
                    )
                }
                await #expect(
                    throws: DatabaseReadTransactionError
                        .versionstampUnavailable
                ) {
                    _ = try await access.requestVersionstamp().value
                }
            }
        }
    }

    @Test("Escaped maintenance access is revoked after its callback")
    func revokesEscapedAccess() async throws {
        let engine = InMemoryEngine()
        let index = Subspace(prefix: Tuple("indexes", "lifetime").pack())
        let key = index.pack(Tuple("entry"))
        let escaped = EscapedIndexMaintenanceAccess()

        try await engine.withTransaction { transaction in
            try await withIndexMaintenanceTransaction(
                transaction: transaction,
                indexSubspace: index
            ) { access in
                try access.setValue([0x03], for: key)
                await escaped.store(access)
            }
        }

        await #expect(throws: DatabaseReadTransactionError.snapshotClosed) {
            _ = try await escaped.read(key)
        }
    }

    @Test("The database root cannot be admitted as an index")
    func rejectsEmptyIndexSubspace() async throws {
        let engine = InMemoryEngine()
        _ = try await engine.withTransaction { transaction in
            await #expect(
                throws: IndexMaintenanceAccessError.invalidIndexSubspace
            ) {
                try await withIndexMaintenanceTransaction(
                    transaction: transaction,
                    indexSubspace: Subspace(prefix: [])
                ) { _ in () }
            }
        }
    }
}
