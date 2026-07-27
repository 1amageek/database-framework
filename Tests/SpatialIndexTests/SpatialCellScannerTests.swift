import DatabaseKit
import DatabaseEngine
import StorageKit
import Testing
@testable import SpatialIndex

@Suite("Spatial cell scanner")
struct SpatialCellScannerTests {
    @Test("Cell scans deduplicate composite identifiers by packed bytes")
    func cellScansDeduplicateCompositeIdentifiers() async throws {
        let engine = InMemoryEngine()
        let indexSubspace = Subspace(prefix: Tuple("spatial-cell-scanner").pack())
        let scanner = SpatialCellScanner(
            indexSubspace: indexSubspace,
            encoding: .s2,
            level: 12
        )
        let firstCell: UInt64 = 10
        let secondCell: UInt64 = 20
        let firstIdentifier = Tuple("tenant-a", Int64(1))
        let secondIdentifier = Tuple("tenant-a", Int64(2))

        try await engine.withTransaction { transaction in
            try transaction.setValue(
                [],
                for: indexSubspace.subspace(Tuple(firstCell)).pack(firstIdentifier)
            )
            try transaction.setValue(
                [],
                for: indexSubspace.subspace(Tuple(secondCell)).pack(firstIdentifier)
            )
            try transaction.setValue(
                [],
                for: indexSubspace.subspace(Tuple(secondCell)).pack(secondIdentifier)
            )
        }

        let keys = try await engine.withTransaction { transaction in
            try await scanner.scanCells(
                cellIds: [firstCell, secondCell],
                limit: nil,
                transaction: transaction
            ).keys
        }

        #expect(keys.count == 2)
        #expect(Set(keys.map { $0.pack() }) == Set([
            firstIdentifier.pack(),
            secondIdentifier.pack()
        ]))
    }

    @Test("Code-range scans reject entries without a primary key")
    func codeRangeScansRejectMissingPrimaryKey() async throws {
        let engine = InMemoryEngine()
        let indexSubspace = Subspace(prefix: Tuple("spatial-code-scanner").pack())
        let scanner = SpatialCellScanner(
            indexSubspace: indexSubspace,
            encoding: .morton,
            level: 12
        )

        try await engine.withTransaction { transaction in
            try transaction.setValue([], for: indexSubspace.pack(Tuple(UInt64(10))))
        }

        await #expect(throws: SpatialCellScannerError.missingPrimaryKey) {
            _ = try await engine.withTransaction { transaction in
                try await scanner.scanCodeRange(
                    minCode: 10,
                    maxCode: 10,
                    limit: nil,
                    transaction: transaction
                )
            }
        }
    }
}
