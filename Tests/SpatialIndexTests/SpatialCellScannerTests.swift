import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import StorageKit
import TestSupport
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
                for: indexSubspace.subspace(firstCell).pack(firstIdentifier)
            )
            try transaction.setValue(
                [],
                for: indexSubspace.subspace(secondCell).pack(firstIdentifier)
            )
            try transaction.setValue(
                [],
                for: indexSubspace.subspace(secondCell).pack(secondIdentifier)
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

    @Test("Retained cell scans do not charge duplicate identifiers")
    func retainedCellScansDoNotChargeDuplicates() async throws {
        let firstIdentifier = Tuple("tenant-a", Int64(1))
        let secondIdentifier = Tuple("tenant-a", Int64(2))

        let baseline = try await retainedCellScanMeasurement(
            entries: [
                (cell: 10, identifier: firstIdentifier),
                (cell: 20, identifier: secondIdentifier),
            ],
            cells: [10, 20],
            limit: nil
        )
        let withDuplicate = try await retainedCellScanMeasurement(
            entries: [
                (cell: 10, identifier: firstIdentifier),
                (cell: 20, identifier: firstIdentifier),
                (cell: 20, identifier: secondIdentifier),
            ],
            cells: [10, 20],
            limit: nil
        )

        #expect(withDuplicate.identifiers == baseline.identifiers)
        #expect(withDuplicate.retainedBytes == baseline.retainedBytes)
        #expect(withDuplicate.peakBytes == baseline.peakBytes)
        #expect(baseline.releasedBytes == 0)
        #expect(withDuplicate.releasedBytes == 0)
    }

    @Test("Retained cell scan sentinel does not consume result memory")
    func retainedCellScanSentinelIsNotRetained() async throws {
        let firstIdentifier = Tuple("tenant-a", Int64(1))
        let secondIdentifier = Tuple("tenant-a", Int64(2))

        let complete = try await retainedCellScanMeasurement(
            entries: [(cell: 10, identifier: firstIdentifier)],
            cells: [10],
            limit: 1
        )
        let truncated = try await retainedCellScanMeasurement(
            entries: [
                (cell: 10, identifier: firstIdentifier),
                (cell: 10, identifier: secondIdentifier),
            ],
            cells: [10],
            limit: 1
        )

        #expect(complete.identifiers == truncated.identifiers)
        #expect(complete.limitReason == nil)
        if case .maxResultsReached(returned: 1, limit: 1) =
            truncated.limitReason {
            // Expected sentinel evidence.
        } else {
            Issue.record("Expected one unretained sentinel result")
        }
        #expect(truncated.retainedBytes == complete.retainedBytes)
        #expect(truncated.peakBytes == complete.peakBytes)
        #expect(complete.releasedBytes == 0)
        #expect(truncated.releasedBytes == 0)
    }

    private func retainedCellScanMeasurement(
        entries: [(cell: UInt64, identifier: Tuple)],
        cells: [UInt64],
        limit: Int?
    ) async throws -> SpatialRetainedScanMeasurement {
        let engine = InMemoryEngine()
        let indexSubspace = Subspace(
            prefix: Tuple("retained-spatial-cell-scanner").pack()
        )
        let scanner = SpatialCellScanner(
            indexSubspace: indexSubspace,
            encoding: .s2,
            level: 12
        )
        try await engine.withTransaction { transaction in
            for entry in entries {
                try transaction.setValue(
                    [],
                    for: indexSubspace
                        .subspace(entry.cell)
                        .pack(entry.identifier)
                )
            }
        }
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: TestProcessMonotonicClock()
        )
        let measurement = try await engine.withTransaction { transaction in
            let result = try await scanner.scanRetained(
                plan: .cells(
                    SpatialCellPlan(cells: cells, reservation: nil)
                ),
                limit: limit,
                transaction: transaction,
                workMeter: meter
            )
            return SpatialRetainedScanMeasurement(
                identifiers: Set(result.keys.map { $0.pack() }),
                limitReason: result.limitReason,
                retainedBytes: meter.retainedIntermediateBytes,
                peakBytes: meter.peakIntermediateBytes,
                releasedBytes: 0
            )
        }
        return SpatialRetainedScanMeasurement(
            identifiers: measurement.identifiers,
            limitReason: measurement.limitReason,
            retainedBytes: measurement.retainedBytes,
            peakBytes: measurement.peakBytes,
            releasedBytes: meter.retainedIntermediateBytes
        )
    }
}

private struct SpatialRetainedScanMeasurement {
    let identifiers: Set<ByteString>
    let limitReason: LimitReason?
    let retainedBytes: UInt64
    let peakBytes: UInt64
    let releasedBytes: UInt64
}
