import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import DatabaseWire
import TestSupport
import Testing
@testable import GraphIndex

@Suite("SPARQL binding prospective footprint")
struct SPARQLBindingFootprintMeterTests {
    @Test("canonical row charges exclude the outer retained Array slot")
    func canonicalRowCharges() throws {
        let meter = try makeFootprintMeter()
        defer { meter.shutdown() }

        #expect(
            try meter.footprint(of: VariableBinding())
                == DatabaseIntermediateFootprint(rows: 1, bytes: 64)
        )
        #expect(
            try meter.footprint(
                of: VariableBinding(["?x": .int64(1)])
            ) == DatabaseIntermediateFootprint(rows: 1, bytes: 210)
        )
        #expect(
            try meter.footprint(
                of: VariableBinding(["?x": .string("abc")])
            ) == DatabaseIntermediateFootprint(rows: 1, bytes: 229)
        )
    }

    @Test("a byte slice charges its complete retained backing owner")
    func byteSliceChargesCompleteOwner() throws {
        let meter = try makeFootprintMeter()
        defer { meter.shutdown() }
        let backing = [UInt8](repeating: 7, count: 4_096)
        let bytes = ByteString(backing)[0..<1]
        let retainedByteCount = try #require(bytes.retainedByteCount)

        #expect(
            try meter.footprint(
                of: VariableBinding(["?x": .bytes(bytes)])
            ) == DatabaseIntermediateFootprint(
                rows: 1,
                bytes: UInt64(retainedByteCount) + 226
            )
        )
    }

    @Test("unknown retained byte storage fails admission explicitly")
    func unknownRetainedByteStorageFailsAdmission() throws {
        let meter = try makeFootprintMeter()
        defer { meter.shutdown() }
        let bytes = ByteString(
            retaining: UnknownRetainedByteOwner(bytes: [1, 2, 3])
        )

        #expect(throws: SPARQLBindingFootprintError.unknownByteStringRetainedSize) {
            try meter.footprint(
                of: VariableBinding(["?x": .bytes(bytes)])
            )
        }
    }

    @Test("join preflight matches the materialized left-biased row")
    func joinPreflightMatchesMaterializedRow() throws {
        let meter = try makeFootprintMeter()
        defer { meter.shutdown() }
        let backing = [UInt8](repeating: 9, count: 4_096)
        let left = VariableBinding([
            "?payload": .bytes(
                ByteString(backing)[0..<1]
            ),
        ])
        let right = VariableBinding([
            "?payload": .bytes(ByteString([9])),
            "?name": .string("row"),
        ])
        let materialized = try #require(left.merged(with: right))

        let prospective = try meter.footprint(
            merging: left,
            with: right
        )
        let materializedFootprint = try meter.footprint(of: materialized)

        #expect(prospective == .compatible(materializedFootprint))
    }

    @Test("join conflict is detected before row construction")
    func joinConflictIsIncompatible() throws {
        let meter = try makeFootprintMeter()
        defer { meter.shutdown() }

        #expect(
            try meter.footprint(
                merging: VariableBinding(["?x": .int64(1)]),
                with: VariableBinding(["?x": .int64(2)])
            ) == .incompatible
        )
    }

    @Test("VALUES preflight charges only the left-biased union")
    func valuesPreflightMatchesMaterializedRow() throws {
        let meter = try makeFootprintMeter()
        defer { meter.shutdown() }
        let seed = VariableBinding([
            "?x": .int64(1),
            "?seed": .string("kept"),
        ])
        let table = SPARQLValuesTable(
            variables: ["?x", "?undefined", "?new"],
            rowCount: 1,
            cells: [.int64(1), nil, .string("added")]
        )
        var materialized = seed
        let mergedExisting = materialized.merge(
            variable: "?x",
            value: .int64(1)
        )
        let mergedNew = materialized.merge(
            variable: "?new",
            value: .string("added")
        )
        #expect(mergedExisting)
        #expect(mergedNew)

        let prospective = try meter.footprint(
            extending: seed,
            with: table,
            row: 0
        )

        #expect(
            prospective == .compatible(
                try meter.footprint(of: materialized)
            )
        )
    }

    @Test("VALUES conflict does not produce a candidate footprint")
    func valuesConflictIsIncompatible() throws {
        let meter = try makeFootprintMeter()
        defer { meter.shutdown() }
        let table = SPARQLValuesTable(
            variables: ["?x"],
            rowCount: 1,
            cells: [.int64(2)]
        )

        #expect(
            try meter.footprint(
                extending: VariableBinding(["?x": .int64(1)]),
                with: table,
                row: 0
            ) == .incompatible
        )
    }

    @Test("BIND preflight matches the exact extended row")
    func bindPreflightMatchesExtendedRow() throws {
        let meter = try makeFootprintMeter()
        defer { meter.shutdown() }
        let seed = VariableBinding(["?x": .int64(1)])
        let materialized = seed.binding("?label", to: .string("value"))

        #expect(
            try meter.footprint(
                extending: seed,
                variable: "?label",
                value: .string("value")
            ) == .compatible(
                try meter.footprint(of: materialized)
            )
        )
        #expect(
            try meter.footprint(
                extending: seed,
                variable: "?x",
                value: .int64(2)
            ) == .incompatible
        )
    }

    @Test("scratch rejection leaves the meter reusable and typed")
    func scratchRejectionLeavesMeterReusable() throws {
        let workMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 10,
                maximumWorkUnits: 10,
                maximumIntermediateRows: 10,
                maximumIntermediateBytes: 500,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        let footprintMeter = try SPARQLBindingFootprintMeter.make(
            workMeter: workMeter,
            stage: .joinCandidate
        )
        defer { footprintMeter.shutdown() }
        let deeplyNestedValue = makeDeeplyNestedArray(depth: 8)

        #expect {
            try footprintMeter.footprint(
                of: VariableBinding(["?value": deeplyNestedValue])
            )
        } throws: { error in
            error as? DatabaseWorkLimitError
                == .maximumIntermediateBytes(
                    stage: .joinCandidate,
                    consumed: 320,
                    requested: 256,
                    maximum: 500
                )
        }
        #expect(
            try footprintMeter.footprint(
                of: VariableBinding(["?x": .int64(1)])
            ) == DatabaseIntermediateFootprint(rows: 1, bytes: 210)
        )
        #expect(workMeter.retainedIntermediateBytes == 320)
    }

    @Test("shutdown releases all admitted traversal scratch")
    func shutdownReleasesScratch() throws {
        let workMeter = makeWorkMeter()
        let footprintMeter = try SPARQLBindingFootprintMeter.make(
            workMeter: workMeter,
            stage: .joinCandidate
        )
        _ = try footprintMeter.footprint(
            of: VariableBinding(["?x": .string("value")])
        )

        #expect(workMeter.retainedIntermediateBytes == 320)
        footprintMeter.shutdown()
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    private func makeFootprintMeter() throws -> SPARQLBindingFootprintMeter {
        try SPARQLBindingFootprintMeter.make(
            workMeter: makeWorkMeter(),
            stage: .joinCandidate
        )
    }

    private func makeWorkMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 100,
                maximumWorkUnits: 100,
                maximumIntermediateRows: 100,
                maximumIntermediateBytes: 1_000_000,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    private func makeDeeplyNestedArray(
        depth: Int
    ) -> FieldValue {
        var value = FieldValue.null
        for _ in 0..<depth {
            value = .array([value, .null])
        }
        return value
    }

    private struct UnknownRetainedByteOwner: ByteStringOwner {
        let bytes: [UInt8]

        var count: Int { bytes.count }

        func borrowBytes(
            _ body: (UnsafeRawBufferPointer) throws -> Void
        ) rethrows {
            try bytes.withUnsafeBytes(body)
        }
    }
}
