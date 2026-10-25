import DatabaseKit
import StorageKit
import StorageKitSystemClock
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("SQL relation row ownership", .timeLimit(.minutes(1)))
struct SQLRelationRowOwnershipTests {
    @Test("A pull of retained input shares its owner without a new row container claim")
    func retainedInputDoesNotRebox() async throws {
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 100,
            maximumWorkUnits: 100000, maximumIntermediateBytes: 1024 * 1024,
            timeoutMilliseconds: 30000), monotonicClock: SystemStorageClock())
        let schema = try CanonicalRelationSchema(unscopedColumns: ["n"],
            types: [.sql(.bigint)], nullability: [false])
        var builder = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(workMeter: meter,
            stage: .bindingCandidate, layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self))
        let value = CanonicalSourceRow(values: [.primitive(.int64(7))], layout: schema.layout)
        try builder.append(footprint: CanonicalRelationalFootprintMeter.sourceAliasFootprint(of: value,
            workMeter: meter, stage: .bindingCandidate), make: { value })
        let relation = CanonicalRelation(schema: schema,
            rows: try builder.finish().moveToSharedOwnership(at: .bindingCandidate))
        let producer = SQLMaterializedRelationProducer(schema: schema, meter: meter) { relation }
        let bytes = meter.retainedIntermediateBytes, rows = meter.retainedIntermediateRows
        let output = try #require(try await producer.next())
        #expect(meter.retainedIntermediateBytes == bytes)
        #expect(meter.retainedIntermediateRows == rows)
        try await producer.close()
        withExtendedLifetime(output) {
            #expect(meter.retainedIntermediateBytes >= bytes)
        }
    }
    @Test("Computed rows reject foreign claims and release their own claim")
    func computedClaimLifetime() throws {
        let meter = makeMeter(), foreign = makeMeter()
        let schema = try CanonicalRelationSchema(unscopedColumns: ["n"], types: [.sql(.bigint)])
        do {
            let output = try SQLRelationRow.producing(CanonicalSourceRow(values: [.primitive(.int64(9))],
                layout: schema.layout), schema: schema, meter: meter)
            #expect(output.value.values == [.primitive(.int64(9))])
            #expect(meter.retainedIntermediateRows > 0)
            withExtendedLifetime(output) {}
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
        let claim = try foreign.reserveIntermediate(bytes: 16, at: .bindingCandidate)
        let row = CanonicalSourceRow(values: [.primitive(.int64(9))], layout: schema.layout,
            payloadLeases: [DatabaseQueryPayloadLease(retaining: claim)])
        #expect(throws: DatabaseIntermediateReservationError.workMeterMismatch) {
            try SQLRelationRow.producing(row, schema: schema, meter: meter)
        }
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("ORDER metadata shares the same layout and changes for distinct physical layouts")
    func orderingDescriptorLifetime() throws {
        let meter = makeMeter()
        let first = try CanonicalRelationSchema(unscopedColumns: ["n", "n"],
            types: [.sql(.integer), .sql(.bigint)], nullability: [false, true])
        let second = try CanonicalRelationSchema(unscopedColumns: ["n"], types: [.sql(.text)])
        do {
            var columns = CanonicalOrderColumns()
            let a = try columns.columns(for: first.layout, meter: meter)
            let bytes = meter.retainedIntermediateBytes
            let b = try columns.columns(for: first.layout, meter: meter)
            #expect(a == b)
            #expect(meter.retainedIntermediateBytes == bytes)
            #expect(a.map(\.number) == [1, 2])
            let c = try columns.columns(for: second.layout, meter: meter)
            #expect(c.count == 1 && c[0].type == .sql(.text))
            withExtendedLifetime(columns) {}
        }
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("A failed descriptor or invalid retained index releases admission")
    func invalidDescriptorAndPosition() throws {
        let meter = makeMeter()
        var columns = CanonicalOrderColumns()
        #expect(throws: (any Error).self) {
            try columns.columns(for: CanonicalSourceLayout(slots: [.init(name: "missing", scope: nil)]), meter: meter)
        }
        #expect(meter.retainedIntermediateBytes == 0)
        let builder = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(workMeter: meter,
            stage: .bindingCandidate, layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self))
        let rows = try builder.finish().moveToSharedOwnership(at: .bindingCandidate)
        #expect(throws: (any Error).self) {
            try SQLRelationRow(retaining: rows, at: 0, schema: CanonicalRelationSchema())
        }
    }

    private func makeMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 100,
            maximumWorkUnits: 100000, maximumIntermediateBytes: 1024 * 1024,
            timeoutMilliseconds: 30000), monotonicClock: SystemStorageClock())
    }

}
