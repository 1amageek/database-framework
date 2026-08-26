import DatabaseKit
import DatabaseTypes
import StorageKit
import TestSupport
import Testing

@testable import DatabaseEngine

@Suite("Retained footprint contracts")
struct DatabaseRetainedFootprintContractTests {
    @Test("array storage footprint is independent of element values")
    func arrayStorageFootprintMatchesIndependentLayout() throws {
        let count = 3
        let observed = try DatabaseIntermediateCollectionMeter.arrayFootprint(
            count: count,
            element: UInt64.self
        )
        let containerBytes = UInt64(MemoryLayout<[UInt64]>.stride)
        let elementBytes = UInt64(MemoryLayout<UInt64>.stride)
        let expected = DatabaseIntermediateFootprint(
            bytes: containerBytes + elementBytes * UInt64(count)
        )

        #expect(observed == expected)
        try assertExactAdmission(expected, stage: .projection)
    }

    @Test("tuple identifier footprint includes packed bytes and row ownership")
    func tupleIdentifierFootprintMatchesIndependentCalculation() throws {
        let tuples = [Tuple("a"), Tuple("longer-identifier")]
        let meter = makeMeter(maximumIntermediateBytes: 4 * 1_024)
        let reservation = try DatabaseIntermediateCollectionMeter.reserveTuples(
            tuples,
            workMeter: meter,
            stage: .indexScan
        )
        defer { reservation.release() }

        var expectedBytes = UInt64(MemoryLayout<[Tuple]>.stride)
            + UInt64(MemoryLayout<Tuple>.stride) * UInt64(tuples.count)
        for tuple in tuples {
            expectedBytes += 32 + UInt64(tuple.packedByteCount)
        }
        let expectedRows = UInt64(tuples.count)

        #expect(meter.retainedIntermediateRows == expectedRows)
        #expect(meter.retainedIntermediateBytes == expectedBytes)
        try assertExactAdmission(
            DatabaseIntermediateFootprint(
                rows: expectedRows,
                bytes: expectedBytes
            ),
            stage: .indexScan
        )
    }

    @Test("scalar value footprint accounts for its owned string bytes")
    func scalarValueFootprintMatchesIndependentCalculation() throws {
        let value = FieldValue.string("retained-value")
        let meter = makeMeter(maximumIntermediateBytes: 4 * 1_024)
        let observed = try CanonicalRelationalFootprintMeter.valueFootprint(
            of: value,
            workMeter: meter,
            stage: .projection
        )
        let expectedBytes = UInt64(MemoryLayout<FieldValue>.stride + 32)
            + UInt64("retained-value".utf8.count)

        #expect(observed == DatabaseIntermediateFootprint(bytes: expectedBytes))
        #expect(meter.retainedIntermediateBytes == 0)
        try assertExactAdmission(
            DatabaseIntermediateFootprint(bytes: expectedBytes),
            stage: .projection
        )
    }

    @Test("persisted model footprint is independently derived from fields")
    func persistedModelFootprintMatchesIndependentCalculation() throws {
        let fields = [
            try PersistableField(
                number: 1,
                name: "identifier",
                value: .string("item-1")
            ),
            try PersistableField(
                number: 2,
                name: "count",
                value: .int32(7)
            ),
        ]
        let model = try PersistedModel(entity: "RetainedItem", fields: fields)
        let meter = makeMeter(maximumIntermediateBytes: 4 * 1_024)
        let observed = try CanonicalRelationalFootprintMeter.footprint(
            of: model,
            workMeter: meter
        )

        let dictionaryBytes = UInt64(MemoryLayout<[String: FieldValue]>.stride)
        var fieldBytes = dictionaryBytes
        for field in fields {
            fieldBytes += 32
                + UInt64(field.name.utf8.count)
                + independentValueFootprint(field.value)
        }
        let expectedBytes = 64 + fieldBytes + dictionaryBytes + 64

        #expect(
            observed == DatabaseIntermediateFootprint(
                rows: 1,
                bytes: expectedBytes
            )
        )
        #expect(meter.retainedIntermediateBytes == 0)
        try assertExactAdmission(
            DatabaseIntermediateFootprint(rows: 1, bytes: expectedBytes),
            stage: .projection
        )
    }

    @Test("static annotation footprint uses the declared UTF-8 count")
    func staticAnnotationFootprintMatchesIndependentCalculation() throws {
        let existing = DatabaseIntermediateFootprint(rows: 1, bytes: 96)
        let name: StaticString = "rank"
        let value = FieldValue.string("0.95")
        let meter = makeMeter(maximumIntermediateBytes: 4 * 1_024)
        let observed = try CanonicalRelationalFootprintMeter.footprint(
            existing,
            appendingAnnotationNamed: name,
            value: value,
            workMeter: meter
        )
        let annotationBytes = 32
            + UInt64(name.utf8CodeUnitCount)
            + independentValueFootprint(value)
        let expected = DatabaseIntermediateFootprint(
            rows: 1,
            bytes: existing.bytes + annotationBytes
        )

        #expect(observed == expected)
        #expect(meter.retainedIntermediateBytes == 0)
        try assertExactAdmission(expected, stage: .projection)
    }

    @Test("destination admission rejects one byte short and accepts exact retention")
    func destinationAdmissionHasExactBoundary() throws {
        let layout = try DatabaseRetainedArrayLayout.validated(
            containerByteCount: 8,
            elementCapacitySlotByteCount: 4,
            sharedOwnerByteCount: 4,
            appendAdmissionByteCount: 3
        )
        let payloadBytes: UInt64 = 5
        let retainedBytes = layout.containerByteCount
            + layout.elementCapacitySlotByteCount
            + payloadBytes

        do {
            let meter = makeMeter(
                maximumIntermediateBytes: retainedBytes
                    + layout.appendAdmissionByteCount
                    - 1
            )
            var builder = try DatabaseRetainedArrayBuilder<Int>(
                workMeter: meter,
                stage: .projection,
                layout: layout
            )
            #expect(throws: DatabaseWorkLimitError.self) {
                try builder.append(
                    footprint: DatabaseIntermediateFootprint(
                        rows: 1,
                        bytes: payloadBytes
                    )
                ) {
                    1
                }
            }
            let builderIsEmpty = builder.isEmpty
            #expect(builderIsEmpty)
            #expect(meter.retainedIntermediateBytes == layout.containerByteCount)
        }

        do {
            let meter = makeMeter(
                maximumIntermediateBytes: retainedBytes
                    + layout.appendAdmissionByteCount
            )
            var builder = try DatabaseRetainedArrayBuilder<Int>(
                workMeter: meter,
                stage: .projection,
                layout: layout
            )
            try builder.append(
                footprint: DatabaseIntermediateFootprint(
                    rows: 1,
                    bytes: payloadBytes
                )
            ) {
                1
            }
            let retained = builder.finish()
            #expect(meter.retainedIntermediateBytes == retainedBytes)
            #expect(retained.count == 1)
        }
    }

    private func makeMeter(maximumIntermediateBytes: UInt64) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateRows: 16,
                maximumIntermediateBytes: maximumIntermediateBytes
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    private func assertExactAdmission(
        _ footprint: DatabaseIntermediateFootprint,
        stage: DatabaseWorkStage
    ) throws {
        precondition(footprint.bytes > 0)
        let shortMeter = makeMeter(
            maximumIntermediateBytes: footprint.bytes - 1
        )
        #expect(throws: DatabaseWorkLimitError.self) {
            try shortMeter.reserveIntermediate(
                rows: footprint.rows,
                bytes: footprint.bytes,
                at: stage
            )
        }
        #expect(shortMeter.retainedIntermediateBytes == 0)

        let exactMeter = makeMeter(maximumIntermediateBytes: footprint.bytes)
        let reservation = try exactMeter.reserveIntermediate(
            rows: footprint.rows,
            bytes: footprint.bytes,
            at: stage
        )
        #expect(exactMeter.retainedIntermediateRows == footprint.rows)
        #expect(exactMeter.retainedIntermediateBytes == footprint.bytes)
        reservation.release()
        #expect(exactMeter.retainedIntermediateRows == 0)
        #expect(exactMeter.retainedIntermediateBytes == 0)
    }

    private func independentValueFootprint(_ value: FieldValue) -> UInt64 {
        let base = UInt64(MemoryLayout<FieldValue>.stride + 32)
        switch value {
        case .string(let string):
            return base + UInt64(string.utf8.count)
        case .int32:
            return base
        default:
            preconditionFailure("Test fixture only covers scalar string and int32 values")
        }
    }
}
