import Testing

@testable import DatabaseEngine

@Suite("Retained hash-table layout")
struct DatabaseRetainedHashTableLayoutTests {
    @Test("growth uses deterministic power-of-two capacity")
    func deterministicGrowth() throws {
        let layout = try DatabaseRetainedHashTableLayout.validated(
            containerByteCount: 64,
            elementCapacitySlotByteCount: 24
        )

        let growth = try layout.growth(from: 0, toFit: 3)
        #expect(growth.capacity == 4)
        #expect(growth.additionalByteCount == 96)

        let unchanged = try layout.growth(from: 4, toFit: 4)
        #expect(unchanged.capacity == 4)
        #expect(unchanged.additionalByteCount == 0)
    }

    @Test("invalid layout dimensions fail explicitly")
    func rejectsInvalidDimensions() {
        #expect(
            throws: DatabaseRetainedHashTableLayoutError
                .zeroContainerByteCount
        ) {
            try DatabaseRetainedHashTableLayout.validated(
                containerByteCount: 0,
                elementCapacitySlotByteCount: 1
            )
        }
        #expect(
            throws: DatabaseRetainedHashTableLayoutError
                .zeroElementCapacitySlotByteCount
        ) {
            try DatabaseRetainedHashTableLayout.validated(
                containerByteCount: 1,
                elementCapacitySlotByteCount: 0
            )
        }
    }

    @Test("invalid growth and arithmetic overflow fail explicitly")
    func rejectsInvalidGrowthAndOverflow() throws {
        let layout = try DatabaseRetainedHashTableLayout.validated(
            containerByteCount: 64,
            elementCapacitySlotByteCount: 24
        )
        #expect(
            throws: DatabaseRetainedHashTableLayoutError
                .invalidCurrentCapacity(-1)
        ) {
            try layout.growth(from: -1, toFit: 0)
        }
        #expect(
            throws: DatabaseRetainedHashTableLayoutError
                .invalidRequiredCount(-1)
        ) {
            try layout.growth(from: 0, toFit: -1)
        }

        let overflowingCapacity = Int.max / 2 + 1
        #expect(
            throws: DatabaseRetainedHashTableLayoutError.capacityOverflow(
                currentCapacity: overflowingCapacity
            )
        ) {
            try layout.growth(
                from: overflowingCapacity,
                toFit: Int.max
            )
        }

        let overflowingBytes = try DatabaseRetainedHashTableLayout.validated(
            containerByteCount: 1,
            elementCapacitySlotByteCount: UInt64.max
        )
        #expect(
            throws: DatabaseIntermediateFootprintError
                .byteMultiplicationOverflow(
                    value: UInt64.max,
                    multiplier: 2
                )
        ) {
            try overflowingBytes.growth(from: 0, toFit: 2)
        }
    }
}
