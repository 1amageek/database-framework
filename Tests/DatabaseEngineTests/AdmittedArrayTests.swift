import Testing
@testable import DatabaseEngine

@Suite("Admitted array capacity")
struct AdmittedArrayTests {
    private func admit(_ array: inout AdmittedArray<Int>, adding count: Int,
        layout: DatabaseRetainedArrayLayout) throws -> UInt64 {
        var charged: UInt64 = 0
        try array.admit(adding: count, layout: layout) { charged += $0 }
        return charged
    }

    @Test("Admission follows layout growth and charges the container once")
    func admissionFollowsLayoutGrowth() throws {
        let layout = try DatabaseRetainedArrayLayout.forElement(Int.self)
        let slot = layout.elementCapacitySlotByteCount
        var array = AdmittedArray<Int>(wrappedValue: [])
        #expect(try admit(&array, adding: 1, layout: layout) == layout.containerByteCount + slot)
        array.wrappedValue.append(1)
        #expect(try admit(&array, adding: 1, layout: layout) == slot)
        array.wrappedValue.append(2)
        #expect(try admit(&array, adding: 1, layout: layout) == 2 * slot)
        array.wrappedValue.append(3)
        #expect(try admit(&array, adding: 1, layout: layout) == 0)
        #expect(array.admittedCapacity == 4)
    }

    @Test("Allocator-reported spare capacity is never charged or consumed")
    func allocatorSpareCapacityIsIgnored() throws {
        let layout = try DatabaseRetainedArrayLayout.forElement(Int.self)
        let slot = layout.elementCapacitySlotByteCount
        var array = AdmittedArray<Int>(wrappedValue: [])
        // Spare storage stands in for allocator rounding that varies by heap state.
        array.wrappedValue.reserveCapacity(64)
        #expect(try admit(&array, adding: 1, layout: layout) == layout.containerByteCount + slot)
        array.wrappedValue.append(1)
        #expect(try admit(&array, adding: 1, layout: layout) == slot)
        #expect(array.admittedCapacity == 2)
    }

    @Test("Removing elements neither refunds nor re-admits slots")
    func removalDoesNotReadmit() throws {
        let layout = try DatabaseRetainedArrayLayout.forElement(Int.self)
        var array = AdmittedArray<Int>(wrappedValue: [])
        _ = try admit(&array, adding: 4, layout: layout)
        array.wrappedValue.append(contentsOf: [1, 2, 3, 4])
        array.wrappedValue.removeAll(keepingCapacity: false)
        #expect(try admit(&array, adding: 4, layout: layout) == 0)
        #expect(array.admittedCapacity == 4)
    }

    @Test("Elements present at initialization are admitted by their producer")
    func initialElementsAreProducerAdmitted() throws {
        let layout = try DatabaseRetainedArrayLayout.forElement(Int.self)
        var array = AdmittedArray<Int>(wrappedValue: [1, 2, 3])
        #expect(array.admittedCapacity == 3)
        #expect(try admit(&array, adding: 1, layout: layout) == 3 * layout.elementCapacitySlotByteCount)
        #expect(array.admittedCapacity == 6)
    }
}
