import DatabaseEngine
import DatabaseKit
import DatabaseTypes

enum SHACLRetainedTermStorageError: Error, Sendable, Equatable {
    case capacityOverflow
}

/// Immutable, request-accounted RDF terms retained between SHACL operators.
struct SHACLRetainedTerms: Sendable, RandomAccessCollection {
    private let elements: DatabaseSharedRetainedArray<RDFTerm>

    init(elements: DatabaseSharedRetainedArray<RDFTerm>) {
        self.elements = elements
    }

    static func empty(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .deduplication
    ) throws -> SHACLRetainedTerms {
        SHACLRetainedTerms(
            elements: try DatabaseSharedRetainedArray<RDFTerm>.empty(
                workMeter: workMeter,
                stage: stage
            )
        )
    }

    var startIndex: Int { elements.startIndex }
    var endIndex: Int { elements.endIndex }
    var count: Int { elements.count }
    var isEmpty: Bool { elements.isEmpty }

    func index(after index: Int) -> Int { elements.index(after: index) }
    func index(before index: Int) -> Int { elements.index(before: index) }
    func distance(from start: Int, to end: Int) -> Int {
        elements.distance(from: start, to: end)
    }
    func index(_ index: Int, offsetBy distance: Int) -> Int {
        elements.index(index, offsetBy: distance)
    }

    subscript(position: Int) -> RDFTerm { elements[position] }

    func contains(_ term: RDFTerm) -> Bool {
        var lower = startIndex
        var upper = endIndex
        while lower < upper {
            let middle = lower + (upper - lower) / 2
            let candidate = elements[middle]
            if candidate < term {
                lower = middle + 1
            } else {
                upper = middle
            }
        }
        return lower < endIndex && elements[lower] == term
    }

    func hasSameMembers(as other: SHACLRetainedTerms) -> Bool {
        guard count == other.count else { return false }
        for index in 0..<count where elements[index] != other.elements[index] {
            return false
        }
        return true
    }
}

/// Builds a canonical term set while accounting both the hash sidecar and the
/// retained output owner before either grows.
struct SHACLRetainedTermSetBuilder: ~Copyable {
    private static let setContainerByteCount: UInt64 = 64
    private static let setCapacitySlotByteCount: UInt64 = 96

    private var membership: Set<RDFTerm>
    private let membershipReservation: DatabaseIntermediateReservation
    private var accountedMembershipCapacity: Int
    private var elements: DatabaseRetainedArrayBuilder<RDFTerm>
    private let footprintMeter: SPARQLBindingFootprintMeter
    private let workMeter: DatabaseWorkMeter
    private let stage: DatabaseWorkStage

    init(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .deduplication,
        expectedCount: Int = 0
    ) throws {
        let targetCapacity = try Self.targetCapacity(
            current: 0,
            requiredCount: expectedCount
        )
        let capacityBytes = try Self.checkedMultiply(
            UInt64(targetCapacity),
            Self.setCapacitySlotByteCount
        )
        let membershipReservation = try workMeter.reserveIntermediate(
            bytes: try Self.checkedAdd(
                Self.setContainerByteCount,
                capacityBytes
            ),
            at: stage
        )
        let elements = try DatabaseRetainedArrayBuilder<RDFTerm>(
            workMeter: workMeter,
            stage: stage,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: RDFTerm.self),
            expectedCount: expectedCount
        )
        let footprintMeter = try SPARQLBindingFootprintMeter.make(
            workMeter: workMeter,
            stage: stage
        )
        var membership: Set<RDFTerm> = []
        membership.reserveCapacity(targetCapacity)
        self.membership = membership
        self.membershipReservation = membershipReservation
        self.accountedMembershipCapacity = targetCapacity
        self.elements = consume elements
        self.footprintMeter = footprintMeter
        self.workMeter = workMeter
        self.stage = stage
    }

    var count: Int { membership.count }

    mutating func insert(_ term: borrowing RDFTerm) throws {
        guard !membership.contains(term) else { return }

        let footprint = try footprintMeter.footprint(
            of: FieldValue.rdfTerm(copy term)
        )
        let appendAdmission = try elements.prepareAppend(
            footprint: footprint,
            at: stage
        )
        let requiredCount = try Self.checkedIncrement(membership.count)
        let targetCapacity = try Self.targetCapacity(
            current: accountedMembershipCapacity,
            requiredCount: requiredCount
        )
        if targetCapacity != accountedMembershipCapacity {
            let additionalBytes = try Self.checkedMultiply(
                UInt64(targetCapacity - accountedMembershipCapacity),
                Self.setCapacitySlotByteCount
            )
            try membershipReservation.reserveAdditional(
                bytes: additionalBytes,
                at: stage
            )
            membership.reserveCapacity(targetCapacity)
            accountedMembershipCapacity = targetCapacity
        }

        let inserted = membership.insert(copy term).inserted
        precondition(inserted, "SHACL term membership changed during insertion")
        elements.append(copy term, using: appendAdmission)
    }

    mutating func formUnion(_ terms: SHACLRetainedTerms) throws {
        for term in terms {
            try insert(term)
        }
    }

    mutating func formUnion(_ terms: Set<RDFTerm>) throws {
        for term in terms.sorted() {
            try insert(term)
        }
    }

    consuming func finish() throws -> SHACLRetainedTerms {
        let sorted = elements.finish().sortingElements(by: <)
        let shared = try sorted.moveToSharedOwnership(at: stage)
        footprintMeter.shutdown()
        membershipReservation.release()
        return SHACLRetainedTerms(elements: shared)
    }

    private static func targetCapacity(
        current: Int,
        requiredCount: Int
    ) throws -> Int {
        guard requiredCount > current else { return current }
        var capacity = max(1, current)
        while capacity < requiredCount {
            let (next, overflow) = capacity.multipliedReportingOverflow(by: 2)
            guard !overflow else {
                throw SHACLRetainedTermStorageError.capacityOverflow
            }
            capacity = next
        }
        return capacity
    }

    private static func checkedIncrement(_ value: Int) throws -> Int {
        let (result, overflow) = value.addingReportingOverflow(1)
        guard !overflow else {
            throw SHACLRetainedTermStorageError.capacityOverflow
        }
        return result
    }

    private static func checkedMultiply(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = left.multipliedReportingOverflow(by: right)
        guard !overflow else {
            throw SHACLRetainedTermStorageError.capacityOverflow
        }
        return result
    }

    private static func checkedAdd(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = left.addingReportingOverflow(right)
        guard !overflow else {
            throw SHACLRetainedTermStorageError.capacityOverflow
        }
        return result
    }
}
