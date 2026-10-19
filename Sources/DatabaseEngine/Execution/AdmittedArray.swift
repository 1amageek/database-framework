import DatabaseKit

/// An Array paired with the element capacity its owner has admitted.
///
/// Admission follows `DatabaseRetainedArrayLayout` growth, never the capacity
/// the allocator reports, so the same work admits the same bytes on every
/// platform and heap state. Admitted capacity never decreases while the
/// owner's claim lives; removing elements does not refund or re-admit slots.
@propertyWrapper
package struct AdmittedArray<Element> {
    package var wrappedValue: [Element]
    /// Element slots already admitted for `wrappedValue`.
    package var admittedCapacity: Int

    /// Elements present at initialization are admitted by their producer.
    package init(wrappedValue: [Element]) {
        self.wrappedValue = wrappedValue
        admittedCapacity = wrappedValue.count
    }

    package var projectedValue: AdmittedArray<Element> {
        get { self }
        _modify { yield &self }
    }

    /// Admits room for `count` more elements and reserves it.
    ///
    /// `charge` receives the container bytes on first growth plus the bytes of
    /// the newly admitted slots, before storage grows.
    package mutating func admit(
        adding count: Int,
        layout: DatabaseRetainedArrayLayout,
        charge: (UInt64) throws -> Void
    ) throws {
        let next = wrappedValue.count.addingReportingOverflow(count)
        guard count >= 0, !next.overflow else { throw QueryValueError.arithmeticOverflow }
        guard next.partialValue > admittedCapacity else { return }
        let growth = try layout.growth(from: admittedCapacity, toFit: next.partialValue)
        let header = admittedCapacity == 0 ? layout.containerByteCount : 0
        try charge(try DatabaseIntermediateFootprint(bytes: header)
            .adding(DatabaseIntermediateFootprint(bytes: growth.additionalByteCount)).bytes)
        wrappedValue.reserveCapacity(growth.capacity)
        admittedCapacity = growth.capacity
    }
}

extension AdmittedArray: Sendable where Element: Sendable {}
