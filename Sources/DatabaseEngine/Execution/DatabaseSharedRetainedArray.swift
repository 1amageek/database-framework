/// Copyable immutable ownership for a retained Array used by fan-out paths.
///
/// Copies share both the COW Array buffer and its reservation. The raw Array
/// is intentionally never exposed or promoted from shared ownership.
package struct DatabaseSharedRetainedArray<Element: Sendable>: Sendable {
    private final class Storage: Sendable {
        let elements: [Element]
        let elementReservation: DatabaseIntermediateReservation
        let ownerReservation: DatabaseIntermediateReservation

        init(
            elements: consuming [Element],
            elementReservation: DatabaseIntermediateReservation,
            ownerReservation: DatabaseIntermediateReservation
        ) {
            self.elements = elements
            self.elementReservation = elementReservation
            self.ownerReservation = ownerReservation
        }
    }

    private let storage: Storage

    init(
        elements: consuming [Element],
        elementReservation: DatabaseIntermediateReservation,
        ownerReservation: DatabaseIntermediateReservation
    ) {
        self.storage = Storage(
            elements: elements,
            elementReservation: elementReservation,
            ownerReservation: ownerReservation
        )
    }

    package var count: Int { storage.elements.count }
    package var isEmpty: Bool { storage.elements.isEmpty }

    package func withSpan<Result, Failure: Error>(
        _ body: (Span<Element>) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        // Copying the Array value retains the same COW buffer for this
        // synchronous borrow; no element storage is materialized.
        let elements = storage.elements
        return try body(elements.span)
    }

    /// Borrows one element while retaining the immutable shared owner.
    package func withElement<Result, Failure: Error>(
        at index: Int,
        _ body: (borrowing Element) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        precondition(
            index >= storage.elements.startIndex
                && index < storage.elements.endIndex
        )
        return try body(storage.elements[index])
    }

    /// Keeps the immutable shared owner alive across suspension while one
    /// element is borrowed by an asynchronous consumer.
    package func withElement<Result, Failure: Error>(
        at index: Int,
        _ body: (borrowing Element) async throws(Failure) -> Result
    ) async throws(Failure) -> Result {
        precondition(
            index >= storage.elements.startIndex
                && index < storage.elements.endIndex
        )
        return try await body(storage.elements[index])
    }
}
