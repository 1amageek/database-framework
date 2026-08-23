/// Copyable immutable ownership for a retained Array used by fan-out paths.
///
/// Copies share both the COW Array buffer and its reservation. Intermediate
/// consumers receive only scoped borrows or an iterator that retains the
/// owner; raw Array promotion is reserved for the final result boundary.
package struct DatabaseSharedRetainedArray<Element: Sendable>:
    Sendable,
    RandomAccessCollection {
    fileprivate final class Storage: Sendable {
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

    package struct Iterator: IteratorProtocol {
        private let storage: Storage
        private var index: Int

        fileprivate init(storage: Storage) {
            self.storage = storage
            self.index = storage.elements.startIndex
        }

        package mutating func next() -> Element? {
            guard index < storage.elements.endIndex else { return nil }
            let element = storage.elements[index]
            storage.elements.formIndex(after: &index)
            return element
        }
    }

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
    package var startIndex: Int { storage.elements.startIndex }
    package var endIndex: Int { storage.elements.endIndex }

    package func index(after index: Int) -> Int { index + 1 }
    package func index(before index: Int) -> Int { index - 1 }
    package func distance(from start: Int, to end: Int) -> Int { end - start }
    package func index(_ index: Int, offsetBy distance: Int) -> Int {
        index + distance
    }

    package subscript(position: Int) -> Element {
        precondition(
            position >= storage.elements.startIndex
                && position < storage.elements.endIndex
        )
        return storage.elements[position]
    }

    package func makeIterator() -> Iterator {
        Iterator(storage: storage)
    }

    /// Creates a bounded view that retains the same immutable owner and its
    /// request reservation. The range uses this collection's zero-based
    /// indices and cannot outlive the retained storage.
    package func boundedView(
        _ range: Range<Int>
    ) -> DatabaseSharedRetainedArrayView<Element> {
        precondition(
            range.lowerBound >= startIndex && range.upperBound <= endIndex
        )
        return DatabaseSharedRetainedArrayView(owner: self, range: range)
    }

    /// Moves the shared Array value into the final result while allowing any
    /// remaining intermediate aliases to retain their reservation. This is
    /// valid only at a top-level output boundary.
    package consuming func promoteToOutput() -> [Element] {
        storage.elements
    }

    package func withSpan<Result, Failure: Error>(
        _ body: (Span<Element>) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        // Copying the Array value retains the same COW buffer for this
        // synchronous borrow; no element storage is materialized.
        let elements = storage.elements
        return try body(elements.span)
    }

    /// Keeps the immutable owner and its reservation alive while an
    /// asynchronous consumer uses the shared COW Array value. The consumer
    /// cannot outlive this call, so the Array never has an unaccounted
    /// lifetime across suspension.
    package func withElements<Result: Sendable>(
        isolation actor: isolated (any Actor)? = #isolation,
        _ body: (borrowing [Element]) async throws -> Result
    ) async rethrows -> Result {
        try await body(storage.elements)
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
        isolation actor: isolated (any Actor)? = #isolation,
        _ body: (borrowing Element) async throws(Failure) -> Result
    ) async throws(Failure) -> Result {
        precondition(
            index >= storage.elements.startIndex
                && index < storage.elements.endIndex
        )
        return try await body(storage.elements[index])
    }
}

/// A bounded, zero-copy view over immutable request-accounted Array storage.
package struct DatabaseSharedRetainedArrayView<Element: Sendable>:
    Sendable,
    RandomAccessCollection {
    private let owner: DatabaseSharedRetainedArray<Element>
    private let range: Range<Int>

    fileprivate init(
        owner: DatabaseSharedRetainedArray<Element>,
        range: Range<Int>
    ) {
        self.owner = owner
        self.range = range
    }

    package var startIndex: Int { 0 }
    package var endIndex: Int { range.count }

    package func index(after index: Int) -> Int { index + 1 }
    package func index(before index: Int) -> Int { index - 1 }
    package func distance(from start: Int, to end: Int) -> Int { end - start }
    package func index(_ index: Int, offsetBy distance: Int) -> Int {
        index + distance
    }

    package subscript(position: Int) -> Element {
        precondition(position >= startIndex && position < endIndex)
        return owner[range.lowerBound + position]
    }
}
