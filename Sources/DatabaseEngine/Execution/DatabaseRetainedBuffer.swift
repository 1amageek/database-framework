/// Unique request-scoped intermediate Array ownership.
///
/// The noncopyable owner keeps the Array and its memory reservation linear.
/// Nested operators borrow a nonescapable Span. A consuming top-level
/// promotion can therefore move the Array buffer and release the reservation
/// without leaving an untracked alias.
package struct DatabaseRetainedBuffer<Element: Sendable>: ~Copyable, Sendable {
    private var elements: [Element]
    private let reservation: DatabaseIntermediateReservation
    private let layout: DatabaseRetainedArrayLayout
    private let accountedCapacity: Int

    init(
        elements: consuming [Element],
        reservation: DatabaseIntermediateReservation,
        layout: DatabaseRetainedArrayLayout,
        accountedCapacity: Int
    ) {
        self.elements = elements
        self.reservation = reservation
        self.layout = layout
        self.accountedCapacity = accountedCapacity
    }

    package var count: Int { elements.count }
    package var isEmpty: Bool { elements.isEmpty }

    package borrowing func withSpan<Result, Failure: Error>(
        _ body: (Span<Element>) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        try body(elements.span)
    }

    /// Borrows one element without exposing the backing Array or creating an
    /// escapable element copy.
    package borrowing func withElement<Result, Failure: Error>(
        at index: Int,
        _ body: (borrowing Element) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        precondition(index >= elements.startIndex && index < elements.endIndex)
        return try body(elements[index])
    }

    /// Keeps this unique owner alive while an asynchronous consumer borrows
    /// one element. The borrowed element cannot escape the borrow scope.
    package borrowing func withElement<Result, Failure: Error>(
        at index: Int,
        _ body: (borrowing Element) async throws(Failure) -> Result
    ) async throws(Failure) -> Result {
        precondition(index >= elements.startIndex && index < elements.endIndex)
        return try await body(elements[index])
    }

    /// Reorders the unique Array in place using a verified permutation. The
    /// caller owns the scratch reservation for the two index maps allocated by
    /// this operation.
    package consuming func reorderingElements(
        destinationOriginalIndex: (Int) -> Int
    ) -> DatabaseRetainedBuffer<Element> {
        var originalAtPosition = Array(elements.indices)
        var positionOfOriginal = Array(elements.indices)
        for destination in elements.indices {
            let desiredOriginal = destinationOriginalIndex(destination)
            precondition(
                desiredOriginal >= elements.startIndex
                    && desiredOriginal < elements.endIndex,
                "Reorder permutation contains an invalid source index"
            )
            let currentPosition = positionOfOriginal[desiredOriginal]
            guard currentPosition != destination else { continue }

            let displacedOriginal = originalAtPosition[destination]
            elements.swapAt(destination, currentPosition)
            originalAtPosition.swapAt(destination, currentPosition)
            positionOfOriginal[desiredOriginal] = destination
            positionOfOriginal[displacedOriginal] = currentPosition
        }
        return consume self
    }

    /// Stable-compacts the unique Array without allocating another element
    /// buffer. Included elements preserve their original relative order.
    package consuming func stableCompacting<Failure: Error>(
        _ isIncluded: (borrowing Element) throws(Failure) -> Bool
    ) throws(Failure) -> DatabaseRetainedBuffer<Element> {
        var writeIndex = elements.startIndex
        for readIndex in elements.indices {
            guard try isIncluded(elements[readIndex]) else { continue }
            if writeIndex != readIndex {
                elements.swapAt(writeIndex, readIndex)
            }
            elements.formIndex(after: &writeIndex)
        }
        if writeIndex < elements.endIndex {
            elements.removeLast(
                elements.distance(from: writeIndex, to: elements.endIndex)
            )
        }
        return consume self
    }

    /// Rewrites element values in place without allowing structural Array
    /// mutation. The retained element count and capacity claims are unchanged.
    package consuming func transformingElements(
        _ transform: (inout Element) -> Void
    ) -> DatabaseRetainedBuffer<Element> {
        for index in elements.indices {
            transform(&elements[index])
        }
        return consume self
    }

    /// Sorts the unique buffer in place with O(1) auxiliary storage.
    ///
    /// Heap sort is used deliberately: request accounting must not depend on
    /// an opaque standard-library scratch allocation proportional to input.
    package consuming func sortingElements<Failure: Error>(
        by areInIncreasingOrder: (
            borrowing Element,
            borrowing Element
        ) throws(Failure) -> Bool
    ) throws(Failure) -> DatabaseRetainedBuffer<Element> {
        if elements.count > 1 {
            try Self.heapSort(
                &elements,
                by: areInIncreasingOrder
            )
        }
        return consume self
    }

    private static func heapSort<Failure: Error>(
        _ elements: inout [Element],
        by areInIncreasingOrder: (
            borrowing Element,
            borrowing Element
        ) throws(Failure) -> Bool
    ) throws(Failure) {
        var start = elements.count / 2
        while start > 0 {
            start -= 1
            try siftDown(
                &elements,
                from: start,
                through: elements.count - 1,
                by: areInIncreasingOrder
            )
        }
        var end = elements.count - 1
        while end > 0 {
            elements.swapAt(0, end)
            end -= 1
            try siftDown(
                &elements,
                from: 0,
                through: end,
                by: areInIncreasingOrder
            )
        }
    }

    private static func siftDown<Failure: Error>(
        _ elements: inout [Element],
        from start: Int,
        through end: Int,
        by areInIncreasingOrder: (
            borrowing Element,
            borrowing Element
        ) throws(Failure) -> Bool
    ) throws(Failure) {
        var root = start
        while true {
            let leftChild = root * 2 + 1
            guard leftChild <= end else { return }
            var selectedChild = leftChild
            let rightChild = leftChild + 1
            if rightChild <= end,
               try areInIncreasingOrder(
                   elements[selectedChild],
                   elements[rightChild]
               ) {
                selectedChild = rightChild
            }
            guard try areInIncreasingOrder(
                elements[root],
                elements[selectedChild]
            ) else {
                return
            }
            elements.swapAt(root, selectedChild)
            root = selectedChild
        }
    }

    /// Removes adjacent duplicates after canonical sorting without allocating
    /// another element buffer. The retained reservation remains conservative
    /// until the final output promotion.
    package consuming func removingAdjacentDuplicates<Failure: Error>(
        by areEquivalent: (
            borrowing Element,
            borrowing Element
        ) throws(Failure) -> Bool
    ) throws(Failure) -> DatabaseRetainedBuffer<Element> {
        guard elements.count > 1 else { return consume self }
        var writeIndex = 1
        for readIndex in 1..<elements.count {
            guard try !areEquivalent(
                elements[writeIndex - 1],
                elements[readIndex]
            ) else {
                continue
            }
            if writeIndex != readIndex {
                elements.swapAt(writeIndex, readIndex)
            }
            writeIndex += 1
        }
        if writeIndex < elements.count {
            elements.removeLast(elements.count - writeIndex)
        }
        return consume self
    }

    /// Moves one visible range to the front and drops the suffix in place.
    /// Array capacity is intentionally retained so no second element buffer is
    /// allocated and downstream appends may reuse the admitted capacity.
    package consuming func retainingSubrange(
        _ range: Range<Int>
    ) -> DatabaseRetainedBuffer<Element> {
        precondition(
            range.lowerBound >= elements.startIndex
                && range.upperBound <= elements.endIndex
        )
        guard range.count != elements.count else { return consume self }
        guard !range.isEmpty else {
            elements.removeAll(keepingCapacity: true)
            return consume self
        }
        for destinationOffset in 0..<range.count {
            let sourceIndex = range.lowerBound + destinationOffset
            if destinationOffset != sourceIndex {
                elements.swapAt(destinationOffset, sourceIndex)
            }
        }
        elements.removeLast(elements.count - range.count)
        return consume self
    }

    /// Releases row and payload claims for elements removed in place while
    /// retaining container and capacity claims owned by the same Array.
    package consuming func releasingRetainedFootprint(
        _ footprint: DatabaseIntermediateFootprint
    ) throws -> DatabaseRetainedBuffer<Element> {
        try reservation.releasePartial(
            rows: footprint.rows,
            bytes: footprint.bytes
        )
        return consume self
    }

    /// Moves the unique COW buffer into final output and removes it from the
    /// intermediate ledger. This operation is valid only at the top-level
    /// result boundary.
    package consuming func promoteToOutput() -> [Element] {
        reservation.release()
        return elements
    }

    #if DATABASE_MULTI_BASE
    /// Ends this unique intermediate owner without promoting its storage to
    /// an output boundary. This is used after a replacement representation
    /// has been admitted and constructed under its own reservation.
    package consuming func discard() {
        reservation.release()
    }
    #endif

    /// Moves the Array into an owner that continues to hold its request-memory
    /// reservation. This is used when a package extension point must return an
    /// owned collection to the canonical dispatcher without creating an
    /// unmetered interval between producer and consumer.
    package consuming func moveRetainingReservation() -> (
        elements: [Element],
        reservation: DatabaseIntermediateReservation
    ) {
        (elements, reservation)
    }

    /// Reopens a unique retained buffer for additional admitted appends. The
    /// Array and reservation move into the builder without materialization.
    package consuming func resumeBuilding(
        at stage: DatabaseWorkStage
    ) -> DatabaseRetainedArrayBuilder<Element> {
        DatabaseRetainedArrayBuilder(
            elements: elements,
            reservation: reservation,
            defaultStage: stage,
            layout: layout,
            accountedCapacity: accountedCapacity
        )
    }

    /// Reserves the shared-owner allocation before this unique buffer is
    /// consumed. If admission fails, the unique buffer remains available.
    package borrowing func prepareToShare(
        at stage: DatabaseWorkStage
    ) throws -> DatabaseRetainedShareAdmission<Element> {
        let ownerReservation = try reservation.reserveChild(
            bytes: layout.sharedOwnerByteCount,
            at: stage
        )
        return DatabaseRetainedShareAdmission(
            sourceReservation: reservation,
            ownerReservation: ownerReservation
        )
    }

    /// Moves the unique buffer into immutable shared ownership for caches or
    /// other fan-out paths without copying its element storage.
    package consuming func share(
        using admission: consuming DatabaseRetainedShareAdmission<Element>
    ) -> DatabaseSharedRetainedArray<Element> {
        let ownerReservation = admission.commit(to: reservation)
        return DatabaseSharedRetainedArray(
            elements: elements,
            elementReservation: reservation,
            ownerReservation: ownerReservation
        )
    }
}
