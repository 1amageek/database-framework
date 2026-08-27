import DatabaseTypes

/// A noncopyable scalar view over one synchronous vector-element borrow.
///
/// The underlying buffer pointer is intentionally private. Callers can inspect
/// scalar values without acquiring an address that could outlive the retained
/// model owner.
package struct DatabaseRetainedVectorElements<Element>: ~Copyable {
    private let elements: UnsafeBufferPointer<Element>

    fileprivate init(_ elements: UnsafeBufferPointer<Element>) {
        self.elements = elements
    }

    package var count: Int { elements.count }

    package subscript(index: Int) -> Element {
        precondition(elements.indices.contains(index))
        return elements[index]
    }
}

/// Noncopyable access to one retained model vector field.
///
/// The view exposes only synchronous scalar borrows. It does not expose the
/// copyable `Vector`, a raw pointer, its model, or the reservation that owns
/// their lifetime.
package struct DatabaseRetainedVectorFieldView: ~Copyable {
    private let vector: Vector

    package init(vector: consuming Vector) {
        self.vector = vector
    }

    package var count: Int { vector.count }
    package var elementType: VectorElementType { vector.elementType }

    package borrowing func withInt8Elements<Result>(
        _ body: (borrowing DatabaseRetainedVectorElements<Int8>) -> Result
    ) -> Result? {
        withElements(borrowing: { vector.withInt8Elements($0) }, body)
    }

    package borrowing func withInt16Elements<Result>(
        _ body: (borrowing DatabaseRetainedVectorElements<Int16>) -> Result
    ) -> Result? {
        withElements(borrowing: { vector.withInt16Elements($0) }, body)
    }

    package borrowing func withInt32Elements<Result>(
        _ body: (borrowing DatabaseRetainedVectorElements<Int32>) -> Result
    ) -> Result? {
        withElements(borrowing: { vector.withInt32Elements($0) }, body)
    }

    package borrowing func withInt64Elements<Result>(
        _ body: (borrowing DatabaseRetainedVectorElements<Int64>) -> Result
    ) -> Result? {
        withElements(borrowing: { vector.withInt64Elements($0) }, body)
    }

    package borrowing func withUInt8Elements<Result>(
        _ body: (borrowing DatabaseRetainedVectorElements<UInt8>) -> Result
    ) -> Result? {
        withElements(borrowing: { vector.withUInt8Elements($0) }, body)
    }

    package borrowing func withUInt16Elements<Result>(
        _ body: (borrowing DatabaseRetainedVectorElements<UInt16>) -> Result
    ) -> Result? {
        withElements(borrowing: { vector.withUInt16Elements($0) }, body)
    }

    package borrowing func withUInt32Elements<Result>(
        _ body: (borrowing DatabaseRetainedVectorElements<UInt32>) -> Result
    ) -> Result? {
        withElements(borrowing: { vector.withUInt32Elements($0) }, body)
    }

    package borrowing func withUInt64Elements<Result>(
        _ body: (borrowing DatabaseRetainedVectorElements<UInt64>) -> Result
    ) -> Result? {
        withElements(borrowing: { vector.withUInt64Elements($0) }, body)
    }

    package borrowing func withFloat32Elements<Result>(
        _ body: (borrowing DatabaseRetainedVectorElements<Float>) -> Result
    ) -> Result? {
        withElements(borrowing: { vector.withFloat32Elements($0) }, body)
    }

    package borrowing func withFloat64Elements<Result>(
        _ body: (borrowing DatabaseRetainedVectorElements<Double>) -> Result
    ) -> Result? {
        withElements(borrowing: { vector.withFloat64Elements($0) }, body)
    }

    private borrowing func withElements<Element, Output>(
        borrowing: (((UnsafeBufferPointer<Element>) -> Void) -> Void?),
        _ body: (borrowing DatabaseRetainedVectorElements<Element>) -> Output
    ) -> Output? {
        withoutActuallyEscaping(body) { body in
            var output: Output?
            guard borrowing({ elements in
                let view = DatabaseRetainedVectorElements(elements)
                output = body(view)
            }) != nil else {
                return nil
            }
            return output
        }
    }
}
