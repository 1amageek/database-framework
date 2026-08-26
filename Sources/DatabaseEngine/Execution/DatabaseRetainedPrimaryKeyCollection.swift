import StorageKit

/// Request-owned primary keys exposed only through zero-based scoped borrows.
///
/// The protocol deliberately does not refine `Collection` and does not return
/// `Tuple`. A consumer can inspect one key only while the concrete owner and
/// its originating work-meter claim remain alive.
package protocol DatabaseRetainedPrimaryKeyCollection: Sendable {
    var count: Int { get }
    var workMeter: DatabaseWorkMeter { get }

    func withRetainedPrimaryKey<Failure: Error>(
        at position: Int,
        _ body: (borrowing Tuple) throws(Failure) -> Void
    ) throws(Failure)
}

extension DatabaseSharedRetainedArray:
    DatabaseRetainedPrimaryKeyCollection where Element == Tuple {
    package func withRetainedPrimaryKey<Failure: Error>(
        at position: Int,
        _ body: (borrowing Tuple) throws(Failure) -> Void
    ) throws(Failure) {
        try withElement(at: position, body)
    }
}
