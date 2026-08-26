import DatabaseKit
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@testable import DatabaseEngine

@Suite("Retained primary-key collection contract")
struct DatabaseRetainedPrimaryKeyCollectionTests {
    @Test("primary-key access is a void scoped borrow")
    func primaryKeyAccessIsVoidScoped() throws {
        let meter = makeMeter()
        let owner = try TestRetainedPrimaryKeys(
            keys: [Tuple("scoped-primary-key")],
            workMeter: meter
        )
        inspect(owner)
        #expect(meter.retainedIntermediateRows == 1)
        #expect(meter.retainedIntermediateBytes > 0)

        owner.release()
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("primary-key borrow retains its owner only for the borrow scope")
    func primaryKeyBorrowIsScopedToOwner() throws {
        let meter = makeMeter()
        var owner: TestRetainedPrimaryKeys? = try TestRetainedPrimaryKeys(
            keys: [Tuple("scoped-primary-key")],
            workMeter: meter
        )

        owner?.withRetainedPrimaryKey(at: 0) { key in
            #expect(key == Tuple("scoped-primary-key"))
            #expect(meter.retainedIntermediateRows == 1)
            #expect(meter.retainedIntermediateBytes > 0)
        }
        #expect(meter.retainedIntermediateRows == 1)
        #expect(meter.retainedIntermediateBytes > 0)

        owner = nil
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("primary-key contract exposes count and originating meter")
    func primaryKeyContractExposesOnlyOwnedMetadata() throws {
        let meter = makeMeter()
        let owner = try TestRetainedPrimaryKeys(
            keys: [Tuple("first"), Tuple("second")],
            workMeter: meter
        )
        let collection: any DatabaseRetainedPrimaryKeyCollection = owner

        #expect(collection.count == 2)
        #expect(collection.workMeter === meter)
        collection.withRetainedPrimaryKey(at: 1) { key in
            #expect(key == Tuple("second"))
        }

        owner.release()
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private func inspect<C: DatabaseRetainedPrimaryKeyCollection>(
        _ collection: borrowing C
    ) {
        collection.withRetainedPrimaryKey(at: 0) { key in
            #expect(key == Tuple("scoped-primary-key"))
        }
    }

    private func makeMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateRows: 2,
                maximumIntermediateBytes: 256
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }
}

private final class TestRetainedPrimaryKeys:
    DatabaseRetainedPrimaryKeyCollection,
    Sendable
{
    private let keys: [Tuple]
    private let reservation: DatabaseIntermediateReservation

    init(
        keys: [Tuple],
        workMeter: DatabaseWorkMeter
    ) throws {
        self.keys = keys
        self.reservation = try workMeter.reserveIntermediate(
            rows: UInt64(keys.count),
            bytes: UInt64(max(1, keys.count) * 32),
            at: .indexScan
        )
    }

    package var count: Int { keys.count }

    package var workMeter: DatabaseWorkMeter {
        reservation.workMeter
    }

    package func withRetainedPrimaryKey<Failure: Error>(
        at position: Int,
        _ body: (borrowing Tuple) throws(Failure) -> Void
    ) throws(Failure) {
        precondition(position >= keys.startIndex && position < keys.endIndex)
        try body(keys[position])
        withExtendedLifetime(reservation) {}
    }

    func release() {
        reservation.release()
    }
}
