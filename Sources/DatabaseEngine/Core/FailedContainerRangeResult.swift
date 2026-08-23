import DatabaseTypes
import StorageKit

struct FailedContainerRangeResult: TransactionRangeResult {
    let error: DatabaseContainerLifecycleError

    func makeCursor() -> FailedContainerRangeCursor {
        FailedContainerRangeCursor(error: error)
    }
}

struct FailedContainerRangeCursor: TransactionRangeCursor {
    let error: DatabaseContainerLifecycleError

    mutating func next() async throws -> (ByteString, ByteString)? {
        throw error
    }

    mutating func finish(
        isolation actor: isolated (any Actor)?
    ) async throws {}
}
