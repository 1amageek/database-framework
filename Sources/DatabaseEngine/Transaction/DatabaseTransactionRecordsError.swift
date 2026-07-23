import StorageKit

public enum DatabaseTransactionRecordsError: Error, Sendable, Equatable {
    case invalidLimit(Int)
    case continuationOutsideRecordRange
    case recordDisappeared(Bytes)
}
