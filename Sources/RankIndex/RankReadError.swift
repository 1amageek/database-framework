import DatabaseTypes
import StorageKit

public enum RankReadError: Error, Sendable, Equatable {
    case missingParameter(String)
    case invalidParameter(String)
    case invalidRange(from: Int, to: Int)
    case invalidPercentile(Double)
    case invalidCount(Int)
    case unsupportedSortExpression
    case invalidPersistableIdentifier(typeName: String)
    case fetchedEntityCountMismatch(expected: Int, actual: Int)
    case missingFetchedEntity(primaryKey: ByteString)
    case missingRankEntry(rank: Int)
}
