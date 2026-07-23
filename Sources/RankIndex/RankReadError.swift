import StorageKit

public enum RankReadError: Error, Sendable, Equatable {
    case missingParameter(String)
    case invalidParameter(String)
    case invalidRange(from: Int, to: Int)
    case invalidPercentile(Double)
    case invalidCount(Int)
    case unsupportedSortExpression
    case invalidRecordIdentifier(typeName: String)
    case duplicateFetchedRecord(primaryKey: Bytes)
    case missingFetchedRecord(primaryKey: Bytes)
    case missingRankEntry(rank: Int)
}
