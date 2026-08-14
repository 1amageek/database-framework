import DatabaseKit
import DatabaseTypes

public enum DatabaseIndexRebuildPhase: Sendable, Hashable {
    case building
    case complete
    case failed
}

public struct DatabaseIndexMaintenanceStatus: Sendable {
    public let entity: String
    public let index: String
    public let partitions: FieldObject
    public let indexState: IndexState
    public let rebuildPhase: DatabaseIndexRebuildPhase?
    public let indexedEntityCount: UInt64
    public let detail: String?
}
