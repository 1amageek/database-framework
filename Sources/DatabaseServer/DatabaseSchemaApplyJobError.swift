import DatabaseKit
@_spi(DatabaseServer) import DatabaseWire

public enum DatabaseSchemaApplyJobError: Error, Sendable, Equatable,
    CustomStringConvertible {
    case invalidInvocation
    case corruptedPlan
    case baseLifecycleTransitionInProgress(Base.ID, String)
    case baseGenerationChanged(Base.ID)
    case publishedSchemaMismatch
    case sliceMadeNoProgress

    public var description: String {
        switch self {
        case .invalidInvocation:
            return "Only schema apply requests can create schema build jobs"
        case .corruptedPlan:
            return "Schema apply job plan is corrupted"
        case .baseLifecycleTransitionInProgress(let id, let lifecycle):
            return "Base '\(id.value)' has an in-progress lifecycle transition '\(lifecycle)'"
        case .baseGenerationChanged(let id):
            return "Base '\(id.value)' changed placement during schema application"
        case .publishedSchemaMismatch:
            return "Published schema does not match the schema build job"
        case .sliceMadeNoProgress:
            return "Schema index build slice made no progress"
        }
    }
}
