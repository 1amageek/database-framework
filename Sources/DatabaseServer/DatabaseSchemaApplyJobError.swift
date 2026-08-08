@_spi(DatabaseServer) import DatabaseWire

public enum DatabaseSchemaApplyJobError: Error, Sendable, Equatable,
    CustomStringConvertible {
    case invalidInvocation
    case noIndexBuildTargets
    case publishedSchemaMismatch
    case sliceMadeNoProgress

    public var description: String {
        switch self {
        case .invalidInvocation:
            return "Only schema apply requests can create schema build jobs"
        case .noIndexBuildTargets:
            return "Schema apply job has no index build targets"
        case .publishedSchemaMismatch:
            return "Published schema does not match the schema build job"
        case .sliceMadeNoProgress:
            return "Schema index build slice made no progress"
        }
    }
}
