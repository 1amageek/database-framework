#if DATABASE_MULTIPLE_BASES
/// Durable lifecycle state controlling admission to one Base.
@_spi(DatabaseExecution)
public enum DatabaseBaseLifecycleState: UInt8, Sendable, Hashable {
    case provisioning = 0
    case active = 1
    case retiring = 2
    case retired = 3
    case moving = 4
    case deleting = 5
    case tombstone = 6

    public var name: String {
        switch self {
        case .provisioning: "provisioning"
        case .active: "active"
        case .retiring: "retiring"
        case .retired: "retired"
        case .moving: "moving"
        case .deleting: "deleting"
        case .tombstone: "tombstone"
        }
    }
}

#endif
