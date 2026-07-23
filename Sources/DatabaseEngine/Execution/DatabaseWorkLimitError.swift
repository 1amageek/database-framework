public enum DatabaseWorkLimitError:
    Error,
    Sendable,
    Hashable,
    CustomStringConvertible {
    case maximumRows(
        stage: DatabaseWorkStage,
        consumed: UInt32,
        requested: UInt32,
        maximum: UInt32
    )
    case maximumWorkUnits(
        stage: DatabaseWorkStage,
        consumed: UInt64,
        requested: UInt64,
        maximum: UInt64
    )
    case maximumIntermediateRows(
        stage: DatabaseWorkStage,
        consumed: UInt64,
        requested: UInt64,
        maximum: UInt64
    )
    case maximumIntermediateBytes(
        stage: DatabaseWorkStage,
        consumed: UInt64,
        requested: UInt64,
        maximum: UInt64
    )
    case deadline(stage: DatabaseWorkStage)

    public var description: String {
        switch self {
        case .maximumRows(let stage, let consumed, let requested, let maximum):
            return "Work stage '\(stage.rawValue)' requested \(requested) rows after \(consumed), exceeding \(maximum)"
        case .maximumWorkUnits(
            let stage,
            let consumed,
            let requested,
            let maximum
        ):
            return "Work stage '\(stage.rawValue)' requested \(requested) units after \(consumed), exceeding \(maximum)"
        case .maximumIntermediateRows(
            let stage,
            let consumed,
            let requested,
            let maximum
        ):
            return "Work stage '\(stage.rawValue)' retained \(requested) rows after \(consumed), exceeding \(maximum)"
        case .maximumIntermediateBytes(
            let stage,
            let consumed,
            let requested,
            let maximum
        ):
            return "Work stage '\(stage.rawValue)' retained \(requested) bytes after \(consumed), exceeding \(maximum)"
        case .deadline(let stage):
            return "Work stage '\(stage.rawValue)' exceeded the request deadline"
        }
    }
}
