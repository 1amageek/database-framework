@_spi(DatabaseServer) import DatabaseWire

public struct DatabaseResumableOperationSlice<State, Result>: Sendable
where State: PersistentJobPayload, Result: Sendable {
    public enum Outcome: Sendable {
        case incomplete(State)
        case complete(Result)
    }

    public let completedWorkUnits: UInt64
    public let totalWorkUnits: UInt64?
    public let outcome: Outcome

    public var isComplete: Bool {
        if case .complete = outcome { return true }
        return false
    }

    public static func incomplete(
        completedWorkUnits: UInt64,
        totalWorkUnits: UInt64? = nil,
        state: State
    ) -> Self {
        Self(
            completedWorkUnits: completedWorkUnits,
            totalWorkUnits: totalWorkUnits,
            outcome: .incomplete(state)
        )
    }

    public static func complete(
        completedWorkUnits: UInt64,
        totalWorkUnits: UInt64? = nil,
        result: Result
    ) -> Self {
        Self(
            completedWorkUnits: completedWorkUnits,
            totalWorkUnits: totalWorkUnits,
            outcome: .complete(result)
        )
    }

    private init(
        completedWorkUnits: UInt64,
        totalWorkUnits: UInt64?,
        outcome: Outcome
    ) {
        self.completedWorkUnits = completedWorkUnits
        self.totalWorkUnits = totalWorkUnits
        self.outcome = outcome
    }
}
