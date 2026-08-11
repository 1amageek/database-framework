@_spi(DatabaseWireRuntime) import DatabaseWire

/// A single-owner unit of resumable work and its next persistent outcome.
///
/// Factory methods keep the completion discriminator consistent with the
/// noncopyable outcome while allowing callers to inspect completion without
/// consuming the payload.
public struct DatabaseResumableOperationSlice<State, Result>: ~Copyable, Sendable
where State: PersistentJobPayload, Result: Sendable {
    public enum Outcome: ~Copyable, Sendable {
        case incomplete(State)
        case complete(Result)
    }

    public let completedWorkUnits: UInt64
    public let totalWorkUnits: UInt64?
    public let outcome: Outcome
    /// A copyable discriminator that can be inspected before consuming outcome.
    public let isComplete: Bool

    public static func incomplete(
        completedWorkUnits: UInt64,
        totalWorkUnits: UInt64? = nil,
        state: State
    ) -> sending Self {
        Self(
            completedWorkUnits: completedWorkUnits,
            totalWorkUnits: totalWorkUnits,
            outcome: .incomplete(state),
            isComplete: false
        )
    }

    public static func complete(
        completedWorkUnits: UInt64,
        totalWorkUnits: UInt64? = nil,
        result: Result
    ) -> sending Self {
        Self(
            completedWorkUnits: completedWorkUnits,
            totalWorkUnits: totalWorkUnits,
            outcome: .complete(result),
            isComplete: true
        )
    }

    private init(
        completedWorkUnits: UInt64,
        totalWorkUnits: UInt64?,
        outcome: consuming Outcome,
        isComplete: Bool
    ) {
        self.completedWorkUnits = completedWorkUnits
        self.totalWorkUnits = totalWorkUnits
        self.outcome = outcome
        self.isComplete = isComplete
    }
}
