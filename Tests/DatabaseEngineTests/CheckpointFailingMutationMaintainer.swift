import DatabaseKit
import Synchronization
@testable import DatabaseEngine

final class CheckpointFailingMutationMaintainer: PersistableMutationMaintainer {
    enum Failure: Error, Sendable, Equatable {
        case expected
    }

    let identifier = "test.checkpoint.fail-once"
    private let failures = Mutex(0)

    var failureCount: Int { failures.withLock { $0 } }

    func validate(schema: Schema) throws {}

    func update(
        identity: EntityReference,
        oldModel: PersistedModel?,
        newModel: PersistedModel?,
        context: borrowing PersistableMutationContext
    ) async throws {
        guard oldModel != nil, newModel?.entity == EntityMutationFixture.persistableType else {
            return
        }
        let shouldFail = failures.withLock { count in
            guard count == 0 else { return false }
            count += 1
            return true
        }
        if shouldFail { throw Failure.expected }
    }

    func validateFinalState(
        of models: [PersistedModel],
        context: borrowing PersistableValidationContext
    ) async throws {}
}
