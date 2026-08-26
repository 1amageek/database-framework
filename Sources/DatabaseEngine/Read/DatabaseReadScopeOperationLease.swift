import Synchronization

/// One root operation or independently draining descendant borrow admitted by
/// a revocable read session.
///
/// Task-local descendants may begin while their parent is admitted, including
/// after session closure starts. Every descendant has an idempotent lifetime;
/// the scope's operation count ends only after the root and all started borrows
/// reach their terminal boundaries.
struct DatabaseReadScopeOperationLease: Sendable {
    @TaskLocal static var current: DatabaseReadScopeOperationLease?

    struct RootActivity: Sendable {
        private let admission: Admission

        fileprivate init(admission: Admission) {
            self.admission = admission
        }

        var isActive: Bool {
            admission.isRootActive
        }
    }

    fileprivate final class Admission: Sendable {
        private struct State: Sendable {
            var referenceCount = 1
            var isRootActive = true
        }

        let scopeIdentity: ObjectIdentifier

        private let state = Mutex(State())
        private let validateOperation: @Sendable () throws -> Void
        private let rootDidEnd: @Sendable () -> Void
        private let endOperation: @Sendable () -> Void

        init(
            scopeIdentity: ObjectIdentifier,
            validateOperation: @escaping @Sendable () throws -> Void,
            rootDidEnd: @escaping @Sendable () -> Void,
            endOperation: @escaping @Sendable () -> Void
        ) {
            self.scopeIdentity = scopeIdentity
            self.validateOperation = validateOperation
            self.rootDidEnd = rootDidEnd
            self.endOperation = endOperation
        }

        func validate() throws {
            guard state.withLock({ $0.referenceCount > 0 }) else {
                throw DatabaseTransactionError.invalidOperationContext
            }
            try validateOperation()
        }

        fileprivate func acquireBorrow() throws -> Lifetime {
            let acquired = state.withLock { state in
                guard state.referenceCount > 0 else { return false }
                let (count, overflow) = state.referenceCount
                    .addingReportingOverflow(1)
                guard !overflow else { return false }
                state.referenceCount = count
                return true
            }
            guard acquired else {
                throw DatabaseTransactionError.invalidOperationContext
            }
            do {
                try validateOperation()
            } catch {
                release(isRoot: false)
                throw error
            }
            return Lifetime(admission: self, isRoot: false)
        }

        var isRootActive: Bool {
            state.withLock { $0.isRootActive }
        }

        func release(isRoot: Bool) {
            let transition = state.withLock { state in
                precondition(state.referenceCount > 0)
                var didEndRoot = false
                if isRoot {
                    precondition(state.isRootActive)
                    state.isRootActive = false
                    didEndRoot = true
                }
                state.referenceCount -= 1
                return (
                    didEndRoot: didEndRoot,
                    didEndOperation: state.referenceCount == 0
                )
            }
            if transition.didEndRoot { rootDidEnd() }
            if transition.didEndOperation { endOperation() }
        }
    }

    fileprivate final class Lifetime: Sendable {
        let admission: Admission
        private let isRoot: Bool
        private let didEnd = Mutex(false)

        init(admission: Admission, isRoot: Bool) {
            self.admission = admission
            self.isRoot = isRoot
        }

        func validate() throws {
            try didEnd.withLock { didEnd in
                guard !didEnd else {
                    throw DatabaseTransactionError.invalidOperationContext
                }
                try admission.validate()
            }
        }

        func borrow() throws -> Lifetime {
            try didEnd.withLock { didEnd in
                guard !didEnd else {
                    throw DatabaseTransactionError.invalidOperationContext
                }
                return try admission.acquireBorrow()
            }
        }

        func end() {
            let shouldRelease = didEnd.withLock { didEnd in
                guard !didEnd else { return false }
                didEnd = true
                return true
            }
            if shouldRelease { admission.release(isRoot: isRoot) }
        }

        deinit {
            end()
        }
    }

    private let lifetime: Lifetime

    init(
        scopeIdentity: ObjectIdentifier,
        validateOperation: @escaping @Sendable () throws -> Void = {},
        rootDidEnd: @escaping @Sendable () -> Void = {},
        endOperation: @escaping @Sendable () -> Void
    ) {
        let admission = Admission(
            scopeIdentity: scopeIdentity,
            validateOperation: validateOperation,
            rootDidEnd: rootDidEnd,
            endOperation: endOperation
        )
        self.lifetime = Lifetime(admission: admission, isRoot: true)
    }

    private init(
        borrowing lifetime: Lifetime
    ) {
        self.lifetime = lifetime
    }

    func validate() throws {
        try lifetime.validate()
    }

    var isRootActive: Bool {
        lifetime.admission.isRootActive
    }

    var rootActivity: RootActivity {
        RootActivity(admission: lifetime.admission)
    }

    func borrowed(by scopeIdentity: ObjectIdentifier) throws
        -> DatabaseReadScopeOperationLease {
        guard lifetime.admission.scopeIdentity == scopeIdentity else {
            throw DatabaseTransactionError.invalidOperationContext
        }
        return try DatabaseReadScopeOperationLease(
            borrowing: lifetime.borrow()
        )
    }

    func end() {
        lifetime.end()
    }
}
