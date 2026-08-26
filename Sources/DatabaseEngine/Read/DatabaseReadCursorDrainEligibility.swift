/// Decides when session invalidation may finish a cursor retained by a child
/// operation.
///
/// Cursor cleanup must remain deferred while the admission's root operation
/// is active. Descendant borrows may outlive that root only to reach their own
/// terminal cleanup boundary.
struct DatabaseReadCursorDrainEligibility: Sendable {
    private let parentRoot: DatabaseReadScopeOperationLease.RootActivity

    init(parentOperation: DatabaseReadScopeOperationLease) {
        self.parentRoot = parentOperation.rootActivity
    }

    var allowsInvalidation: Bool {
        !parentRoot.isActive
    }
}
