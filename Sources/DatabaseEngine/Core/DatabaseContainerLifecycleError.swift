/// Failures caused by using a database container outside its owned storage
/// lifecycle.
public enum DatabaseContainerLifecycleError: Error, Sendable, Equatable {
    /// The configuration has already transferred its storage engine to an open
    /// or opening container.
    case configurationAlreadyUsed

    /// The container has started its terminal shutdown transition.
    case shuttingDown

    /// The container and its storage engine have completed shutdown.
    case shutdown

    /// The process cannot admit another concurrent storage operation.
    case operationLimitExceeded
}
