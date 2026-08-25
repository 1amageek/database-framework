/// Owns asynchronous resource shutdown for one serialized integration-test
/// scenario. Once draining starts, no resource can enter the scenario.
package actor ScenarioResourceOwner {
    private struct ResourceEntry: Sendable {
        let shutdown: @Sendable () async -> Void
    }

    private enum DrainState {
        case accepting
        case draining([CheckedContinuation<Void, Never>])
        case drained
    }

    private var drainState = DrainState.accepting
    private var entries: [ResourceEntry] = []
    private var identities: Set<ObjectIdentifier> = []

    package init() {}

    package var resourceCount: Int {
        entries.count
    }

    /// Registers one resource for exactly-once shutdown.
    ///
    /// A false result means scenario draining already started. The caller
    /// retains ownership and must shut down the rejected resource.
    package func register<Resource: AnyObject & Sendable>(
        _ resource: Resource,
        shutdown: @escaping @Sendable (Resource) async -> Void
    ) -> Bool {
        guard case .accepting = drainState else {
            return false
        }
        let identity = ObjectIdentifier(resource)
        guard identities.insert(identity).inserted else {
            return true
        }
        entries.append(
            ResourceEntry(
                shutdown: { await shutdown(resource) }
            )
        )
        return true
    }

    /// Stops accepting resources and awaits every owned shutdown action.
    package func shutdownAll() async {
        switch drainState {
        case .drained:
            return
        case .draining(var waiters):
            await withCheckedContinuation { continuation in
                waiters.append(continuation)
                drainState = .draining(waiters)
            }
            return
        case .accepting:
            drainState = .draining([])
        }

        let resources = entries.reversed()
        entries.removeAll(keepingCapacity: false)
        identities.removeAll(keepingCapacity: false)
        for resource in resources {
            await resource.shutdown()
        }

        let waiters: [CheckedContinuation<Void, Never>]
        if case .draining(let registeredWaiters) = drainState {
            waiters = registeredWaiters
        } else {
            waiters = []
        }
        drainState = .drained
        for waiter in waiters {
            waiter.resume()
        }
    }
}
