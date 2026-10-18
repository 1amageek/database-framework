import Synchronization

/// Request-local fallback position for failures without an expression occurrence.
package final class SQLBlockFailurePosition: Sendable {
    private let storage = Mutex<Int?>(nil)

    package init() {}
    package var stepIndex: Int? { storage.withLock { $0 } }
    func reset() { storage.withLock { $0 = nil } }
    func record(_ index: Int) { storage.withLock { $0 = index } }
}
