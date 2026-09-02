import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@testable import DatabaseEngine

private let postClosureProbeKey = ByteString(
    utf8: "post-closure-cancellation-probe"
)
private let postClosureProbeValue = ByteString([0x2a])

/// Proves the post-closure cancellation contract of a read execution.
///
/// A read result must not reach a cancelled caller, and a committed write must
/// never be reported as cancelled. Both are decided at the same place: the
/// point where the attempt has closed authoritatively. The commit barrier is
/// the only seam that reaches it, because it holds the caller inside the
/// commit, after the operation has produced its value and before the attempt
/// closes.
@Suite("Post-closure cancellation", .serialized)
struct PostClosureCancellationTests {
    @Persistable
    struct Anchor {
        #Directory<Anchor>("post-closure-cancellation", "anchors")

        var id: String = ""
    }

    @Test("A read result is not handed to a cancelled caller")
    func readResultFailsWhenCancelledAfterClosure() async throws {
        let (container, control) = try await makeControlledContainer()
        defer { Task { await container.shutdown() } }
        let context = container.testBaseContext()
        // Warm the container so the measured read owns the next read-only
        // commit rather than a first-use metadata transaction.
        _ = try await context.withStorageAccess(requiredAccess: .read) { _ in
            true
        }

        let barrier = control.suspendNextReadOnlyCommit()
        let read = Task {
            try await context.withStorageAccess(requiredAccess: .read) { _ in
                "read result"
            }
        }
        await barrier.waitUntilEntered()
        read.cancel()
        barrier.release()

        await #expect(throws: CancellationError.self) {
            try await read.value
        }
    }

    @Test("An uncancelled read result reaches its caller")
    func readResultReachesUncancelledCaller() async throws {
        let (container, control) = try await makeControlledContainer()
        defer { Task { await container.shutdown() } }
        let context = container.testBaseContext()
        _ = try await context.withStorageAccess(requiredAccess: .read) { _ in
            true
        }

        let barrier = control.suspendNextReadOnlyCommit()
        let read = Task {
            try await context.withStorageAccess(requiredAccess: .read) { _ in
                "read result"
            }
        }
        await barrier.waitUntilEntered()
        barrier.release()

        #expect(try await read.value == "read result")
    }

    @Test("A committed write is not reported as cancelled")
    func writeResultSurvivesCancellationAtItsCommit() async throws {
        let (container, control) = try await makeControlledContainer()
        defer { Task { await container.shutdown() } }
        let context = container.testBaseContext()

        let barrier = control.suspendNextMutatingCommit()
        let write = Task {
            try await context.withTransaction { transaction in
                try transaction.storageAccess.setValue(
                    postClosureProbeValue,
                    for: postClosureProbeKey
                )
            }
        }
        await barrier.waitUntilEntered()
        write.cancel()
        barrier.release()

        // The commit is authoritative, so the caller's cancellation must not
        // turn it into a failure.
        try await write.value

        let stored = try await context.withStorageAccess(
            requiredAccess: .read
        ) { access in
            try await access.getValue(for: postClosureProbeKey)
        }
        #expect(stored == postClosureProbeValue)
    }

    private func makeControlledContainer() async throws -> (
        DBContainer,
        StorageTransactionControl
    ) {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await DBContainer.open(
            testing: try Schema(
                entities: [try Anchor.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: storage),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try EntityRuntimeDefinition(Anchor.self).registration()
                ]
            ),
            security: .testingDisabled
        )
        return (container, storage.control)
    }
}
