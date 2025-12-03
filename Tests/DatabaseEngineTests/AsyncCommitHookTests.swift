// AsyncCommitHookTests.swift
// Tests for AsyncCommitHooks, PreCommitCheck, and PostCommitAction

import Testing
import Foundation
import Synchronization
@testable import DatabaseEngine

@Suite("AsyncCommitHook Tests")
struct AsyncCommitHookTests {

    // MARK: - Configuration Tests

    @Test func defaultConfiguration() {
        let config = HookExecutionConfiguration.default

        #expect(config.enableConcurrentPreCommitChecks)
        #expect(config.enableConcurrentPostCommitActions)
        #expect(config.preCommitCheckTimeoutSeconds == 5.0)
        #expect(config.postCommitActionTimeoutSeconds == 30.0)
    }

    @Test func sequentialConfiguration() {
        let config = HookExecutionConfiguration.sequential

        #expect(!config.enableConcurrentPreCommitChecks)
        #expect(!config.enableConcurrentPostCommitActions)
    }

    // MARK: - Pre-Commit Check Management Tests

    @Test func addPreCommitCheck() {
        let hooks = AsyncCommitHooks()
        let check = MockPreCommitCheck(identifier: "test")

        let id = hooks.addPreCommitCheck(check)

        #expect(hooks.preCommitCheckCount == 1)
        #expect(id != UUID())
    }

    @Test func removePreCommitCheckById() {
        let hooks = AsyncCommitHooks()
        let check = MockPreCommitCheck(identifier: "test")

        let id = hooks.addPreCommitCheck(check)
        hooks.removePreCommitCheck(id: id)

        #expect(hooks.preCommitCheckCount == 0)
    }

    @Test func removePreCommitChecksByPredicate() {
        let hooks = AsyncCommitHooks()
        hooks.addPreCommitCheck(MockPreCommitCheck(identifier: "keep"))
        hooks.addPreCommitCheck(MockPreCommitCheck(identifier: "remove1"))
        hooks.addPreCommitCheck(MockPreCommitCheck(identifier: "remove2"))

        hooks.removePreCommitChecks { $0.identifier.hasPrefix("remove") }

        #expect(hooks.preCommitCheckCount == 1)
    }

    // MARK: - Post-Commit Action Management Tests

    @Test func addPostCommitAction() {
        let hooks = AsyncCommitHooks()
        let action = MockPostCommitAction(identifier: "test")

        let id = hooks.addPostCommitAction(action)

        #expect(hooks.postCommitActionCount == 1)
        #expect(id != UUID())
    }

    @Test func removePostCommitActionById() {
        let hooks = AsyncCommitHooks()
        let action = MockPostCommitAction(identifier: "test")

        let id = hooks.addPostCommitAction(action)
        hooks.removePostCommitAction(id: id)

        #expect(hooks.postCommitActionCount == 0)
    }

    @Test func removePostCommitActionsByPredicate() {
        let hooks = AsyncCommitHooks()
        hooks.addPostCommitAction(MockPostCommitAction(identifier: "keep"))
        hooks.addPostCommitAction(MockPostCommitAction(identifier: "remove1"))
        hooks.addPostCommitAction(MockPostCommitAction(identifier: "remove2"))

        hooks.removePostCommitActions { $0.identifier.hasPrefix("remove") }

        #expect(hooks.postCommitActionCount == 1)
    }

    // MARK: - Pre-Commit Check Execution Tests

    @Test func runPreCommitChecksSucceeds() async throws {
        let hooks = AsyncCommitHooks()
        let executed = Mutex(false)
        let check = MockPreCommitCheck(identifier: "test") {
            executed.withLock { $0 = true }
        }
        hooks.addPreCommitCheck(check)

        let context = CommitContext(
            insertedRecords: [],
            updatedRecords: [],
            deletedRecords: []
        )

        try await hooks.runPreCommitChecks(context: context)

        #expect(executed.withLock { $0 })
    }

    @Test func runPreCommitChecksThrowsOnFailure() async {
        let hooks = AsyncCommitHooks()
        let check = FailingPreCommitCheck()
        hooks.addPreCommitCheck(check)

        let context = CommitContext(
            insertedRecords: [],
            updatedRecords: [],
            deletedRecords: []
        )

        do {
            try await hooks.runPreCommitChecks(context: context)
            Issue.record("Expected error to be thrown")
        } catch {
            // Expected
        }
    }

    @Test func preCommitChecksRunInPriorityOrder() async throws {
        let hooks = AsyncCommitHooks(configuration: .sequential)
        let order = Mutex<[String]>([])

        let lowPriorityCheck = MockPreCommitCheck(identifier: "low", priority: 10) {
            order.withLock { $0.append("low") }
        }
        let highPriorityCheck = MockPreCommitCheck(identifier: "high", priority: 100) {
            order.withLock { $0.append("high") }
        }
        let mediumPriorityCheck = MockPreCommitCheck(identifier: "medium", priority: 50) {
            order.withLock { $0.append("medium") }
        }

        // Add in different order than priority
        hooks.addPreCommitCheck(lowPriorityCheck)
        hooks.addPreCommitCheck(highPriorityCheck)
        hooks.addPreCommitCheck(mediumPriorityCheck)

        let context = CommitContext(
            insertedRecords: [],
            updatedRecords: [],
            deletedRecords: []
        )

        try await hooks.runPreCommitChecks(context: context)

        let executionOrder = order.withLock { $0 }
        #expect(executionOrder == ["high", "medium", "low"])
    }

    // MARK: - Post-Commit Action Execution Tests

    @Test func runPostCommitActionsReturnsResults() async {
        let hooks = AsyncCommitHooks()
        let action = MockPostCommitAction(identifier: "test")
        hooks.addPostCommitAction(action)

        let context = CommitContext(
            insertedRecords: [],
            updatedRecords: [],
            deletedRecords: []
        )

        let results = await hooks.runPostCommitActions(
            commitVersion: 12345,
            context: context
        )

        #expect(results.count == 1)
        #expect(results[0].succeeded)
        #expect(results[0].identifier == "test")
    }

    @Test func postCommitActionFailuresAreCaptured() async {
        let hooks = AsyncCommitHooks()
        let action = FailingPostCommitAction()
        hooks.addPostCommitAction(action)

        let context = CommitContext(
            insertedRecords: [],
            updatedRecords: [],
            deletedRecords: []
        )

        let results = await hooks.runPostCommitActions(
            commitVersion: 12345,
            context: context
        )

        #expect(results.count == 1)
        #expect(!results[0].succeeded)
        #expect(results[0].error != nil)
    }

    @Test func postCommitActionsRunInPriorityOrder() async {
        let hooks = AsyncCommitHooks(configuration: .sequential)
        let order = Mutex<[String]>([])

        let lowPriorityAction = MockPostCommitAction(identifier: "low", priority: 10) {
            order.withLock { $0.append("low") }
        }
        let highPriorityAction = MockPostCommitAction(identifier: "high", priority: 100) {
            order.withLock { $0.append("high") }
        }

        hooks.addPostCommitAction(lowPriorityAction)
        hooks.addPostCommitAction(highPriorityAction)

        let context = CommitContext(
            insertedRecords: [],
            updatedRecords: [],
            deletedRecords: []
        )

        _ = await hooks.runPostCommitActions(
            commitVersion: 12345,
            context: context
        )

        let executionOrder = order.withLock { $0 }
        #expect(executionOrder == ["high", "low"])
    }

    // MARK: - PostCommitResult Tests

    @Test func postCommitResultDuration() async {
        let hooks = AsyncCommitHooks()
        let action = MockPostCommitAction(identifier: "slow") {
            try? await Task.sleep(nanoseconds: 50_000_000) // 50ms
        }
        hooks.addPostCommitAction(action)

        let context = CommitContext(
            insertedRecords: [],
            updatedRecords: [],
            deletedRecords: []
        )

        let results = await hooks.runPostCommitActions(
            commitVersion: 12345,
            context: context
        )

        #expect(results[0].duration >= 0.05) // At least 50ms
    }

    // MARK: - PreCommitValidationError Tests

    @Test func recordCountLimitExceededErrorDescription() {
        let error = PreCommitValidationError.recordCountLimitExceeded(
            typeName: "User",
            current: 90,
            adding: 20,
            limit: 100
        )

        let description = error.description
        #expect(description.contains("User"))
        #expect(description.contains("90"))
        #expect(description.contains("20"))
        #expect(description.contains("100"))
    }

    @Test func requiredFieldsMissingErrorDescription() {
        let error = PreCommitValidationError.requiredFieldsMissing(
            typeName: "User",
            recordId: "user-123",
            missingFields: ["email", "name"]
        )

        let description = error.description
        #expect(description.contains("User"))
        #expect(description.contains("user-123"))
        #expect(description.contains("email"))
        #expect(description.contains("name"))
    }
}

// MARK: - Mock Types

private final class MockPreCommitCheck: PreCommitCheck, @unchecked Sendable {
    let identifier: String
    let priority: Int
    let supportsConcurrentExecution = true
    private let onValidate: @Sendable () -> Void

    init(identifier: String, priority: Int = 0, onValidate: @escaping @Sendable () -> Void = {}) {
        self.identifier = identifier
        self.priority = priority
        self.onValidate = onValidate
    }

    func validate(context: CommitContext) async throws {
        onValidate()
    }
}

private struct FailingPreCommitCheck: PreCommitCheck {
    let identifier = "failing"
    let priority = 0
    let supportsConcurrentExecution = true

    func validate(context: CommitContext) async throws {
        throw TestError.validationFailed
    }
}

private final class MockPostCommitAction: PostCommitAction, @unchecked Sendable {
    let identifier: String
    let priority: Int
    let failureIsError = false
    private let onExecute: @Sendable () async -> Void

    init(identifier: String, priority: Int = 0, onExecute: @escaping @Sendable () async -> Void = {}) {
        self.identifier = identifier
        self.priority = priority
        self.onExecute = onExecute
    }

    func execute(commitVersion: Int64, context: CommitContext) async throws {
        await onExecute()
    }
}

private struct FailingPostCommitAction: PostCommitAction {
    let identifier = "failing"
    let priority = 0
    let failureIsError = true

    func execute(commitVersion: Int64, context: CommitContext) async throws {
        throw TestError.actionFailed
    }
}

private enum TestError: Error {
    case validationFailed
    case actionFailed
}
