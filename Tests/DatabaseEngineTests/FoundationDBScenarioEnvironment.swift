#if !os(WASI)
#if FOUNDATION_DB
import Foundation
import StorageKit
import TestSupport

/// Exposes FoundationDB initialization and serialized access to engine scenarios.
///
/// Delegates lifecycle and exclusivity to `FoundationDBScenarioCoordinator`.
actor FoundationDBScenarioEnvironment {
    /// Shared singleton instance
    static let shared = FoundationDBScenarioEnvironment()

    /// Private initializer (use shared instance)
    private init() {}

    /// Ensure FDB client is initialized
    ///
    /// Safe to call multiple times - initialization happens only once via
    /// FoundationDBScenarioCoordinator.shared. All calls await the same initialization.
    func ensureInitialized() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    /// Execute an engine scenario with serialized FoundationDB access.
    ///
    /// Use this for integration scenarios that must prevent version conflicts.
    /// ```swift
    /// @Test func engineOperationIsSerialized() async throws {
    ///     try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
    ///         // FoundationDB engine operations run here.
    ///     }
    /// }
    /// ```
    func withSerializedAccess<T: Sendable>(
        _ operation: @Sendable () async throws -> T
    ) async throws -> T {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess(operation)
    }
}
#endif

#endif
