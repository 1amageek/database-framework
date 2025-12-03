import Foundation
import FoundationDB
import TestSupport

/// FDB test environment manager
///
/// Ensures FDBClient is initialized exactly once across all tests.
/// Uses FDBTestSetup from TestSupport to ensure single initialization and serialization.
actor FDBTestEnvironment {
    /// Shared singleton instance
    static let shared = FDBTestEnvironment()

    /// Private initializer (use shared instance)
    private init() {}

    /// Ensure FDB client is initialized
    ///
    /// Safe to call multiple times - initialization happens only once via
    /// FDBTestSetup.shared. All calls await the same initialization.
    func ensureInitialized() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    /// Execute a test with serialized FDB access
    ///
    /// Use this for FDB integration tests to prevent version conflicts.
    /// ```swift
    /// @Test func myTest() async throws {
    ///     try await FDBTestEnvironment.shared.withSerializedAccess {
    ///         // FDB operations here
    ///     }
    /// }
    /// ```
    func withSerializedAccess<T: Sendable>(
        _ operation: @Sendable () async throws -> T
    ) async throws -> T {
        try await FDBTestSetup.shared.withSerializedAccess(operation)
    }
}
