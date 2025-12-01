import Foundation
import FoundationDB
import TestSupport

/// FDB test environment manager
///
/// Ensures FDBClient is initialized exactly once across all tests.
/// Uses FDBTestSetup from TestSupport to ensure single initialization.
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
}
