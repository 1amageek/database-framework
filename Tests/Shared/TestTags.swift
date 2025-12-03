// TestTags.swift
// Test tags for filtering tests

import Testing

/// Test tags for filtering test execution
///
/// **Usage**:
/// ```swift
/// @Suite("My FDB Tests", .tags(.fdb))
/// struct MyFDBTests { ... }
/// ```
///
/// **Running tests without FDB**:
/// ```bash
/// swift test --filter "!fdb"
/// ```
///
/// **Running only FDB tests**:
/// ```bash
/// swift test --filter "fdb"
/// ```
extension Tag {
    /// Tag for tests that require a running FoundationDB instance
    @Tag public static var fdb: Self

    /// Tag for tests that require a running FoundationDB instance (alias)
    @Tag public static var requiresFDB: Self

    /// Tag for performance tests
    @Tag public static var performance: Self
}
