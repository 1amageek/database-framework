// SecurityConfiguration.swift
// DatabaseEngine - Security configuration for DBContainer

import DatabaseKit

/// Security configuration
///
/// Configures security behavior for the DBContainer.
/// Security is enabled by default (secure by default).
///
/// **Usage**:
/// ```swift
/// // Security enabled (secure by default)
/// let container = try DBContainer.open(for: schema)
///
/// // Security enabled with custom admin roles
/// let container = try DBContainer.open(
///     for: schema,
///     security: .enabled(adminRoles: ["admin", "superuser"])
/// )
///
/// // Security disabled (ONLY for testing)
/// let testContainer = try DBContainer.open(
///     for: schema,
///     security: .disabled
/// )
/// ```
public struct SecurityConfiguration: Sendable {
    /// Whether security evaluation is enabled
    public let isEnabled: Bool

    /// Roles treated as Admin (skip security evaluation)
    public let adminRoles: Set<String>

    public init(
        isEnabled: Bool = true,
        adminRoles: Set<String> = ["admin"]
    ) {
        self.isEnabled = isEnabled
        self.adminRoles = adminRoles
    }

    /// Security disabled
    ///
    /// **Warning**: Use only for testing. In production, always use `.enabled()`.
    public static let disabled = SecurityConfiguration(
        isEnabled: false,
        adminRoles: []
    )

    /// Security enabled with specified options (default)
    ///
    /// - Parameters:
    ///   - adminRoles: Roles that bypass security evaluation (default: ["admin"])
    public static func enabled(
        adminRoles: Set<String> = ["admin"]
    ) -> SecurityConfiguration {
        SecurityConfiguration(isEnabled: true, adminRoles: adminRoles)
    }
}
