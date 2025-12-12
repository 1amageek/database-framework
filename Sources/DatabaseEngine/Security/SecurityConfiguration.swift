// SecurityConfiguration.swift
// DatabaseEngine - Security configuration for FDBContainer

import Foundation
import Core

/// Security configuration
///
/// Configures security behavior for the FDBContainer.
/// Security is enabled by default (secure by default).
///
/// **Usage**:
/// ```swift
/// // Security enabled with strict mode (default - secure by default)
/// let container = try FDBContainer(for: schema)
///
/// // Security enabled with custom admin roles
/// let container = try FDBContainer(
///     for: schema,
///     security: .enabled(adminRoles: ["admin", "superuser"])
/// )
///
/// // Non-strict mode (allows models without SecurityPolicy - for migration)
/// let container = try FDBContainer(
///     for: schema,
///     security: .enabled(strict: false)
/// )
///
/// // Security disabled (ONLY for testing)
/// let testContainer = try FDBContainer(
///     for: schema,
///     security: .disabled
/// )
/// ```
public struct SecurityConfiguration: Sendable {
    /// Whether security evaluation is enabled
    public let isEnabled: Bool

    /// Whether to deny access for models without SecurityPolicy implementation
    ///
    /// - `true` (default): Models without SecurityPolicy are denied (secure-by-default)
    /// - `false`: Models without SecurityPolicy are allowed (for backward compatibility)
    public let strict: Bool

    /// Roles treated as Admin (skip security evaluation)
    public let adminRoles: Set<String>

    public init(isEnabled: Bool = true, strict: Bool = true, adminRoles: Set<String> = ["admin"]) {
        self.isEnabled = isEnabled
        self.strict = strict
        self.adminRoles = adminRoles
    }

    /// Security disabled
    ///
    /// **Warning**: Use only for testing. In production, always use `.enabled()`.
    public static let disabled = SecurityConfiguration(isEnabled: false, strict: false, adminRoles: [])

    /// Security enabled with specified options (default)
    ///
    /// - Parameters:
    ///   - strict: Deny models without SecurityPolicy (default: true for secure-by-default)
    ///   - adminRoles: Roles that bypass security evaluation (default: ["admin"])
    public static func enabled(
        strict: Bool = true,
        adminRoles: Set<String> = ["admin"]
    ) -> SecurityConfiguration {
        SecurityConfiguration(isEnabled: true, strict: strict, adminRoles: adminRoles)
    }
}
