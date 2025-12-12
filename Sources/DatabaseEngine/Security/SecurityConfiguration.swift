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
/// // Security enabled (default)
/// let container = try FDBContainer(for: schema)
///
/// // Security enabled with custom admin roles
/// let container = try FDBContainer(
///     for: schema,
///     security: .enabled(adminRoles: ["admin", "superuser"])
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

    /// Roles treated as Admin (skip security evaluation)
    public let adminRoles: Set<String>

    public init(isEnabled: Bool = true, adminRoles: Set<String> = ["admin"]) {
        self.isEnabled = isEnabled
        self.adminRoles = adminRoles
    }

    /// Security disabled
    ///
    /// **Warning**: Use only for testing. In production, always use `.enabled()`.
    public static let disabled = SecurityConfiguration(isEnabled: false, adminRoles: [])

    /// Security enabled with specified admin roles (default)
    ///
    /// - Parameter adminRoles: Roles that bypass security evaluation (default: ["admin"])
    public static func enabled(adminRoles: Set<String> = ["admin"]) -> SecurityConfiguration {
        SecurityConfiguration(isEnabled: true, adminRoles: adminRoles)
    }
}
