// IndexState.swift
// FDBRuntime - Index lifecycle state management
//
// Defines the three-state lifecycle of indexes (disabled, writeOnly, readable).
// State transitions are controlled by IndexStateManager.

/// Index lifecycle state
///
/// **Purpose**: Manage index construction and availability
/// - Determine whether index should be maintained
/// - Control whether index can be used in queries
/// - Support online index construction (write-only state during build)
///
/// **State transitions**:
/// ```
/// disabled → writeOnly → readable
/// readable → disabled (index removal)
/// ```
///
/// **Examples**:
/// ```swift
/// // New index (under construction)
/// let state = IndexState.writeOnly  // Maintained but not queryable
///
/// // Construction complete
/// let state = IndexState.readable   // Fully operational
///
/// // Index removal
/// let state = IndexState.disabled   // Not maintained, not usable
/// ```
public enum IndexState: UInt8, Sendable, CustomStringConvertible {
    /// Fully operational (can be queried)
    ///
    /// **Behavior**:
    /// - Maintained on record save/delete
    /// - Can be used in queries
    case readable = 0

    /// Not maintained (not usable)
    ///
    /// **Behavior**:
    /// - Not maintained on record save/delete
    /// - Cannot be used in queries
    /// - Initial state when index is first defined
    case disabled = 1

    /// Maintained but not queryable (under construction)
    ///
    /// **Behavior**:
    /// - Maintained on record save/delete
    /// - Cannot be used in queries
    /// - State during online index construction
    case writeOnly = 2

    /// Whether index should be maintained on record changes
    ///
    /// **Examples**:
    /// ```swift
    /// IndexState.readable.shouldMaintain   // true
    /// IndexState.writeOnly.shouldMaintain  // true
    /// IndexState.disabled.shouldMaintain   // false
    /// ```
    public var shouldMaintain: Bool {
        switch self {
        case .readable, .writeOnly:
            return true
        case .disabled:
            return false
        }
    }

    /// Whether index can be used in queries
    ///
    /// **Examples**:
    /// ```swift
    /// IndexState.readable.isReadable   // true
    /// IndexState.writeOnly.isReadable  // false
    /// IndexState.disabled.isReadable   // false
    /// ```
    public var isReadable: Bool {
        switch self {
        case .readable:
            return true
        case .writeOnly, .disabled:
            return false
        }
    }

    public var description: String {
        switch self {
        case .readable:
            return "readable"
        case .disabled:
            return "disabled"
        case .writeOnly:
            return "writeOnly"
        }
    }
}
