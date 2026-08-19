import DatabaseKit
import DatabaseTypes

/// Deployment-specific configuration for one compiled index.
///
/// The model declaration owns logical index meaning. This contract owns only
/// execution policy that may vary between database runtimes, such as algorithm
/// selection or memory budgets.
public protocol IndexRuntimeConfiguration: Sendable {
    /// Semantic index type this configuration serves.
    static var indexType: IndexType { get }

    /// Semantic index type this value serves.
    var indexType: IndexType { get }

    /// Explicit stable name of the compiled index this policy targets.
    var indexName: String { get }

    /// Module-owned execution policy represented by canonical primitive values.
    ///
    /// DatabaseEngine preserves these values without interpreting a module's
    /// algorithm-specific policy.
    var executionOptions: FieldObject { get throws }
}

extension IndexRuntimeConfiguration {
    public var indexType: IndexType {
        Self.indexType
    }

    public var executionOptions: FieldObject {
        get throws {
            FieldObject()
        }
    }
}
