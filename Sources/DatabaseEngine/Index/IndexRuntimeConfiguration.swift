/// Deployment-specific configuration for one compiled index.
///
/// The model declaration owns logical index meaning. This contract owns only
/// execution policy that may vary between database runtimes, such as algorithm
/// selection, memory budgets, or physical subspace isolation.
public protocol IndexRuntimeConfiguration: Sendable {
    /// Identifier of the compiled index kind this configuration serves.
    static var kindIdentifier: String { get }

    /// Canonical persisted field selected by the compiled model schema.
    var fieldName: String { get }

    /// Canonical persisted entity selected by the compiled model schema.
    var entityName: String { get }

    /// Stable compiled index name.
    var indexName: String { get }

    /// Optional physical subdivision within the compiled index.
    var subspaceKey: String? { get }
}

extension IndexRuntimeConfiguration {
    public var indexName: String {
        "\(entityName)_\(fieldName)"
    }

    public var subspaceKey: String? {
        nil
    }
}
