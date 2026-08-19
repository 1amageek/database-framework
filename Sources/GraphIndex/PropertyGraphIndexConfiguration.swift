import DatabaseKit

/// Runtime field layout derived from a validated property-graph declaration.
@_spi(DatabaseExecution)
public struct PropertyGraphIndexConfiguration: Sendable, Hashable {
    public let sourceFieldName: String
    public let labelFieldName: String?
    public let targetFieldName: String
    public let namespaceFieldName: String?
    public let declarativeStrategy: PropertyGraphIndexStrategy

    public var strategy: GraphIndexStrategy {
        declarativeStrategy.storageStrategy
    }

    public init?(descriptor: IndexDescriptor) {
        guard case .graph(
            .property(
                let source,
                let label,
                let target,
                let graph,
                let strategy
            ), _
        ) = descriptor.declaration.definition else {
            return nil
        }
        self.sourceFieldName = source.name
        switch label {
        case .field(let field):
            self.labelFieldName = field.name
        case .implicit:
            self.labelFieldName = nil
        }
        self.targetFieldName = target.name
        self.namespaceFieldName = graph?.name
        self.declarativeStrategy = strategy
    }
}
