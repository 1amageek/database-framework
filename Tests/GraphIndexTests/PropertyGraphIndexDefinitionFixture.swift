import DatabaseKit

func propertyGraphIndexDefinition(
    source: FieldIdentity,
    label: FieldIdentity,
    target: FieldIdentity,
    namespace: FieldIdentity? = nil,
    strategy: PropertyGraphIndexStrategy
) -> GraphIndexDefinition<FieldIdentity> {
    .property(
        source: source,
        label: .field(label),
        target: target,
        graph: namespace,
        strategy: strategy
    )
}
