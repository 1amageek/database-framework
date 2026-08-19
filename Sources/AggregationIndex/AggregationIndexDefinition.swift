import DatabaseEngine
import DatabaseKit

extension ResolvedIndex {
    func aggregateDefinition(
        _ expectedFunction: AggregateFunctionType
    ) throws(IndexMaintainerProviderError) -> (
        function: AggregateIndexFunction,
        groupBy: [IndexKey<FieldIdentity>],
        value: FieldIdentity?
    ) {
        guard case .aggregate(let function, let groupBy, let value) = definition,
              function.type == expectedFunction else {
            throw .typeMismatch(
                registered: .aggregate(expectedFunction),
                actual: type
            )
        }
        return (function, groupBy, value)
    }

    func aggregateValueType(
        _ expectedFunction: AggregateFunctionType
    ) throws(IndexMaintainerProviderError) -> IndexScalarType {
        let declaration = try aggregateDefinition(expectedFunction)
        guard declaration.value != nil,
              let valueType = descriptor.keyFieldSchemas.last?.indexScalarType
        else {
            throw .invalidDefinition(
                indexType: .aggregate(expectedFunction),
                reason: "The aggregate value field has no scalar runtime type"
            )
        }
        return valueType
    }
}
