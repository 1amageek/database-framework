import DatabaseKit

/// Pure SQL text generation with one cumulative invocation budget.
public protocol SQLSerializing: Sendable {
    func serialize(_ statement: QueryStatement) throws(SQLSerializationError) -> String
    func serialize(_ statements: [QueryStatement]) throws(SQLSerializationError) -> String
    func serialize(_ query: SelectQuery) throws(SQLSerializationError) -> String
    func serialize(_ expression: Expression) throws(SQLSerializationError) -> String
    func serialize(_ literal: Literal) throws(SQLSerializationError) -> String
    func serialize(_ type: DataType) throws(SQLSerializationError) -> String
    func serialize(_ source: DataSource) throws(SQLSerializationError) -> String
    func serialize(_ aggregate: AggregateFunction) throws(SQLSerializationError) -> String
    func serialize(_ pattern: MatchPattern) throws(SQLSerializationError) -> String
    func serialize(_ path: PathPattern) throws(SQLSerializationError) -> String
    func serialize(_ element: PathElement) throws(SQLSerializationError) -> String
    func serialize(_ node: NodePattern) throws(SQLSerializationError) -> String
    func serialize(_ edge: EdgePattern) throws(SQLSerializationError) -> String
    func serialize(_ quantifier: PathQuantifier) throws(SQLSerializationError) -> String
    func serialize(_ mode: PathMode) throws(SQLSerializationError) -> String
    func serialize(_ source: GraphTableSource) throws(SQLSerializationError) -> String
    func serialize(_ graph: CreateGraphStatement) throws(SQLSerializationError) -> String
    func serialize(_ vertex: VertexTableDefinition) throws(SQLSerializationError) -> String
    func serialize(_ edge: EdgeTableDefinition) throws(SQLSerializationError) -> String
    func serialize(_ reference: VertexReference) throws(SQLSerializationError) -> String
    func serialize(_ label: LabelExpression) throws(SQLSerializationError) -> String
    func serialize(_ properties: PropertiesSpec) throws(SQLSerializationError) -> String
    func serialize(_ join: JoinType) throws(SQLSerializationError) -> String
}
