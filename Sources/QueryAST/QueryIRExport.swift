@_exported import QueryIR

// Disambiguate from Foundation.Expression (macOS 15+)
public typealias Expression = QueryIR.Expression
public typealias Literal = QueryIR.Literal
public typealias Variable = QueryIR.Variable
public typealias ColumnRef = QueryIR.ColumnRef
public typealias SelectQuery = QueryIR.SelectQuery
public typealias DataSource = QueryIR.DataSource
public typealias SortKey = QueryIR.SortKey
public typealias SortDirection = QueryIR.SortDirection
public typealias NullOrdering = QueryIR.NullOrdering
public typealias FunctionCall = QueryIR.FunctionCall
public typealias AggregateFunction = QueryIR.AggregateFunction
public typealias AggregateBinding = QueryIR.AggregateBinding
public typealias DataType = QueryIR.DataType
public typealias Projection = QueryIR.Projection
public typealias ProjectionItem = QueryIR.ProjectionItem
public typealias QueryStatement = QueryIR.QueryStatement
