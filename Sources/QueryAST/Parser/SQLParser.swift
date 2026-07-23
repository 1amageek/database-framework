/// SQLParser.swift
/// SQL Query Parser
///
/// Reference:
/// - ISO/IEC 9075:2023 (SQL)
/// - ISO/IEC 9075-16:2023 (SQL/PGQ)

import QueryIR

/// SQL Parser for converting SQL strings to AST
public final class SQLParser {
    /// Parser errors
    public enum ParseError: Error, Sendable, Equatable {
        case unexpectedToken(expected: String, found: String, position: Int)
        case unexpectedEndOfInput(expected: String)
        case invalidSyntax(message: String, position: Int)
        case unsupportedFeature(String)
    }

    /// Token types
    private enum Token: Sendable, Equatable {
        case keyword(String)
        case identifier(String)
        case string(String)
        case number(String)
        case parameter(QueryParameterReference)
        case invalidParameter(String)
        case symbol(String)
        case eof
    }

    private enum PositionalParameterStyle: Sendable {
        case anonymous
        case explicit
    }

    private enum EdgeEndpointRole: String, Sendable {
        case source
        case destination

        var displayName: String {
            switch self {
            case .source: "Source"
            case .destination: "Destination"
            }
        }
    }

    private var input: String
    private var position: String.Index
    private var currentToken: Token
    private var anonymousParameterPosition: UInt32
    private var positionalParameterStyle: PositionalParameterStyle?
    private let structuralLimits: QueryStructuralLimits
    private var structuralLedger: QueryStructuralResourceLedger
    private var structuralError: QueryStructuralValidationError?

    public init(
        structuralLimits: QueryStructuralLimits = .default
    ) {
        self.input = ""
        self.position = "".startIndex
        self.currentToken = .eof
        self.anonymousParameterPosition = 0
        self.positionalParameterStyle = nil
        self.structuralLimits = structuralLimits
        self.structuralLedger = QueryStructuralResourceLedger(
            limits: structuralLimits
        )
        self.structuralError = nil
    }

    /// Parse a SQL SELECT query
    public func parseSelect(_ sql: String) throws -> SelectQuery {
        resetParserState(for: sql)
        return try withStructuralErrorPrecedence {
            advance()
            let query = try parseSelectQuery()
            try consumeStatementTerminator()
            try QueryStructuralValidator.validate(
                query,
                limits: structuralLimits
            )
            return query
        }
    }

    /// Parse any SQL statement
    public func parse(_ sql: String) throws -> QueryStatement {
        resetParserState(for: sql)
        return try withStructuralErrorPrecedence {
            advance()
            let statement = try parseStatement()
            try consumeStatementTerminator()
            try QueryStructuralValidator.validate(
                statement,
                limits: structuralLimits
            )
            return statement
        }
    }

    private func resetParserState(for sql: String) {
        input = sql
        position = input.startIndex
        anonymousParameterPosition = 0
        positionalParameterStyle = nil
        structuralLedger = QueryStructuralResourceLedger(
            limits: structuralLimits
        )
        structuralError = nil
    }

    private func withStructuralErrorPrecedence<Result>(
        _ operation: () throws -> Result
    ) throws -> Result {
        do {
            let result = try operation()
            if let structuralError {
                throw structuralError
            }
            return result
        } catch {
            if let structuralError {
                throw structuralError
            }
            throw error
        }
    }

    private func enterStructuralNesting() throws {
        try structuralLedger.enterNesting()
    }

    private func leaveStructuralNesting() {
        structuralLedger.leaveNesting()
    }

    private func makeStructuralNode<Value>(
        _ value: @autoclosure () -> Value
    ) throws -> Value {
        try structuralLedger.consume(.totalNodes)
        return value()
    }

    private func makeLiteralExpression(
        _ literal: Literal
    ) throws -> Expression {
        let admittedLiteral = try makeStructuralNode(literal)
        return try makeStructuralNode(.literal(admittedLiteral))
    }

    private func makeAggregateExpression(
        _ aggregate: AggregateFunction
    ) throws -> Expression {
        let admittedAggregate = try makeStructuralNode(aggregate)
        return try makeStructuralNode(.aggregate(admittedAggregate))
    }

    private func admitCollectionElement(
        amount: UInt64 = 1
    ) throws {
        try structuralLedger.consume(.collectionElements, amount: amount)
    }
}

// MARK: - Tokenizer

extension SQLParser {
    private func admitToken(_ token: Token) {
        guard structuralError == nil else {
            currentToken = .eof
            return
        }
        do {
            try structuralLedger.consume(.inputTokens)
            currentToken = token
        } catch {
            structuralError = error
            currentToken = .eof
        }
    }

    private func advance() {
        skipWhitespace()

        guard position < input.endIndex else {
            currentToken = .eof
            return
        }

        let char = input[position]

        if char == "?" {
            position = input.index(after: position)
            guard positionalParameterStyle != .explicit else {
                admitToken(.invalidParameter("Cannot mix '?' and '$n' positional parameters"))
                return
            }
            positionalParameterStyle = .anonymous
            guard anonymousParameterPosition < UInt32.max else {
                admitToken(.invalidParameter("Anonymous parameter position overflow"))
                return
            }
            anonymousParameterPosition += 1
            admitToken(.parameter(.position(anonymousParameterPosition)))
            return
        }

        if char == "$" {
            let marker = position
            position = input.index(after: position)
            let start = position
            while position < input.endIndex, input[position].isNumber {
                position = input.index(after: position)
            }
            if position < input.endIndex,
               input[position].isLetter || input[position] == "_" {
                while position < input.endIndex,
                      input[position].isLetter || input[position].isNumber || input[position] == "_" {
                    position = input.index(after: position)
                }
                admitToken(.symbol(String(input[marker..<position])))
            } else if start < position,
               let value = UInt32(String(input[start..<position])),
               value > 0 {
                if positionalParameterStyle == .anonymous {
                    admitToken(.invalidParameter("Cannot mix '?' and '$n' positional parameters"))
                } else {
                    positionalParameterStyle = .explicit
                    admitToken(.parameter(.position(value)))
                }
            } else {
                admitToken(.invalidParameter(String(input[marker..<position])))
            }
            return
        }

        // Keywords and identifiers
        if char.isLetter || char == "_" {
            let start = position
            while position < input.endIndex && (input[position].isLetter || input[position].isNumber || input[position] == "_") {
                position = input.index(after: position)
            }
            let word = String(input[start..<position])
            let upper = word.uppercased()

            // Check if keyword
            let keywords = ["SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN",
                           "IS", "NULL", "TRUE", "FALSE", "AS", "JOIN", "INNER", "LEFT", "RIGHT",
                           "FULL", "CROSS", "ON", "USING", "GROUP", "BY", "HAVING", "ORDER", "ASC",
                           "DESC", "LIMIT", "OFFSET", "DISTINCT", "ALL", "UNION", "INTERSECT",
                           "EXCEPT", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
                           "CREATE", "DROP", "TABLE", "INDEX", "GRAPH", "PROPERTY", "MATCH",
                           "WITH", "CASE", "WHEN", "THEN", "ELSE", "END", "CAST", "COUNT",
                           "SUM", "AVG", "MIN", "MAX", "EXISTS", "ANY", "SOME", "NULLS",
                           "FIRST", "LAST", "OVER", "PARTITION", "ROWS", "RANGE",
                           "GRAPH_TABLE", "COLUMNS", "WALK", "TRAIL", "ACYCLIC", "SIMPLE",
                           "SHORTEST", "DEFAULT", "CONFLICT", "DO", "NOTHING", "RETURNING",
                           "IF", "VERTEX", "TABLES", "EDGE", "KEY", "LABEL",
                           "DESTINATION", "REFERENCES", "PROPERTIES", "NO"]

            if keywords.contains(upper) {
                admitToken(.keyword(upper))
            } else {
                admitToken(.identifier(word))
            }
            return
        }

        // Numbers
        if char.isNumber {
            let start = position
            var hasDecimalPoint = false
            var hasExponent = false
            while position < input.endIndex {
                let current = input[position]
                if current.isNumber {
                    position = input.index(after: position)
                } else if current == "." && !hasDecimalPoint && !hasExponent {
                    hasDecimalPoint = true
                    position = input.index(after: position)
                } else if (current == "e" || current == "E") && !hasExponent {
                    hasExponent = true
                    position = input.index(after: position)
                    if position < input.endIndex,
                       input[position] == "+" || input[position] == "-" {
                        position = input.index(after: position)
                    }
                } else {
                    break
                }
            }
            admitToken(.number(String(input[start..<position])))
            return
        }

        // Strings - SQL standard: use '' to escape single quotes
        // Reference: ISO/IEC 9075:2023 Section 5.3 <character string literal>
        if char == "'" {
            position = input.index(after: position)
            var value = ""
            while position < input.endIndex {
                let c = input[position]
                if c == "'" {
                    // Check for escaped quote ('')
                    let next = input.index(after: position)
                    if next < input.endIndex && input[next] == "'" {
                        // Escaped quote: '' -> '
                        value.append("'")
                        position = input.index(after: next)
                    } else {
                        // End of string
                        position = next
                        break
                    }
                } else {
                    value.append(c)
                    position = input.index(after: position)
                }
            }
            admitToken(.string(value))
            return
        }

        // Multi-character symbols
        let twoChar = String(input[position...].prefix(2))
        if ["<=", ">=", "<>", "!=", "||", "&&", "->", "<-"].contains(twoChar) {
            position = input.index(position, offsetBy: 2)
            admitToken(.symbol(twoChar))
            return
        }

        // Single character symbols
        position = input.index(after: position)
        admitToken(.symbol(String(char)))
    }

    private func skipWhitespace() {
        while position < input.endIndex {
            let char = input[position]
            if char.isWhitespace {
                position = input.index(after: position)
            } else if char == "-" && input.index(after: position) < input.endIndex && input[input.index(after: position)] == "-" {
                // Single-line comment
                while position < input.endIndex && input[position] != "\n" {
                    position = input.index(after: position)
                }
            } else if char == "/" && input.index(after: position) < input.endIndex && input[input.index(after: position)] == "*" {
                // Multi-line comment
                position = input.index(position, offsetBy: 2)
                while position < input.endIndex {
                    if input[position] == "*" && input.index(after: position) < input.endIndex && input[input.index(after: position)] == "/" {
                        position = input.index(position, offsetBy: 2)
                        break
                    }
                    position = input.index(after: position)
                }
            } else {
                break
            }
        }
    }

    private func expect(_ tokenType: String) throws {
        switch currentToken {
        case .keyword(let kw) where kw == tokenType:
            advance()
        case .symbol(let s) where s == tokenType:
            advance()
        default:
            throw ParseError.unexpectedToken(
                expected: tokenType,
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    private func tokenDescription(_ token: Token) -> String {
        switch token {
        case .keyword(let k): return "keyword '\(k)'"
        case .identifier(let i): return "identifier '\(i)'"
        case .string(let s): return "string '\(s)'"
        case .number(let n): return "number '\(n)'"
        case .parameter(let parameter): return "parameter '\(parameter)'"
        case .invalidParameter(let reason): return "invalid parameter '\(reason)'"
        case .symbol(let s): return "symbol '\(s)'"
        case .eof: return "end of input"
        }
    }

    private func isSymbol(_ s: String) -> Bool {
        if case .symbol(let sym) = currentToken {
            return sym == s
        }
        return false
    }

    private func isKeyword(_ k: String) -> Bool {
        if case .keyword(let kw) = currentToken {
            return kw == k
        }
        return false
    }

    @discardableResult
    private func consumeContextualWord(_ word: String) -> Bool {
        switch currentToken {
        case .identifier(let value) where value.uppercased() == word:
            advance()
            return true
        case .keyword(let value) where value == word:
            advance()
            return true
        default:
            return false
        }
    }

    private func expectContextualWord(_ word: String) throws {
        guard consumeContextualWord(word) else {
            throw ParseError.unexpectedToken(
                expected: word,
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    private func consumeStatementTerminator() throws {
        if case .symbol(";") = currentToken {
            advance()
        }
        guard case .eof = currentToken else {
            throw ParseError.unexpectedToken(
                expected: "end of input",
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }
}

// MARK: - Statement Parsing

extension SQLParser {
    private func parseStatement() throws -> QueryStatement {
        switch currentToken {
        case .keyword("SELECT"):
            return .select(try parseSelectQuery())
        case .keyword("INSERT"):
            return .insert(try parseInsertQuery())
        case .keyword("UPDATE"):
            return .update(try parseUpdateQuery())
        case .keyword("DELETE"):
            return .delete(try parseDeleteQuery())
        case .keyword("CREATE"):
            advance()
            if case .keyword("PROPERTY") = currentToken {
                return .createGraph(try parseCreateGraph())
            }
            throw ParseError.unsupportedFeature("CREATE statement type")
        case .keyword("DROP"):
            advance()
            try expect("PROPERTY")
            try expect("GRAPH")
            guard case .identifier(let graphName) = currentToken else {
                throw ParseError.unexpectedToken(
                    expected: "property graph name",
                    found: tokenDescription(currentToken),
                    position: input.distance(
                        from: input.startIndex,
                        to: position
                    )
                )
            }
            advance()
            return .dropGraph(graphName)
        default:
            throw ParseError.invalidSyntax(
                message: "Expected statement keyword",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    private func parseSelectQuery() throws -> SelectQuery {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }

        // WITH clause (CTE) support
        var subqueries: [NamedSubquery]?
        if case .keyword("WITH") = currentToken {
            subqueries = try parseWithClause()
        }

        try expect("SELECT")

        // DISTINCT
        var distinct = false
        if case .keyword("DISTINCT") = currentToken {
            distinct = true
            advance()
        }

        // Projection
        let projection = try parseProjection()

        // FROM
        let source: DataSource
        if case .keyword("FROM") = currentToken {
            advance()
            source = try parseDataSource()
        } else {
            source = try makeStructuralNode(.table(TableRef("")))
        }

        // WHERE
        var filter: Expression?
        if case .keyword("WHERE") = currentToken {
            advance()
            filter = try parseExpression()
        }

        // GROUP BY
        var groupBy: [Expression]?
        if case .keyword("GROUP") = currentToken {
            advance()
            try expect("BY")
            groupBy = try parseExpressionList()
        }

        // HAVING
        var having: Expression?
        if case .keyword("HAVING") = currentToken {
            advance()
            having = try parseExpression()
        }

        // ORDER BY
        var orderBy: [SortKey]?
        if case .keyword("ORDER") = currentToken {
            advance()
            try expect("BY")
            orderBy = try parseOrderBy()
        }

        // LIMIT
        var limit: Int?
        if case .keyword("LIMIT") = currentToken {
            advance()
            if case .number(let n) = currentToken {
                limit = Int(n)
                advance()
            }
        }

        // OFFSET
        var offset: Int?
        if case .keyword("OFFSET") = currentToken {
            advance()
            if case .number(let n) = currentToken {
                offset = Int(n)
                advance()
            }
        }

        return try makeStructuralNode(
            SelectQuery(
                projection: projection,
                source: source,
                filter: filter,
                groupBy: groupBy,
                having: having,
                orderBy: orderBy,
                limit: limit,
                offset: offset,
                distinct: distinct,
                subqueries: subqueries
            )
        )
    }

    private func parseWithClause() throws -> [NamedSubquery] {
        try expect("WITH")

        // RECURSIVE keyword (semantic check only)
        if case .keyword("RECURSIVE") = currentToken {
            advance()
        }

        var subqueries: [NamedSubquery] = []
        var first = true

        while first || isSymbol(",") {
            if !first { advance() }
            first = false

            guard case .identifier(let name) = currentToken else {
                throw ParseError.unexpectedToken(
                    expected: "CTE name",
                    found: tokenDescription(currentToken),
                    position: input.distance(from: input.startIndex, to: position)
                )
            }
            advance()

            // Optional column list: name(col1, col2)
            var columnList: [String]?
            if isSymbol("(") {
                advance()
                columnList = []
                var colFirst = true
                while colFirst || isSymbol(",") {
                    if !colFirst { advance() }
                    colFirst = false
                    if case .identifier(let col) = currentToken {
                        try admitCollectionElement()
                        columnList?.append(col)
                        advance()
                    }
                }
                try expect(")")
            }

            try expect("AS")

            // Materialization hint (optional)
            var materialized: Materialization?
            if case .keyword("MATERIALIZED") = currentToken {
                materialized = .materialized
                advance()
            } else if case .keyword("NOT") = currentToken {
                advance()
                // NOT must be followed by MATERIALIZED
                try expect("MATERIALIZED")
                materialized = .notMaterialized
            }

            try expect("(")
            let query = try parseSelectQuery()
            try expect(")")

            try admitCollectionElement()
            subqueries.append(
                try makeStructuralNode(
                    NamedSubquery(
                        name: name,
                        columns: columnList,
                        query: query,
                        materialized: materialized
                    )
                )
            )
        }

        return subqueries
    }

    private func parseProjection() throws -> Projection {
        if case .symbol("*") = currentToken {
            advance()
            return .all
        }

        var items: [ProjectionItem] = []
        var first = true
        while first || isSymbol(",") {
            if !first {
                advance()
            }
            first = false
            let expr = try parseExpression()
            var alias: String?
            if case .keyword("AS") = currentToken {
                advance()
                if case .identifier(let name) = currentToken {
                    alias = name
                    advance()
                }
            }
            try admitCollectionElement()
            items.append(
                try makeStructuralNode(
                    ProjectionItem(expr, alias: alias)
                )
            )
        }

        return .items(items)
    }

    private func parseDataSource() throws -> DataSource {
        let source = try parseTableRef()

        // Check for JOINs
        var result = source
        while case .keyword(let kw) = currentToken, ["INNER", "LEFT", "RIGHT", "FULL", "CROSS", "JOIN"].contains(kw) {
            let joinType = try parseJoinType()
            let right = try parseTableRef()
            var condition: JoinCondition?

            if case .keyword("ON") = currentToken {
                advance()
                condition = .on(try parseExpression())
            } else if case .keyword("USING") = currentToken {
                advance()
                try expect("(")
                var cols: [String] = []
                var first = true
                while first || isSymbol(",") {
                    if !first { advance() }
                    first = false
                    if case .identifier(let name) = currentToken {
                        try admitCollectionElement()
                        cols.append(name)
                        advance()
                    } else {
                        break
                    }
                }
                try expect(")")
                condition = .using(cols)
            }

            result = try makeStructuralNode(
                .join(
                    JoinClause(
                        type: joinType,
                        left: result,
                        right: right,
                        condition: condition
                    )
                )
            )
        }

        return result
    }

    private func parseTableRef() throws -> DataSource {
        // Check for GRAPH_TABLE
        if case .keyword("GRAPH_TABLE") = currentToken {
            return try parseGraphTable()
        }

        // Check for subquery: (SELECT ...) or (WITH ...)
        if isSymbol("(") {
            advance()

            // Verify it's a subquery (starts with SELECT or WITH)
            switch currentToken {
            case .keyword("SELECT"), .keyword("WITH"):
                break  // Valid subquery start
            default:
                throw ParseError.invalidSyntax(
                    message: "Expected SELECT or WITH after '(' in FROM clause",
                    position: input.distance(from: input.startIndex, to: position)
                )
            }

            let subquery = try parseSelectQuery()
            try expect(")")

            // Alias is required for subqueries
            var alias: String?
            if case .keyword("AS") = currentToken {
                advance()
            }
            if case .identifier(let a) = currentToken {
                alias = a
                advance()
            }

            guard let subqueryAlias = alias else {
                throw ParseError.invalidSyntax(
                    message: "Subquery in FROM clause requires an alias",
                    position: input.distance(from: input.startIndex, to: position)
                )
            }

            return try makeStructuralNode(
                .subquery(subquery, alias: subqueryAlias)
            )
        }

        let table = try parseNamedTableReference(allowsAlias: true)
        return try makeStructuralNode(
            .table(table)
        )
    }

    private func parseNamedTableReference(
        allowsAlias: Bool
    ) throws -> TableRef {
        guard case .identifier(let firstName) = currentToken else {
            throw ParseError.unexpectedToken(
                expected: "table name",
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        advance()

        let schema: String?
        let table: String
        if isSymbol(".") {
            advance()
            guard case .identifier(let tableName) = currentToken else {
                throw ParseError.unexpectedToken(
                    expected: "table name after schema qualifier",
                    found: tokenDescription(currentToken),
                    position: input.distance(
                        from: input.startIndex,
                        to: position
                    )
                )
            }
            schema = firstName
            table = tableName
            advance()
        } else {
            schema = nil
            table = firstName
        }

        var alias: String?
        if allowsAlias, isKeyword("AS") {
            advance()
            guard case .identifier(let name) = currentToken else {
                throw ParseError.unexpectedToken(
                    expected: "table alias",
                    found: tokenDescription(currentToken),
                    position: input.distance(
                        from: input.startIndex,
                        to: position
                    )
                )
            }
            alias = name
            advance()
        } else if allowsAlias, case .identifier(let name) = currentToken {
            alias = name
            advance()
        }

        return TableRef(schema: schema, table: table, alias: alias)
    }

    private func parseJoinType() throws -> JoinType {
        var joinType: JoinType = .inner

        switch currentToken {
        case .keyword("LEFT"):
            joinType = .left
            advance()
        case .keyword("RIGHT"):
            joinType = .right
            advance()
        case .keyword("FULL"):
            joinType = .full
            advance()
        case .keyword("CROSS"):
            joinType = .cross
            advance()
        case .keyword("INNER"):
            advance()
        default:
            break
        }

        if case .keyword("JOIN") = currentToken {
            advance()
        }

        return joinType
    }

    private func parseExpression() throws -> Expression {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }
        return try parseOrExpression()
    }

    private func parseOrExpression() throws -> Expression {
        var left = try parseAndExpression()
        while case .keyword("OR") = currentToken {
            advance()
            let right = try parseAndExpression()
            left = try makeStructuralNode(.or(left, right))
        }
        return left
    }

    private func parseAndExpression() throws -> Expression {
        var left = try parseNotExpression()
        while case .keyword("AND") = currentToken {
            advance()
            let right = try parseNotExpression()
            left = try makeStructuralNode(.and(left, right))
        }
        return left
    }

    private func parseNotExpression() throws -> Expression {
        var operatorCount = 0
        while case .keyword("NOT") = currentToken {
            try enterStructuralNesting()
            operatorCount += 1
            advance()
        }
        defer {
            for _ in 0..<operatorCount {
                leaveStructuralNesting()
            }
        }
        var expression = try parseComparisonExpression()
        for _ in 0..<operatorCount {
            expression = try makeStructuralNode(.not(expression))
        }
        return expression
    }

    private func parseComparisonExpression() throws -> Expression {
        let left = try parseAddExpression()

        switch currentToken {
        case .symbol("="):
            advance()
            let right = try parseAddExpression()
            return try makeStructuralNode(.equal(left, right))
        case .symbol("<>"), .symbol("!="):
            advance()
            let right = try parseAddExpression()
            return try makeStructuralNode(.notEqual(left, right))
        case .symbol("<"):
            advance()
            let right = try parseAddExpression()
            return try makeStructuralNode(.lessThan(left, right))
        case .symbol("<="):
            advance()
            let right = try parseAddExpression()
            return try makeStructuralNode(.lessThanOrEqual(left, right))
        case .symbol(">"):
            advance()
            let right = try parseAddExpression()
            return try makeStructuralNode(.greaterThan(left, right))
        case .symbol(">="):
            advance()
            let right = try parseAddExpression()
            return try makeStructuralNode(.greaterThanOrEqual(left, right))
        case .keyword("IS"):
            advance()
            let notNull = currentToken == .keyword("NOT")
            if notNull { advance() }
            try expect("NULL")
            if notNull {
                return try makeStructuralNode(.isNotNull(left))
            }
            return try makeStructuralNode(.isNull(left))
        case .keyword("LIKE"):
            advance()
            if case .string(let pattern) = currentToken {
                advance()
                return try makeStructuralNode(.like(left, pattern: pattern))
            }
            throw ParseError.invalidSyntax(message: "Expected string pattern after LIKE", position: input.distance(from: input.startIndex, to: position))
        case .keyword("IN"):
            advance()
            try expect("(")

            // Check if it's a subquery or value list
            if case .keyword("SELECT") = currentToken {
                let subquery = try parseSelectQuery()
                try expect(")")
                return try makeStructuralNode(
                    .inSubquery(left, subquery: subquery)
                )
            }

            // Handle WITH clause (CTE) that starts a subquery
            if case .keyword("WITH") = currentToken {
                let subquery = try parseSelectQuery()
                try expect(")")
                return try makeStructuralNode(
                    .inSubquery(left, subquery: subquery)
                )
            }

            // Value list (existing logic)
            var values: [Expression] = []
            var first = true
            while first || isSymbol(",") {
                if !first { advance() }
                first = false
                try admitCollectionElement()
                values.append(try parseExpression())
            }
            try expect(")")
            return try makeStructuralNode(.inList(left, values: values))
        case .keyword("BETWEEN"):
            advance()
            let low = try parseAddExpression()
            try expect("AND")
            let high = try parseAddExpression()
            return try makeStructuralNode(
                .between(left, low: low, high: high)
            )
        default:
            return left
        }
    }

    private func parseAddExpression() throws -> Expression {
        var left = try parseMulExpression()
        while case .symbol(let s) = currentToken, ["+", "-"].contains(s) {
            advance()
            let right = try parseMulExpression()
            if s == "+" {
                left = try makeStructuralNode(.add(left, right))
            } else {
                left = try makeStructuralNode(.subtract(left, right))
            }
        }
        return left
    }

    private func parseMulExpression() throws -> Expression {
        var left = try parseUnaryExpression()
        while case .symbol(let s) = currentToken, ["*", "/", "%"].contains(s) {
            advance()
            let right = try parseUnaryExpression()
            switch s {
            case "*": left = try makeStructuralNode(.multiply(left, right))
            case "/": left = try makeStructuralNode(.divide(left, right))
            case "%": left = try makeStructuralNode(.modulo(left, right))
            default: break
            }
        }
        return left
    }

    private func parseUnaryExpression() throws -> Expression {
        var operatorCount = 0
        while case .symbol("-") = currentToken {
            try enterStructuralNesting()
            operatorCount += 1
            advance()
        }
        defer {
            for _ in 0..<operatorCount {
                leaveStructuralNesting()
            }
        }
        var expression = try parsePrimaryExpression()
        for _ in 0..<operatorCount {
            expression = try makeStructuralNode(.negate(expression))
        }
        return expression
    }

    private func parsePrimaryExpression() throws -> Expression {
        switch currentToken {
        case .symbol("("):
            advance()
            // Disambiguate: subquery vs parenthesized expression
            if case .keyword("SELECT") = currentToken {
                let subquery = try parseSelectQuery()
                try expect(")")
                return try makeStructuralNode(.subquery(subquery))
            }
            // Handle WITH clause (CTE) that starts a subquery
            if case .keyword("WITH") = currentToken {
                let subquery = try parseSelectQuery()
                try expect(")")
                return try makeStructuralNode(.subquery(subquery))
            }
            let expr = try parseExpression()
            try expect(")")
            return expr

        case .keyword("EXISTS"):
            advance()
            try expect("(")
            let subquery = try parseSelectQuery()
            try expect(")")
            return try makeStructuralNode(.exists(subquery))

        case .number(let n):
            advance()
            if n.contains("e") || n.contains("E") {
                guard let value = Double(n), value.isFinite else {
                    throw invalidNumericLiteral(n)
                }
                return try makeLiteralExpression(.double(value))
            }
            if n.contains(".") {
                guard let value = Literal.parseDecimal(n) else {
                    throw invalidNumericLiteral(n)
                }
                return try makeLiteralExpression(value)
            }
            guard let value = Literal.parseInteger(n) else {
                throw invalidNumericLiteral(n)
            }
            return try makeLiteralExpression(value)

        case .string(let s):
            advance()
            return try makeLiteralExpression(.string(s))

        case .parameter(let reference):
            advance()
            return try makeStructuralNode(.parameter(reference))

        case .symbol(":"):
            advance()
            guard case .identifier(let name) = currentToken else {
                throw ParseError.invalidSyntax(
                    message: "Expected a named parameter identifier after ':'",
                    position: input.distance(from: input.startIndex, to: position)
                )
            }
            advance()
            return try makeStructuralNode(.parameter(.name(name)))

        case .invalidParameter(let reason):
            throw ParseError.invalidSyntax(
                message: reason,
                position: input.distance(from: input.startIndex, to: position)
            )

        case .keyword("TRUE"):
            advance()
            return try makeLiteralExpression(.bool(true))

        case .keyword("FALSE"):
            advance()
            return try makeLiteralExpression(.bool(false))

        case .keyword("NULL"):
            advance()
            return try makeLiteralExpression(.null)

        case .keyword("COUNT"), .keyword("SUM"), .keyword("AVG"), .keyword("MIN"), .keyword("MAX"):
            return try parseAggregate()

        case .keyword("CASE"):
            return try parseCaseExpression()

        case .identifier(let name):
            advance()
            // Check for function call
            if case .symbol("(") = currentToken {
                advance()
                var args: [Expression] = []
                if !isSymbol(")") {
                    var first = true
                    while first || isSymbol(",") {
                        if !first { advance() }
                        first = false
                        try admitCollectionElement()
                        args.append(try parseExpression())
                    }
                }
                try expect(")")
                return try makeStructuralNode(
                    .function(FunctionCall(name: name, arguments: args))
                )
            }
            // Check for qualified name
            if case .symbol(".") = currentToken {
                advance()
                if case .identifier(let col) = currentToken {
                    advance()
                    return try makeStructuralNode(
                        .column(ColumnRef(table: name, column: col))
                    )
                }
            }
            return try makeStructuralNode(
                .column(ColumnRef(column: name))
            )

        default:
            throw ParseError.unexpectedToken(
                expected: "expression",
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    private func invalidNumericLiteral(_ value: String) -> ParseError {
        .invalidSyntax(
            message: "Invalid or out-of-range numeric literal: \(value)",
            position: input.distance(from: input.startIndex, to: position)
        )
    }

    private func parseAggregate() throws -> Expression {
        let funcName = currentToken
        advance()
        try expect("(")

        var distinct = false
        if case .keyword("DISTINCT") = currentToken {
            distinct = true
            advance()
        }

        var arg: Expression?
        if case .symbol("*") = currentToken {
            advance()
            arg = nil
        } else {
            arg = try parseExpression()
        }

        try expect(")")

        switch funcName {
        case .keyword("COUNT"):
            return try makeAggregateExpression(
                .count(arg, distinct: distinct)
            )
        case .keyword("SUM"):
            let operand = try arg ?? makeLiteralExpression(.null)
            return try makeAggregateExpression(
                .sum(operand, distinct: distinct)
            )
        case .keyword("AVG"):
            let operand = try arg ?? makeLiteralExpression(.null)
            return try makeAggregateExpression(
                .avg(operand, distinct: distinct)
            )
        case .keyword("MIN"):
            let operand = try arg ?? makeLiteralExpression(.null)
            return try makeAggregateExpression(.min(operand))
        case .keyword("MAX"):
            let operand = try arg ?? makeLiteralExpression(.null)
            return try makeAggregateExpression(.max(operand))
        default:
            throw ParseError.invalidSyntax(message: "Unknown aggregate function", position: input.distance(from: input.startIndex, to: position))
        }
    }

    private func parseCaseExpression() throws -> Expression {
        try expect("CASE")

        var cases: [CaseWhenPair] = []
        while case .keyword("WHEN") = currentToken {
            advance()
            let condition = try parseExpression()
            try expect("THEN")
            let result = try parseExpression()
            try admitCollectionElement()
            cases.append(
                try makeStructuralNode(
                    CaseWhenPair(condition: condition, result: result)
                )
            )
        }

        var elseResult: Expression?
        if case .keyword("ELSE") = currentToken {
            advance()
            elseResult = try parseExpression()
        }

        try expect("END")

        return try makeStructuralNode(
            .caseWhen(cases: cases, elseResult: elseResult)
        )
    }

    private func parseExpressionList() throws -> [Expression] {
        var exprs: [Expression] = []
        var first = true
        while first || isSymbol(",") {
            if !first { advance() }
            first = false
            try admitCollectionElement()
            exprs.append(try parseExpression())
        }
        return exprs
    }

    private func parseOrderBy() throws -> [SortKey] {
        var keys: [SortKey] = []
        var first = true
        while first || isSymbol(",") {
            if !first { advance() }
            first = false
            let expr = try parseExpression()
            var direction: SortDirection = .ascending
            if case .keyword("DESC") = currentToken {
                direction = .descending
                advance()
            } else if case .keyword("ASC") = currentToken {
                advance()
            }
            var nulls: NullOrdering?
            if case .keyword("NULLS") = currentToken {
                advance()
                if case .keyword("FIRST") = currentToken {
                    nulls = .first
                    advance()
                } else if case .keyword("LAST") = currentToken {
                    nulls = .last
                    advance()
                }
            }
            try admitCollectionElement()
            keys.append(
                try makeStructuralNode(
                    SortKey(expr, direction: direction, nulls: nulls)
                )
            )
        }
        return keys
    }

    private func parseInsertQuery() throws -> InsertQuery {
        try expect("INSERT")
        try expect("INTO")
        let target = try parseNamedTableReference(allowsAlias: false)

        let columns: [String]?
        if isSymbol("(") {
            columns = try parseIdentifierList(
                openingConsumed: false,
                expected: "INSERT column"
            )
        } else {
            columns = nil
        }

        let source: InsertSource
        if isKeyword("VALUES") {
            advance()
            var rows: [[Expression]] = []
            repeat {
                try admitCollectionElement()
                try expect("(")
                var row: [Expression] = []
                guard !isSymbol(")") else {
                    throw ParseError.invalidSyntax(
                        message: "INSERT VALUES rows cannot be empty",
                        position: input.distance(
                            from: input.startIndex,
                            to: position
                        )
                    )
                }
                while true {
                    try admitCollectionElement()
                    row.append(try parseExpression())
                    guard isSymbol(",") else { break }
                    advance()
                }
                try expect(")")
                rows.append(row)
                guard isSymbol(",") else { break }
                advance()
            } while true
            let valuesSource = InsertSource.values(rows)
            source = try makeStructuralNode(valuesSource)
        } else if isKeyword("SELECT") || isKeyword("WITH") {
            let query = try parseSelectQuery()
            source = try makeStructuralNode(.select(query))
        } else if isKeyword("DEFAULT") {
            advance()
            try expect("VALUES")
            source = try makeStructuralNode(.defaultValues)
        } else {
            throw ParseError.unexpectedToken(
                expected: "VALUES, SELECT, WITH, or DEFAULT VALUES",
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }

        var onConflict: OnConflictAction?
        if isKeyword("ON") {
            advance()
            try expect("CONFLICT")
            try expect("DO")
            if isKeyword("NOTHING") {
                advance()
                onConflict = try makeStructuralNode(.doNothing)
            } else if isKeyword("UPDATE") {
                advance()
                try expect("SET")
                let assignments = try parseAssignments()
                let predicate: Expression?
                if isKeyword("WHERE") {
                    advance()
                    predicate = try parseExpression()
                } else {
                    predicate = nil
                }
                onConflict = try makeStructuralNode(
                    .doUpdate(
                        assignments: assignments,
                        where: predicate
                    )
                )
            } else {
                throw ParseError.unexpectedToken(
                    expected: "NOTHING or UPDATE after ON CONFLICT DO",
                    found: tokenDescription(currentToken),
                    position: input.distance(
                        from: input.startIndex,
                        to: position
                    )
                )
            }
        }

        let returning = try parseReturningClause()
        return try makeStructuralNode(
            InsertQuery(
                target: target,
                columns: columns,
                source: source,
                onConflict: onConflict,
                returning: returning
            )
        )
    }

    private func parseUpdateQuery() throws -> UpdateQuery {
        try expect("UPDATE")
        let target = try parseNamedTableReference(allowsAlias: true)
        try expect("SET")
        let assignments = try parseAssignments()

        let source: DataSource?
        if isKeyword("FROM") {
            advance()
            source = try parseDataSource()
        } else {
            source = nil
        }

        let filter: Expression?
        if isKeyword("WHERE") {
            advance()
            filter = try parseExpression()
        } else {
            filter = nil
        }

        let returning = try parseReturningClause()
        return try makeStructuralNode(
            UpdateQuery(
                target: target,
                assignments: assignments,
                from: source,
                filter: filter,
                returning: returning
            )
        )
    }

    private func parseDeleteQuery() throws -> DeleteQuery {
        try expect("DELETE")
        try expect("FROM")
        let target = try parseNamedTableReference(allowsAlias: true)

        let source: DataSource?
        if isKeyword("USING") {
            advance()
            source = try parseDataSource()
        } else {
            source = nil
        }

        let filter: Expression?
        if isKeyword("WHERE") {
            advance()
            filter = try parseExpression()
        } else {
            filter = nil
        }

        let returning = try parseReturningClause()
        return try makeStructuralNode(
            DeleteQuery(
                target: target,
                using: source,
                filter: filter,
                returning: returning
            )
        )
    }

    private func parseCreateGraph() throws -> CreateGraphStatement {
        try expect("PROPERTY")
        try expect("GRAPH")

        let ifNotExists: Bool
        if isKeyword("IF") {
            advance()
            try expect("NOT")
            try expect("EXISTS")
            ifNotExists = true
        } else {
            ifNotExists = false
        }

        guard case .identifier(let graphName) = currentToken else {
            throw ParseError.unexpectedToken(
                expected: "property graph name",
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        advance()

        try expect("VERTEX")
        try expect("TABLES")
        try expect("(")
        var vertexTables: [VertexTableDefinition] = []
        guard !isSymbol(")") else {
            throw ParseError.invalidSyntax(
                message: "CREATE PROPERTY GRAPH requires at least one vertex table",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        while true {
            try admitCollectionElement()
            vertexTables.append(try parseVertexTableDefinition())
            guard isSymbol(",") else { break }
            advance()
        }
        try expect(")")

        var edgeTables: [EdgeTableDefinition] = []
        if isKeyword("EDGE") {
            advance()
            try expect("TABLES")
            try expect("(")
            guard !isSymbol(")") else {
                throw ParseError.invalidSyntax(
                    message: "EDGE TABLES cannot be empty",
                    position: input.distance(
                        from: input.startIndex,
                        to: position
                    )
                )
            }
            while true {
                try admitCollectionElement()
                edgeTables.append(try parseEdgeTableDefinition())
                guard isSymbol(",") else { break }
                advance()
            }
            try expect(")")
        }

        return try makeStructuralNode(
            CreateGraphStatement(
                graphName: graphName,
                ifNotExists: ifNotExists,
                vertexTables: vertexTables,
                edgeTables: edgeTables
            )
        )
    }

    private func parseVertexTableDefinition() throws -> VertexTableDefinition {
        let tableName = try parseRequiredIdentifier("vertex table name")
        let alias = try parseOptionalAlias()
        try expect("KEY")
        let keyColumns = try parseIdentifierList(
            openingConsumed: false,
            expected: "vertex key column"
        )
        let label = try parseOptionalLabelExpression()
        let properties = try parseOptionalPropertiesSpec()
        return try makeStructuralNode(
            VertexTableDefinition(
                tableName: tableName,
                alias: alias,
                keyColumns: keyColumns,
                labelExpression: label,
                propertiesSpec: properties
            )
        )
    }

    private func parseEdgeTableDefinition() throws -> EdgeTableDefinition {
        let tableName = try parseRequiredIdentifier("edge table name")
        let alias = try parseOptionalAlias()
        try expect("KEY")
        let keyColumns = try parseIdentifierList(
            openingConsumed: false,
            expected: "edge key column"
        )
        try expectContextualWord("SOURCE")
        let source = try parseVertexReference(role: .source)
        try expect("DESTINATION")
        let destination = try parseVertexReference(role: .destination)
        let label = try parseOptionalLabelExpression()
        let properties = try parseOptionalPropertiesSpec()
        return try makeStructuralNode(
            EdgeTableDefinition(
                tableName: tableName,
                alias: alias,
                keyColumns: keyColumns,
                sourceVertex: source,
                destinationVertex: destination,
                labelExpression: label,
                propertiesSpec: properties
            )
        )
    }

    private func parseVertexReference(
        role: EdgeEndpointRole
    ) throws -> VertexReference {
        try expect("KEY")
        let sourceColumns = try parseIdentifierList(
            openingConsumed: false,
            expected: "\(role.rawValue) edge key column"
        )
        try expect("REFERENCES")
        let tableName = try parseRequiredIdentifier(
            "\(role.rawValue) referenced vertex table"
        )
        let targetColumns = try parseIdentifierList(
            openingConsumed: false,
            expected: "\(role.rawValue) referenced key column"
        )
        guard sourceColumns.count == targetColumns.count else {
            throw ParseError.invalidSyntax(
                message: "\(role.displayName) key and referenced key widths must match",
                position: input.distance(from: input.startIndex, to: position)
            )
        }

        var mappings: [KeyColumnMapping] = []
        mappings.reserveCapacity(sourceColumns.count)
        for index in sourceColumns.indices {
            try admitCollectionElement()
            mappings.append(
                try makeStructuralNode(
                    KeyColumnMapping(
                        source: sourceColumns[index],
                        target: targetColumns[index]
                    )
                )
            )
        }
        return try makeStructuralNode(
            VertexReference(tableName: tableName, keyColumns: mappings)
        )
    }

    private func parseOptionalAlias() throws -> String? {
        guard isKeyword("AS") else { return nil }
        advance()
        return try parseRequiredIdentifier("table alias")
    }

    private func parseOptionalLabelExpression() throws -> LabelExpression? {
        guard isKeyword("LABEL") else { return nil }
        advance()
        return try parseLabelOrExpression()
    }

    private func parseLabelOrExpression() throws -> LabelExpression {
        let first = try parseLabelAndExpression()
        var expressions: [LabelExpression]?
        while isSymbol("|") {
            if expressions == nil {
                try admitCollectionElement(amount: 2)
                expressions = [first]
            } else {
                try admitCollectionElement()
            }
            advance()
            expressions?.append(try parseLabelAndExpression())
        }
        guard let expressions else { return first }
        return try makeStructuralNode(.or(expressions))
    }

    private func parseLabelAndExpression() throws -> LabelExpression {
        let first = try parseLabelPrimaryExpression()
        var expressions: [LabelExpression]?
        while isSymbol("&") {
            if expressions == nil {
                try admitCollectionElement(amount: 2)
                expressions = [first]
            } else {
                try admitCollectionElement()
            }
            advance()
            expressions?.append(try parseLabelPrimaryExpression())
        }
        guard let expressions else { return first }
        return try makeStructuralNode(.and(expressions))
    }

    private func parseLabelPrimaryExpression() throws -> LabelExpression {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }

        if isSymbol("(") {
            advance()
            if case .identifier(let column) = currentToken,
               peekNextToken() == .symbol(")") {
                advance()
                try expect(")")
                return try makeStructuralNode(.column(column))
            }
            let expression = try parseLabelOrExpression()
            try expect(")")
            return expression
        }

        let label = try parseRequiredIdentifier("label name")
        return try makeStructuralNode(.single(label))
    }

    private func parseOptionalPropertiesSpec() throws -> PropertiesSpec? {
        if isKeyword("NO") {
            advance()
            try expect("PROPERTIES")
            return try makeStructuralNode(PropertiesSpec.none)
        }
        guard isKeyword("PROPERTIES") else { return nil }
        advance()

        if isKeyword("ALL") {
            advance()
            try expect("COLUMNS")
            if isKeyword("EXCEPT") {
                advance()
                let columns = try parseIdentifierList(
                    openingConsumed: false,
                    expected: "excluded property column"
                )
                return try makeStructuralNode(.allExcept(columns))
            }
            return try makeStructuralNode(.all)
        }

        let columns = try parseIdentifierList(
            openingConsumed: false,
            expected: "property column"
        )
        return try makeStructuralNode(.columns(columns))
    }

    private func parseRequiredIdentifier(
        _ expected: String
    ) throws -> String {
        guard case .identifier(let identifier) = currentToken else {
            throw ParseError.unexpectedToken(
                expected: expected,
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        advance()
        return identifier
    }

    private func parseAssignments() throws -> [Assignment] {
        var assignments: [Assignment] = []
        while true {
            guard case .identifier(let column) = currentToken else {
                throw ParseError.unexpectedToken(
                    expected: "assignment column",
                    found: tokenDescription(currentToken),
                    position: input.distance(
                        from: input.startIndex,
                        to: position
                    )
                )
            }
            advance()
            try expect("=")
            let value = try parseExpression()
            try admitCollectionElement()
            assignments.append(
                try makeStructuralNode(
                    Assignment(column: column, value: value)
                )
            )
            guard isSymbol(",") else { break }
            advance()
        }
        return assignments
    }

    private func parseReturningClause() throws -> [ProjectionItem]? {
        guard isKeyword("RETURNING") else { return nil }
        advance()

        var items: [ProjectionItem] = []
        while true {
            let expression = try parseExpression()
            let alias: String?
            if isKeyword("AS") {
                advance()
                guard case .identifier(let name) = currentToken else {
                    throw ParseError.unexpectedToken(
                        expected: "RETURNING alias",
                        found: tokenDescription(currentToken),
                        position: input.distance(
                            from: input.startIndex,
                            to: position
                        )
                    )
                }
                alias = name
                advance()
            } else {
                alias = nil
            }
            try admitCollectionElement()
            items.append(
                try makeStructuralNode(
                    ProjectionItem(expression, alias: alias)
                )
            )
            guard isSymbol(",") else { break }
            advance()
        }
        return items
    }

    private func parseIdentifierList(
        openingConsumed: Bool,
        expected: String
    ) throws -> [String] {
        if !openingConsumed {
            try expect("(")
        }
        var identifiers: [String] = []
        while true {
            guard case .identifier(let identifier) = currentToken else {
                throw ParseError.unexpectedToken(
                    expected: expected,
                    found: tokenDescription(currentToken),
                    position: input.distance(
                        from: input.startIndex,
                        to: position
                    )
                )
            }
            try admitCollectionElement()
            identifiers.append(identifier)
            advance()
            guard isSymbol(",") else { break }
            advance()
        }
        try expect(")")
        return identifiers
    }
}

// MARK: - GRAPH_TABLE Parsing (ISO/IEC 9075-16:2023 SQL/PGQ)

extension SQLParser {
    /// Parse GRAPH_TABLE clause
    /// Syntax: GRAPH_TABLE(graphName, MATCH pattern [COLUMNS (...)])
    private func parseGraphTable() throws -> DataSource {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }

        try expect("GRAPH_TABLE")
        try expect("(")

        // Graph name
        guard case .identifier(let graphName) = currentToken else {
            throw ParseError.unexpectedToken(
                expected: "graph name",
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        advance()

        try expect(",")

        // MATCH pattern
        let matchPattern = try parseMatchPattern()

        // Optional COLUMNS clause
        var columns: [GraphTableColumn]?
        if case .keyword("COLUMNS") = currentToken {
            columns = try parseColumnsClause()
        }

        try expect(")")

        let alias: String?
        if case .keyword("AS") = currentToken {
            advance()
            guard case .identifier(let name) = currentToken else {
                throw ParseError.unexpectedToken(
                    expected: "GRAPH_TABLE alias",
                    found: tokenDescription(currentToken),
                    position: input.distance(from: input.startIndex, to: position)
                )
            }
            alias = name
            advance()
        } else if case .identifier(let name) = currentToken {
            alias = name
            advance()
        } else {
            alias = nil
        }

        let source = try makeStructuralNode(
            GraphTableSource(
                graphName: graphName,
                matchPattern: matchPattern,
                columns: columns,
                alias: alias
            )
        )

        return try makeStructuralNode(.graphTable(source))
    }

    /// Parse MATCH pattern
    /// Syntax: MATCH [mode] pathPattern [, pathPattern] [WHERE expr]
    private func parseMatchPattern() throws -> MatchPattern {
        try expect("MATCH")

        // Parse path patterns
        var paths: [PathPattern] = []
        var first = true

        while first || isSymbol(",") {
            if !first { advance() }
            first = false

            let path = try parsePathPattern()
            try admitCollectionElement()
            paths.append(path)
        }

        // Optional WHERE clause
        var whereExpr: Expression?
        if case .keyword("WHERE") = currentToken {
            advance()
            whereExpr = try parseExpression()
        }

        return try makeStructuralNode(
            MatchPattern(paths: paths, where: whereExpr)
        )
    }

    /// Parse path pattern
    /// Syntax: [pathVar =] [mode] pathElement pathElement ...
    private func parsePathPattern() throws -> PathPattern {
        var pathVariable: String?
        var mode: PathMode?

        // Check for path variable: p = ...
        if case .identifier(let name) = currentToken {
            let next = peekNextToken()
            if case .symbol("=") = next {
                pathVariable = name
                advance()  // identifier
                advance()  // =
            }
        }

        // Check for path mode
        if case .keyword(let kw) = currentToken {
            switch kw {
            case "WALK":
                mode = .walk
                advance()
            case "TRAIL":
                mode = .trail
                advance()
            case "ACYCLIC":
                mode = .acyclic
                advance()
            case "SIMPLE":
                mode = .simple
                advance()
            case "SHORTEST":
                // Check for ALL SHORTEST
                advance()
                if consumeContextualWord("PATH") {
                    mode = .anyShortest
                } else if case .keyword("ALL") = currentToken {
                    advance()
                    try expectContextualWord("PATH")
                    mode = .allShortest
                } else {
                    mode = .anyShortest
                }
            case "ALL":
                advance()
                try expect("SHORTEST")
                // PATH is optional
                consumeContextualWord("PATH")
                mode = .allShortest
            default:
                break
            }
        }

        // Parse path elements
        var elements: [PathElement] = []

        // First element must be a node
        let firstNode = try parseNodePattern()
        try admitCollectionElement()
        elements.append(try makeStructuralNode(.node(firstNode)))

        // Parse edge-node pairs
        while isSymbol("-") || isSymbol("<") || isSymbol("<-") || isSymbol("->") {
            let edge = try parseEdgePattern()
            try admitCollectionElement()
            elements.append(try makeStructuralNode(.edge(edge)))

            let node = try parseNodePattern()
            try admitCollectionElement()
            elements.append(try makeStructuralNode(.node(node)))
        }

        return try makeStructuralNode(
            PathPattern(
                pathVariable: pathVariable,
                elements: elements,
                mode: mode
            )
        )
    }

    /// Parse node pattern
    /// Syntax: (var:Label {prop: val})
    private func parseNodePattern() throws -> NodePattern {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }

        try expect("(")

        var variable: String?
        var labels: [String]?
        var properties: [PropertyBinding]?

        // Parse variable and/or label
        if case .identifier(let name) = currentToken {
            advance()

            // Check for label: var:Label
            if isSymbol(":") {
                variable = name
                advance()

                if case .identifier(let label) = currentToken {
                    labels = [label]
                    advance()
                }
            } else {
                // Just variable, no label
                variable = name
            }
        }

        // Parse properties: {prop: val, ...}
        if isSymbol("{") {
            advance()
            properties = []

            var first = true
            while first || isSymbol(",") {
                if !first { advance() }
                first = false

                guard case .identifier(let propName) = currentToken else {
                    break
                }
                advance()

                try expect(":")

                let expr = try parseExpression()
                try admitCollectionElement()
                properties?.append(
                    try makeStructuralNode(
                        PropertyBinding(key: propName, value: expr)
                    )
                )
            }

            try expect("}")
        }

        try expect(")")

        return try makeStructuralNode(
            NodePattern(
                variable: variable,
                labels: labels,
                properties: properties
            )
        )
    }

    /// Parse edge pattern using state machine
    /// Grammar: EdgePattern ::= StartSymbol [EdgeDetails] EndSymbol
    /// Reference: ISO/IEC 9075-16:2023 SQL/PGQ
    private func parseEdgePattern() throws -> EdgePattern {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }

        // State 1: Parse start symbol
        enum StartSymbol {
            case hyphen       // -
            case leftArrow    // <-
            case leftAngle    // <
            case rightArrow   // -> (complete anonymous)
        }

        let start: StartSymbol
        if isSymbol("<-") {
            advance()
            start = .leftArrow
        } else if isSymbol("->") {
            advance()
            start = .rightArrow
        } else if isSymbol("<") {
            advance()
            // Check if next token is [ (immediate bracket) or - (explicit hyphen)
            if isSymbol("[") {
                // <[...] pattern: immediate bracket without explicit hyphen
                start = .leftAngle
            } else {
                // <-[...] or <- pattern: explicit hyphen
                try expect("-")
                start = .leftAngle
            }
        } else if isSymbol("-") {
            advance()
            start = .hyphen
        } else {
            throw ParseError.unexpectedToken(
                expected: "edge pattern start (-, <-, <, or ->)",
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }

        // State 2: Check for edge details [...]
        var variable: String?
        var labels: [String]?
        var properties: [PropertyBinding]?
        var hasDetails = false

        if isSymbol("[") {
            // Validate: only -> is a complete arrow that cannot have details
            if start == .rightArrow {
                throw ParseError.invalidSyntax(
                    message: "Edge brackets must come before arrow direction (use -[...]->, not ->[...])",
                    position: input.distance(from: input.startIndex, to: position)
                )
            }

            hasDetails = true
            advance()

            // Parse variable and/or label
            if case .identifier(let name) = currentToken {
                advance()

                // Check for label: var:Label
                if isSymbol(":") {
                    variable = name
                    advance()

                    if case .identifier(let label) = currentToken {
                        labels = [label]
                        advance()
                    }
                } else {
                    // Just variable, no label
                    variable = name
                }
            }

            // Parse properties: {prop: val, ...}
            if isSymbol("{") {
                advance()
                properties = []

                var first = true
                while first || isSymbol(",") {
                    if !first { advance() }
                    first = false

                    guard case .identifier(let propName) = currentToken else {
                        break
                    }
                    advance()

                    try expect(":")

                    let expr = try parseExpression()
                    try admitCollectionElement()
                    properties?.append(
                        try makeStructuralNode(
                            PropertyBinding(key: propName, value: expr)
                        )
                    )
                }

                try expect("}")
            }

            try expect("]")
        }

        // State 3: Parse end symbol (only if not a complete arrow)
        enum EndSymbol {
            case rightArrow   // ->
            case hyphen       // -
            case none         // ε (empty)
        }

        let end: EndSymbol

        if start == .rightArrow {
            // Complete anonymous arrow (->): no end symbol expected
            if hasDetails {
                // Defensive check (should have been caught above)
                throw ParseError.invalidSyntax(
                    message: "Internal error: -> with details",
                    position: input.distance(from: input.startIndex, to: position)
                )
            }
            end = .none
        } else {
            // Expect end symbol for incomplete starts (-, <)
            if isSymbol("->") {
                advance()
                end = .rightArrow
            } else if isSymbol("-") {
                advance()
                if isSymbol(">") {
                    // -> split into two tokens
                    advance()
                    end = .rightArrow
                } else {
                    end = .hyphen
                }
            } else {
                end = .none
            }
        }

        // Resolve direction using type-safe pattern matching
        // Reference: ISO/IEC 9075-16:2023 direction resolution table
        let direction: EdgeDirection

        switch (start, end, hasDetails) {
        // Patterns with brackets
        case (.hyphen, .rightArrow, true):
            direction = .outgoing  // -[...]->
        case (.leftArrow, .hyphen, true), (.leftAngle, .hyphen, true):
            direction = .incoming  // <-[...]- or <[...]-
        case (.hyphen, .hyphen, true):
            direction = .undirected  // -[...]-
        case (.leftArrow, .rightArrow, true), (.leftAngle, .rightArrow, true):
            direction = .any  // <-[...]-> or <[...]->

        // Anonymous edges (no brackets)
        case (.rightArrow, .none, false):
            direction = .outgoing  // ->
        case (.leftArrow, .none, false):
            direction = .incoming  // <-
        case (.hyphen, .none, false):
            direction = .undirected  // - (anonymous)

        // Patterns without brackets but with end symbol (rare)
        case (.hyphen, .rightArrow, false):
            direction = .outgoing  // -->
        case (.leftArrow, .hyphen, false), (.leftAngle, .hyphen, false):
            direction = .incoming  // <--
        case (.hyphen, .hyphen, false):
            direction = .undirected  // --

        default:
            throw ParseError.invalidSyntax(
                message: "Invalid edge pattern combination",
                position: input.distance(from: input.startIndex, to: position)
            )
        }

        return try makeStructuralNode(
            EdgePattern(
                variable: variable,
                labels: labels,
                properties: properties,
                direction: direction
            )
        )
    }

    /// Parse COLUMNS clause
    /// Syntax: COLUMNS (expr AS alias, ...)
    private func parseColumnsClause() throws -> [GraphTableColumn] {
        try expect("COLUMNS")
        try expect("(")

        var columns: [GraphTableColumn] = []
        var first = true

        while first || isSymbol(",") {
            if !first { advance() }
            first = false

            let expr = try parseExpression()

            try expect("AS")

            guard case .identifier(let alias) = currentToken else {
                throw ParseError.unexpectedToken(
                    expected: "column alias",
                    found: tokenDescription(currentToken),
                    position: input.distance(from: input.startIndex, to: position)
                )
            }
            advance()

            try admitCollectionElement()
            columns.append(
                try makeStructuralNode(
                    GraphTableColumn(expression: expr, alias: alias)
                )
            )
        }

        try expect(")")

        return columns
    }

    /// Peek at the next token without consuming current token
    private func peekNextToken() -> Token {
        let savedPosition = position
        let savedToken = currentToken
        let savedAnonymousParameterPosition = anonymousParameterPosition
        let savedPositionalParameterStyle = positionalParameterStyle
        let savedStructuralLedger = structuralLedger
        let savedStructuralError = structuralError
        advance()
        let next = currentToken
        position = savedPosition
        currentToken = savedToken
        anonymousParameterPosition = savedAnonymousParameterPosition
        positionalParameterStyle = savedPositionalParameterStyle
        structuralLedger = savedStructuralLedger
        structuralError = savedStructuralError
        return next
    }
}
