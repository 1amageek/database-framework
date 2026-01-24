/// SQLParser.swift
/// SQL Query Parser
///
/// Reference:
/// - ISO/IEC 9075:2023 (SQL)
/// - ISO/IEC 9075-16:2023 (SQL/PGQ)

import Foundation

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
        case symbol(String)
        case eof
    }

    private var input: String
    private var position: String.Index
    private var currentToken: Token

    public init() {
        self.input = ""
        self.position = "".startIndex
        self.currentToken = .eof
    }

    /// Parse a SQL SELECT query
    public func parseSelect(_ sql: String) throws -> SelectQuery {
        self.input = sql
        self.position = input.startIndex
        advance()

        return try parseSelectQuery()
    }

    /// Parse any SQL statement
    public func parse(_ sql: String) throws -> QueryStatement {
        self.input = sql
        self.position = input.startIndex
        advance()

        return try parseStatement()
    }
}

// MARK: - Tokenizer

extension SQLParser {
    private func advance() {
        skipWhitespace()

        guard position < input.endIndex else {
            currentToken = .eof
            return
        }

        let char = input[position]

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
                           "FIRST", "LAST", "OVER", "PARTITION", "ROWS", "RANGE"]

            if keywords.contains(upper) {
                currentToken = .keyword(upper)
            } else {
                currentToken = .identifier(word)
            }
            return
        }

        // Numbers
        if char.isNumber {
            let start = position
            while position < input.endIndex && (input[position].isNumber || input[position] == ".") {
                position = input.index(after: position)
            }
            currentToken = .number(String(input[start..<position]))
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
            currentToken = .string(value)
            return
        }

        // Multi-character symbols
        let twoChar = String(input[position...].prefix(2))
        if ["<=", ">=", "<>", "!=", "||", "&&", "->", "<-"].contains(twoChar) {
            position = input.index(position, offsetBy: 2)
            currentToken = .symbol(twoChar)
            return
        }

        // Single character symbols
        position = input.index(after: position)
        currentToken = .symbol(String(char))
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
        default:
            throw ParseError.invalidSyntax(
                message: "Expected statement keyword",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    private func parseSelectQuery() throws -> SelectQuery {
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
        var source: DataSource = .table(TableRef(""))
        if case .keyword("FROM") = currentToken {
            advance()
            source = try parseDataSource()
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

        return SelectQuery(
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

            subqueries.append(NamedSubquery(
                name: name,
                columns: columnList,
                query: query,
                materialized: materialized
            ))
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
            items.append(ProjectionItem(expr, alias: alias))
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
                        cols.append(name)
                        advance()
                    } else {
                        break
                    }
                }
                try expect(")")
                condition = .using(cols)
            }

            result = .join(JoinClause(type: joinType, left: result, right: right, condition: condition))
        }

        return result
    }

    private func parseTableRef() throws -> DataSource {
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

            return .subquery(subquery, alias: subqueryAlias)
        }

        // Existing table reference logic
        guard case .identifier(let name) = currentToken else {
            throw ParseError.unexpectedToken(
                expected: "table name",
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        advance()

        var alias: String?
        if case .keyword("AS") = currentToken {
            advance()
            if case .identifier(let a) = currentToken {
                alias = a
                advance()
            }
        } else if case .identifier(let a) = currentToken {
            alias = a
            advance()
        }

        return .table(TableRef(table: name, alias: alias))
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
        try parseOrExpression()
    }

    private func parseOrExpression() throws -> Expression {
        var left = try parseAndExpression()
        while case .keyword("OR") = currentToken {
            advance()
            let right = try parseAndExpression()
            left = .or(left, right)
        }
        return left
    }

    private func parseAndExpression() throws -> Expression {
        var left = try parseNotExpression()
        while case .keyword("AND") = currentToken {
            advance()
            let right = try parseNotExpression()
            left = .and(left, right)
        }
        return left
    }

    private func parseNotExpression() throws -> Expression {
        if case .keyword("NOT") = currentToken {
            advance()
            return .not(try parseNotExpression())
        }
        return try parseComparisonExpression()
    }

    private func parseComparisonExpression() throws -> Expression {
        let left = try parseAddExpression()

        switch currentToken {
        case .symbol("="):
            advance()
            return .equal(left, try parseAddExpression())
        case .symbol("<>"), .symbol("!="):
            advance()
            return .notEqual(left, try parseAddExpression())
        case .symbol("<"):
            advance()
            return .lessThan(left, try parseAddExpression())
        case .symbol("<="):
            advance()
            return .lessThanOrEqual(left, try parseAddExpression())
        case .symbol(">"):
            advance()
            return .greaterThan(left, try parseAddExpression())
        case .symbol(">="):
            advance()
            return .greaterThanOrEqual(left, try parseAddExpression())
        case .keyword("IS"):
            advance()
            let notNull = currentToken == .keyword("NOT")
            if notNull { advance() }
            try expect("NULL")
            return notNull ? .isNotNull(left) : .isNull(left)
        case .keyword("LIKE"):
            advance()
            if case .string(let pattern) = currentToken {
                advance()
                return .like(left, pattern: pattern)
            }
            throw ParseError.invalidSyntax(message: "Expected string pattern after LIKE", position: input.distance(from: input.startIndex, to: position))
        case .keyword("IN"):
            advance()
            try expect("(")

            // Check if it's a subquery or value list
            if case .keyword("SELECT") = currentToken {
                let subquery = try parseSelectQuery()
                try expect(")")
                return .inSubquery(left, subquery: subquery)
            }

            // Handle WITH clause (CTE) that starts a subquery
            if case .keyword("WITH") = currentToken {
                let subquery = try parseSelectQuery()
                try expect(")")
                return .inSubquery(left, subquery: subquery)
            }

            // Value list (existing logic)
            var values: [Expression] = []
            var first = true
            while first || isSymbol(",") {
                if !first { advance() }
                first = false
                values.append(try parseExpression())
            }
            try expect(")")
            return .inList(left, values: values)
        case .keyword("BETWEEN"):
            advance()
            let low = try parseAddExpression()
            try expect("AND")
            let high = try parseAddExpression()
            return .between(left, low: low, high: high)
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
                left = .add(left, right)
            } else {
                left = .subtract(left, right)
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
            case "*": left = .multiply(left, right)
            case "/": left = .divide(left, right)
            case "%": left = .modulo(left, right)
            default: break
            }
        }
        return left
    }

    private func parseUnaryExpression() throws -> Expression {
        if case .symbol("-") = currentToken {
            advance()
            return .negate(try parseUnaryExpression())
        }
        return try parsePrimaryExpression()
    }

    private func parsePrimaryExpression() throws -> Expression {
        switch currentToken {
        case .symbol("("):
            advance()
            // Disambiguate: subquery vs parenthesized expression
            if case .keyword("SELECT") = currentToken {
                let subquery = try parseSelectQuery()
                try expect(")")
                return .subquery(subquery)
            }
            // Handle WITH clause (CTE) that starts a subquery
            if case .keyword("WITH") = currentToken {
                let subquery = try parseSelectQuery()
                try expect(")")
                return .subquery(subquery)
            }
            let expr = try parseExpression()
            try expect(")")
            return expr

        case .keyword("EXISTS"):
            advance()
            try expect("(")
            let subquery = try parseSelectQuery()
            try expect(")")
            return .exists(subquery)

        case .number(let n):
            advance()
            if n.contains(".") {
                return .literal(.double(Double(n) ?? 0))
            }
            return .literal(.int(Int64(n) ?? 0))

        case .string(let s):
            advance()
            return .literal(.string(s))

        case .keyword("TRUE"):
            advance()
            return .literal(.bool(true))

        case .keyword("FALSE"):
            advance()
            return .literal(.bool(false))

        case .keyword("NULL"):
            advance()
            return .literal(.null)

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
                        args.append(try parseExpression())
                    }
                }
                try expect(")")
                return .function(FunctionCall(name: name, arguments: args))
            }
            // Check for qualified name
            if case .symbol(".") = currentToken {
                advance()
                if case .identifier(let col) = currentToken {
                    advance()
                    return .column(ColumnRef(table: name, column: col))
                }
            }
            return .column(ColumnRef(column: name))

        default:
            throw ParseError.unexpectedToken(
                expected: "expression",
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
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
            return .aggregate(.count(arg, distinct: distinct))
        case .keyword("SUM"):
            return .aggregate(.sum(arg ?? .literal(.null), distinct: distinct))
        case .keyword("AVG"):
            return .aggregate(.avg(arg ?? .literal(.null), distinct: distinct))
        case .keyword("MIN"):
            return .aggregate(.min(arg ?? .literal(.null)))
        case .keyword("MAX"):
            return .aggregate(.max(arg ?? .literal(.null)))
        default:
            throw ParseError.invalidSyntax(message: "Unknown aggregate function", position: input.distance(from: input.startIndex, to: position))
        }
    }

    private func parseCaseExpression() throws -> Expression {
        try expect("CASE")

        var cases: [(Expression, Expression)] = []
        while case .keyword("WHEN") = currentToken {
            advance()
            let condition = try parseExpression()
            try expect("THEN")
            let result = try parseExpression()
            cases.append((condition, result))
        }

        var elseResult: Expression?
        if case .keyword("ELSE") = currentToken {
            advance()
            elseResult = try parseExpression()
        }

        try expect("END")

        return .caseWhen(cases: cases, elseResult: elseResult)
    }

    private func parseExpressionList() throws -> [Expression] {
        var exprs: [Expression] = []
        var first = true
        while first || isSymbol(",") {
            if !first { advance() }
            first = false
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
            keys.append(SortKey(expr, direction: direction, nulls: nulls))
        }
        return keys
    }

    private func parseInsertQuery() throws -> InsertQuery {
        throw ParseError.unsupportedFeature("INSERT parsing not yet implemented")
    }

    private func parseUpdateQuery() throws -> UpdateQuery {
        throw ParseError.unsupportedFeature("UPDATE parsing not yet implemented")
    }

    private func parseDeleteQuery() throws -> DeleteQuery {
        throw ParseError.unsupportedFeature("DELETE parsing not yet implemented")
    }

    private func parseCreateGraph() throws -> CreateGraphStatement {
        throw ParseError.unsupportedFeature("CREATE PROPERTY GRAPH parsing not yet implemented")
    }
}
