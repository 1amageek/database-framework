/// SPARQLParser.swift
/// SPARQL Query Parser
///
/// Reference:
/// - W3C SPARQL 1.1 Query Language
/// - W3C SPARQL 1.2 (Draft)

import Foundation

/// SPARQL Parser for converting SPARQL strings to AST
public final class SPARQLParser {
    /// Parser errors
    public enum ParseError: Error, Sendable, Equatable {
        case unexpectedToken(expected: String, found: String, position: Int)
        case unexpectedEndOfInput(expected: String)
        case invalidSyntax(message: String, position: Int)
        case invalidIRI(String)
        case unsupportedFeature(String)
    }

    /// Token types
    private enum Token: Sendable, Equatable {
        case keyword(String)
        case iri(String)
        case prefixedName(prefix: String, local: String)
        case variable(String)
        case string(String, language: String?, datatype: String?)
        case integer(String)
        case decimal(String)
        case double(String)
        case blankNode(String)
        case symbol(String)
        case eof
    }

    private var input: String
    private var position: String.Index
    private var currentToken: Token
    private var prefixes: [String: String]

    public init() {
        self.input = ""
        self.position = "".startIndex
        self.currentToken = .eof
        self.prefixes = SPARQLTerm.commonPrefixes
    }

    /// SPARQL 1.1 built-in function keywords [121] BuiltInCall + [127] Aggregate
    /// Used by parsePrimaryExpression() to route to parseBuiltInCall()
    private static let builtInFunctionKeywords: Set<String> = [
        // Aggregates [127]
        "COUNT", "SUM", "AVG", "MIN", "MAX", "SAMPLE", "GROUP_CONCAT",
        // Existing [121]
        "BOUND", "EXISTS", "NOT", "REGEX",
        // Conditional
        "IF", "COALESCE",
        // 0-arg
        "NOW", "RAND", "UUID", "STRUUID",
        // 0-1 arg
        "BNODE",
        // 1-arg: String
        "STR", "STRLEN", "UCASE", "LCASE", "ENCODE_FOR_URI",
        // 1-arg: Lang/Datatype
        "LANG", "DATATYPE",
        // 1-arg: IRI
        "IRI", "URI",
        // 1-arg: Date/Time
        "YEAR", "MONTH", "DAY", "HOURS", "MINUTES", "SECONDS", "TIMEZONE", "TZ",
        // 1-arg: Hash
        "MD5", "SHA1", "SHA256", "SHA384", "SHA512",
        // 1-arg: Type checks
        "ISIRI", "ISURI", "ISBLANK", "ISLITERAL", "ISNUMERIC",
        // RDF-star (SPARQL-star): type check + accessors
        "ISTRIPLE", "TRIPLE", "SUBJECT", "PREDICATE", "OBJECT",
        // 2-arg
        "LANGMATCHES", "CONTAINS", "STRSTARTS", "STRENDS",
        "STRBEFORE", "STRAFTER", "SAMETERM", "STRDT", "STRLANG",
        // 2-3 arg [123]
        "SUBSTR",
        // 3-4 arg [124]
        "REPLACE",
        // variadic
        "CONCAT",
    ]

    /// Enable debug logging
    nonisolated(unsafe) private static var debugEnabled = false
    public static func enableDebug(_ enabled: Bool) {
        debugEnabled = enabled
    }
    private func log(_ message: String) {
        if Self.debugEnabled {
            print("[SPARQLParser] \(message)")
        }
    }

    /// Parse a SPARQL query
    public func parse(_ sparql: String) throws -> QueryStatement {
        log("parse() START: '\(sparql.prefix(50))...'")
        self.input = sparql
        self.position = input.startIndex
        self.prefixes = SPARQLTerm.commonPrefixes
        advance()
        log("parse() initial token: \(currentToken)")

        // Parse prologue (PREFIX and BASE declarations)
        try parsePrologue()
        log("parse() after prologue, token: \(currentToken)")

        // Parse query form
        switch currentToken {
        case .keyword("SELECT"):
            return .select(try parseSelectQuery())
        case .keyword("CONSTRUCT"):
            return .construct(try parseConstructQuery())
        case .keyword("ASK"):
            return .ask(try parseAskQuery())
        case .keyword("DESCRIBE"):
            return .describe(try parseDescribeQuery())
        default:
            throw ParseError.invalidSyntax(
                message: "Expected query form (SELECT, CONSTRUCT, ASK, DESCRIBE)",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    /// Parse a SPARQL SELECT query
    public func parseSelect(_ sparql: String) throws -> SelectQuery {
        self.input = sparql
        self.position = input.startIndex
        self.prefixes = SPARQLTerm.commonPrefixes
        advance()

        try parsePrologue()
        return try parseSelectQuery()
    }
}

// MARK: - Tokenizer

extension SPARQLParser {
    private func advance() {
        skipWhitespace()

        guard position < input.endIndex else {
            currentToken = .eof
            return
        }

        let char = input[position]

        // Variables: ?var or $var
        if char == "?" || char == "$" {
            position = input.index(after: position)
            let start = position
            while position < input.endIndex && isVarChar(input[position]) {
                position = input.index(after: position)
            }
            currentToken = .variable(String(input[start..<position]))
            return
        }

        // Multi-character symbols (must be checked before IRI to distinguish << from <iri>)
        let twoChar = String(input[position...].prefix(2))
        if ["<=", ">=", "!=", "&&", "||", "^^", "<<", ">>"].contains(twoChar) {
            position = input.index(position, offsetBy: 2)
            currentToken = .symbol(twoChar)
            return
        }

        // IRIs: <...>
        if char == "<" {
            position = input.index(after: position)
            let start = position
            while position < input.endIndex && input[position] != ">" {
                position = input.index(after: position)
            }
            let iri = String(input[start..<position])
            if position < input.endIndex {
                position = input.index(after: position)
            }
            currentToken = .iri(iri)
            return
        }

        // Blank nodes: _:name
        if char == "_" && position < input.index(before: input.endIndex) && input[input.index(after: position)] == ":" {
            position = input.index(position, offsetBy: 2)
            let start = position
            while position < input.endIndex && isVarChar(input[position]) {
                position = input.index(after: position)
            }
            currentToken = .blankNode(String(input[start..<position]))
            return
        }

        // Strings
        if char == "\"" || char == "'" {
            let quote = char
            let tripleQuote = String(input[position...].prefix(3)) == String(repeating: quote, count: 3)

            if tripleQuote {
                position = input.index(position, offsetBy: 3)
                currentToken = parseLongString(quote: quote)
            } else {
                position = input.index(after: position)
                currentToken = parseShortString(quote: quote)
            }
            return
        }

        // Numbers
        if char.isNumber || (char == "." && position < input.index(before: input.endIndex) && input[input.index(after: position)].isNumber) {
            currentToken = parseNumber()
            return
        }

        // Keywords and prefixed names
        if char.isLetter || char == ":" {
            let start = position
            while position < input.endIndex {
                let c = input[position]
                if c.isLetter || c.isNumber || c == "_" || c == "-" || c == ":" || c == "." {
                    position = input.index(after: position)
                } else {
                    break
                }
            }

            // Trim trailing dots
            while position > start && input[input.index(before: position)] == "." {
                position = input.index(before: position)
            }

            let word = String(input[start..<position])

            // Check for prefixed name
            if let colonIndex = word.firstIndex(of: ":") {
                let prefix = String(word[..<colonIndex])
                let local = String(word[word.index(after: colonIndex)...])

                // Check if it's a keyword followed by colon (unlikely but handle it)
                let upper = word.uppercased()
                if !isKeywordString(upper) {
                    currentToken = .prefixedName(prefix: prefix, local: local)
                    return
                }
            }

            let upper = word.uppercased()
            if isKeywordString(upper) {
                currentToken = .keyword(upper)
            } else {
                // Bare word - treat as prefixed name with empty prefix
                currentToken = .prefixedName(prefix: "", local: word)
            }
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
            } else if char == "#" {
                // Comment
                while position < input.endIndex && input[position] != "\n" {
                    position = input.index(after: position)
                }
            } else {
                break
            }
        }
    }

    private func isVarChar(_ char: Character) -> Bool {
        char.isLetter || char.isNumber || char == "_"
    }

    private func isKeywordString(_ word: String) -> Bool {
        let keywords = ["SELECT", "CONSTRUCT", "DESCRIBE", "ASK", "WHERE", "FROM", "NAMED",
                       "PREFIX", "BASE", "OPTIONAL", "UNION", "FILTER", "GRAPH", "SERVICE",
                       "SILENT", "BIND", "AS", "VALUES", "MINUS", "GROUP", "BY", "HAVING",
                       "ORDER", "ASC", "DESC", "LIMIT", "OFFSET", "DISTINCT", "REDUCED",
                       "NOT", "IN", "EXISTS", "BOUND", "IF", "COALESCE", "REGEX", "STR",
                       "LANG", "LANGMATCHES", "DATATYPE", "IRI", "URI", "BNODE", "RAND",
                       "ABS", "CEIL", "FLOOR", "ROUND", "CONCAT", "STRLEN", "UCASE", "LCASE",
                       "ENCODE_FOR_URI", "CONTAINS", "STRSTARTS", "STRENDS", "STRBEFORE",
                       "STRAFTER", "YEAR", "MONTH", "DAY", "HOURS", "MINUTES", "SECONDS",
                       "TIMEZONE", "TZ", "NOW", "UUID", "STRUUID", "MD5", "SHA1", "SHA256",
                       "SHA384", "SHA512", "ISIRI", "ISURI", "ISBLANK", "ISLITERAL",
                       "ISNUMERIC", "SAMETERM", "TRUE", "FALSE", "COUNT", "SUM", "MIN",
                       "MAX", "AVG", "SAMPLE", "GROUP_CONCAT", "SEPARATOR", "A", "UNDEF",
                       "SUBSTR", "REPLACE", "STRDT", "STRLANG",
                       "ISTRIPLE", "TRIPLE", "SUBJECT", "PREDICATE", "OBJECT"]
        return keywords.contains(word)
    }

    private func parseShortString(quote: Character) -> Token {
        var value = ""
        while position < input.endIndex && input[position] != quote {
            if input[position] == "\\" {
                position = input.index(after: position)
                if position < input.endIndex {
                    value.append(parseEscape())
                }
            } else {
                value.append(input[position])
                position = input.index(after: position)
            }
        }
        if position < input.endIndex {
            position = input.index(after: position)
        }

        // Check for language tag or datatype
        var language: String?
        var datatype: String?

        if position < input.endIndex && input[position] == "@" {
            position = input.index(after: position)
            let langStart = position
            while position < input.endIndex && (input[position].isLetter || input[position] == "-") {
                position = input.index(after: position)
            }
            language = String(input[langStart..<position])
        } else if String(input[position...].prefix(2)) == "^^" {
            position = input.index(position, offsetBy: 2)
            advance()
            if case .iri(let iri) = currentToken {
                datatype = iri
            } else if case .prefixedName(let prefix, let local) = currentToken {
                datatype = resolvePrefixedName(prefix: prefix, local: local)
            }
        }

        return .string(value, language: language, datatype: datatype)
    }

    private func parseLongString(quote: Character) -> Token {
        var value = ""
        let endQuote = String(repeating: quote, count: 3)

        while position < input.endIndex {
            if String(input[position...].prefix(3)) == endQuote {
                position = input.index(position, offsetBy: 3)
                break
            }
            if input[position] == "\\" {
                position = input.index(after: position)
                if position < input.endIndex {
                    value.append(parseEscape())
                }
            } else {
                value.append(input[position])
                position = input.index(after: position)
            }
        }

        return .string(value, language: nil, datatype: nil)
    }

    private func parseEscape() -> Character {
        let char = input[position]
        position = input.index(after: position)
        switch char {
        case "n": return "\n"
        case "r": return "\r"
        case "t": return "\t"
        case "\\": return "\\"
        case "\"": return "\""
        case "'": return "'"
        default: return char
        }
    }

    private func parseNumber() -> Token {
        let start = position
        var hasDecimal = false
        var hasExponent = false

        while position < input.endIndex {
            let char = input[position]
            if char.isNumber {
                position = input.index(after: position)
            } else if char == "." && !hasDecimal {
                hasDecimal = true
                position = input.index(after: position)
            } else if (char == "e" || char == "E") && !hasExponent {
                hasExponent = true
                position = input.index(after: position)
                if position < input.endIndex && (input[position] == "+" || input[position] == "-") {
                    position = input.index(after: position)
                }
            } else {
                break
            }
        }

        let numStr = String(input[start..<position])
        if hasExponent {
            return .double(numStr)
        } else if hasDecimal {
            return .decimal(numStr)
        } else {
            return .integer(numStr)
        }
    }

    private func resolvePrefixedName(prefix: String, local: String) -> String {
        if let base = prefixes[prefix] {
            return base + local
        }
        return "\(prefix):\(local)"
    }

    private func expect(_ keyword: String) throws {
        guard case .keyword(let kw) = currentToken, kw == keyword else {
            throw ParseError.unexpectedToken(
                expected: keyword,
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        advance()
    }

    private func expectSymbol(_ symbol: String) throws {
        guard case .symbol(let s) = currentToken, s == symbol else {
            throw ParseError.unexpectedToken(
                expected: symbol,
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        advance()
    }

    private func tokenDescription(_ token: Token) -> String {
        switch token {
        case .keyword(let k): return "keyword '\(k)'"
        case .iri(let i): return "IRI <\(i)>"
        case .prefixedName(let p, let l): return "prefixed name '\(p):\(l)'"
        case .variable(let v): return "variable ?\(v)"
        case .string(let s, _, _): return "string \"\(s)\""
        case .integer(let n): return "integer \(n)"
        case .decimal(let n): return "decimal \(n)"
        case .double(let n): return "double \(n)"
        case .blankNode(let b): return "blank node _:\(b)"
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

    private func isVariable() -> Bool {
        if case .variable = currentToken {
            return true
        }
        return false
    }
}

// MARK: - Query Parsing

extension SPARQLParser {
    private func parsePrologue() throws {
        while true {
            switch currentToken {
            case .keyword("PREFIX"):
                advance()
                guard case .prefixedName(let prefix, _) = currentToken else {
                    throw ParseError.invalidSyntax(message: "Expected prefix name", position: input.distance(from: input.startIndex, to: position))
                }
                advance()
                guard case .iri(let iri) = currentToken else {
                    throw ParseError.invalidSyntax(message: "Expected IRI", position: input.distance(from: input.startIndex, to: position))
                }
                prefixes[prefix] = iri
                advance()

            case .keyword("BASE"):
                advance()
                guard case .iri(_) = currentToken else {
                    throw ParseError.invalidSyntax(message: "Expected IRI", position: input.distance(from: input.startIndex, to: position))
                }
                // Store base IRI if needed
                advance()

            default:
                return
            }
        }
    }

    private func parseSelectQuery() throws -> SelectQuery {
        try expect("SELECT")

        // DISTINCT / REDUCED
        var distinct = false
        var reduced = false
        if case .keyword("DISTINCT") = currentToken {
            distinct = true
            advance()
        } else if case .keyword("REDUCED") = currentToken {
            reduced = true
            advance()
        }

        // Projection
        let projection = try parseSelectClause()

        // Dataset clauses (FROM)
        while case .keyword("FROM") = currentToken {
            advance()
            // Skip NAMED keyword if present
            if case .keyword("NAMED") = currentToken {
                advance()
            }
            // Skip IRI
            if case .iri(_) = currentToken {
                advance()
            }
        }

        // WHERE clause
        var pattern: GraphPattern = .basic([])
        if case .keyword("WHERE") = currentToken {
            advance()
        }
        if case .symbol("{") = currentToken {
            pattern = try parseGroupGraphPattern()
        }

        // Solution modifiers
        var groupBy: [Expression]?
        if case .keyword("GROUP") = currentToken {
            advance()
            try expect("BY")
            groupBy = try parseGroupCondition()
        }

        var having: Expression?
        if case .keyword("HAVING") = currentToken {
            advance()
            having = try parseConstraint()
        }

        var orderBy: [SortKey]?
        if case .keyword("ORDER") = currentToken {
            advance()
            try expect("BY")
            orderBy = try parseOrderCondition()
        }

        var limit: Int?
        if case .keyword("LIMIT") = currentToken {
            advance()
            if case .integer(let n) = currentToken {
                limit = Int(n)
                advance()
            }
        }

        var offset: Int?
        if case .keyword("OFFSET") = currentToken {
            advance()
            if case .integer(let n) = currentToken {
                offset = Int(n)
                advance()
            }
        }

        return SelectQuery(
            projection: projection,
            source: .graphPattern(pattern),
            filter: nil,
            groupBy: groupBy,
            having: having,
            orderBy: orderBy,
            limit: limit,
            offset: offset,
            distinct: distinct,
            reduced: reduced
        )
    }

    private func parseSelectClause() throws -> Projection {
        if case .symbol("*") = currentToken {
            advance()
            return .all
        }

        var items: [ProjectionItem] = []
        while true {
            switch currentToken {
            case .variable(let name):
                items.append(ProjectionItem(.variable(Variable(name))))
                advance()
            case .symbol("("):
                advance()
                let expr = try parseExpression()
                try expect("AS")
                guard case .variable(let alias) = currentToken else {
                    throw ParseError.invalidSyntax(message: "Expected variable after AS", position: input.distance(from: input.startIndex, to: position))
                }
                advance()
                try expectSymbol(")")
                items.append(ProjectionItem(expr, alias: alias))
            default:
                break
            }

            // Check if next token starts a new select item
            if case .variable = currentToken { continue }
            if case .symbol("(") = currentToken { continue }
            break
        }

        return .items(items)
    }

    private func parseGroupGraphPattern() throws -> GraphPattern {
        try expectSymbol("{")
        let pattern = try parseGroupGraphPatternSub()
        try expectSymbol("}")
        return pattern
    }

    private func parseGroupGraphPatternSub() throws -> GraphPattern {
        log("parseGroupGraphPatternSub() START, token: \(currentToken)")
        var patterns: [GraphPattern] = []
        var loopCount = 0

        while true {
            loopCount += 1
            log("parseGroupGraphPatternSub() loop[\(loopCount)] token: \(currentToken)")

            // Check for end of group
            if case .symbol("}") = currentToken {
                log("parseGroupGraphPatternSub() found '}', breaking")
                break
            }
            if case .eof = currentToken {
                log("parseGroupGraphPatternSub() found EOF, breaking")
                break
            }

            // Track if we made progress to avoid infinite loop
            var madeProgress = false

            // Parse triple patterns
            if let tripleBlock = try? parseTriplesBlock() {
                log("parseGroupGraphPatternSub() parsed tripleBlock")
                patterns.append(tripleBlock)
                madeProgress = true
            }

            // Parse graph pattern not triples
            switch currentToken {
            case .keyword("OPTIONAL"):
                advance()
                let optPattern = try parseGroupGraphPattern()
                if let last = patterns.last {
                    patterns[patterns.count - 1] = .optional(last, optPattern)
                } else {
                    patterns.append(.optional(.basic([]), optPattern))
                }
                madeProgress = true

            case .keyword("UNION"):
                advance()
                let rightPattern = try parseGroupGraphPattern()
                if let last = patterns.last {
                    patterns[patterns.count - 1] = .union(last, rightPattern)
                }
                madeProgress = true

            case .keyword("MINUS"):
                advance()
                let minusPattern = try parseGroupGraphPattern()
                if let last = patterns.last {
                    patterns[patterns.count - 1] = .minus(last, minusPattern)
                }
                madeProgress = true

            case .keyword("FILTER"):
                advance()
                let constraint = try parseConstraint()
                if let last = patterns.last {
                    patterns[patterns.count - 1] = .filter(last, constraint)
                } else {
                    patterns.append(.filter(.basic([]), constraint))
                }
                madeProgress = true

            case .keyword("BIND"):
                advance()
                try expectSymbol("(")
                let expr = try parseExpression()
                try expect("AS")
                guard case .variable(let varName) = currentToken else {
                    throw ParseError.invalidSyntax(message: "Expected variable", position: input.distance(from: input.startIndex, to: position))
                }
                advance()
                try expectSymbol(")")
                if let last = patterns.last {
                    patterns[patterns.count - 1] = .bind(last, variable: varName, expression: expr)
                } else {
                    patterns.append(.bind(.basic([]), variable: varName, expression: expr))
                }
                madeProgress = true

            case .keyword("VALUES"):
                let valuesPattern = try parseInlineData()
                patterns.append(valuesPattern)
                madeProgress = true

            case .keyword("GRAPH"):
                advance()
                let graphName = try parseTerm()
                let graphPattern = try parseGroupGraphPattern()
                patterns.append(.graph(name: graphName, pattern: graphPattern))
                madeProgress = true

            case .keyword("SERVICE"):
                advance()
                var silent = false
                if case .keyword("SILENT") = currentToken {
                    silent = true
                    advance()
                }
                guard case .iri(let endpoint) = currentToken else {
                    throw ParseError.invalidSyntax(message: "Expected IRI for SERVICE", position: input.distance(from: input.startIndex, to: position))
                }
                advance()
                let servicePattern = try parseGroupGraphPattern()
                patterns.append(.service(endpoint: endpoint, pattern: servicePattern, silent: silent))
                madeProgress = true

            case .symbol("{"):
                let subPattern = try parseGroupGraphPattern()
                patterns.append(subPattern)
                madeProgress = true

            default:
                log("parseGroupGraphPatternSub() default case, token: \(currentToken)")
                break
            }

            // If no progress was made and we're not at end, break to avoid infinite loop
            if !madeProgress {
                log("parseGroupGraphPatternSub() no progress, breaking")
                break
            }
        }

        log("parseGroupGraphPatternSub() END, patterns count: \(patterns.count)")
        // Combine patterns
        if patterns.isEmpty {
            return .basic([])
        } else if patterns.count == 1 {
            return patterns[0]
        } else {
            return patterns.dropFirst().reduce(patterns[0]) { .join($0, $1) }
        }
    }

    private func parseTriplesBlock() throws -> GraphPattern {
        log("parseTriplesBlock() START, token: \(currentToken)")
        var triples: [TriplePattern] = []

        let subject = try parseTerm()
        log("parseTriplesBlock() subject parsed: \(subject), next token: \(currentToken)")

        // Parse predicate-object list
        var continueOuter = true
        while continueOuter {
            if isSymbol(".") {
                advance()
                break
            }
            if isSymbol(";") {
                advance()
                if isSymbol("}") { break }
                if isSymbol(".") { continue }
            }

            let predicate = try parseVerb()

            // Parse object list
            var firstObject = true
            while firstObject || isSymbol(",") {
                if !firstObject {
                    advance()
                }
                firstObject = false
                let object = try parseTerm()
                triples.append(TriplePattern(subject: subject, predicate: predicate, object: object))
            }

            continueOuter = isSymbol(";")
        }

        // Consume trailing dot if present
        if isSymbol(".") {
            log("parseTriplesBlock() consuming trailing dot")
            advance()
        }

        log("parseTriplesBlock() END, triples count: \(triples.count), next token: \(currentToken)")
        return .basic(triples)
    }

    private func parseVerb() throws -> SPARQLTerm {
        if case .keyword("A") = currentToken {
            advance()
            return .rdfType
        }
        return try parseTerm()
    }

    private func parseTerm() throws -> SPARQLTerm {
        switch currentToken {
        // RDF-star: << subject predicate object >>
        case .symbol("<<"):
            advance() // consume <<
            let subject = try parseTerm()
            let predicate = try parseTerm()
            let object = try parseTerm()
            try expectSymbol(">>")
            return .quotedTriple(subject: subject, predicate: predicate, object: object)

        case .variable(let name):
            advance()
            return .variable(name)

        case .iri(let iri):
            advance()
            return .iri(iri)

        case .prefixedName(let prefix, let local):
            advance()
            return .prefixedName(prefix: prefix, local: local)

        case .string(let value, let language, let datatype):
            advance()
            if let lang = language {
                return .literal(.langLiteral(value: value, language: lang))
            } else if let dt = datatype {
                return .literal(.typedLiteral(value: value, datatype: dt))
            } else {
                return .literal(.string(value))
            }

        case .integer(let n):
            advance()
            return .literal(.int(Int64(n) ?? 0))

        case .decimal(let n), .double(let n):
            advance()
            return .literal(.double(Double(n) ?? 0))

        case .blankNode(let id):
            advance()
            return .blankNode(id)

        case .keyword("TRUE"):
            advance()
            return .literal(.bool(true))

        case .keyword("FALSE"):
            advance()
            return .literal(.bool(false))

        default:
            throw ParseError.invalidSyntax(
                message: "Expected term",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    /// [69] Constraint ::= BrackettedExpression | BuiltInCall | FunctionCall
    private func parseConstraint() throws -> Expression {
        // BrackettedExpression
        if case .symbol("(") = currentToken {
            advance()
            let expr = try parseExpression()
            try expectSymbol(")")
            return expr
        }
        // BuiltInCall [121]
        if case .keyword(let kw) = currentToken, Self.builtInFunctionKeywords.contains(kw) {
            return try parseBuiltInCall()
        }
        // FunctionCall [70]: iri ArgList
        if case .iri(let iri) = currentToken {
            advance()
            return try parseIRIFunctionCall(iri: iri)
        }
        if case .prefixedName(let prefix, let local) = currentToken {
            advance()
            let resolved = resolvePrefixedName(prefix: prefix, local: local)
            return try parseIRIFunctionCall(iri: resolved)
        }
        // No valid Constraint production matched
        throw ParseError.invalidSyntax(
            message: "Expected constraint (bracketed expression, built-in call, or function call)",
            position: input.distance(from: input.startIndex, to: position)
        )
    }

    /// [121] BuiltInCall — All SPARQL 1.1 built-in functions
    private func parseBuiltInCall() throws -> Expression {
        switch currentToken {
        // BOUND [121]: 'BOUND' '(' Var ')'
        case .keyword("BOUND"):
            advance()
            try expectSymbol("(")
            guard case .variable(let varName) = currentToken else {
                throw ParseError.invalidSyntax(message: "Expected variable", position: input.distance(from: input.startIndex, to: position))
            }
            advance()
            try expectSymbol(")")
            return .bound(Variable(varName))

        // NotExistsFunc [126]: 'NOT' 'EXISTS' GroupGraphPattern
        case .keyword("NOT"):
            advance()
            if case .keyword("EXISTS") = currentToken {
                advance()
                let pattern = try parseGroupGraphPattern()
                return .not(.exists(SelectQuery(
                    projection: .all,
                    source: .graphPattern(pattern)
                )))
            }
            return .not(try parseConstraint())

        // ExistsFunc [125]: 'EXISTS' GroupGraphPattern
        case .keyword("EXISTS"):
            advance()
            let pattern = try parseGroupGraphPattern()
            return .exists(SelectQuery(
                projection: .all,
                source: .graphPattern(pattern)
            ))

        // RegexExpression [122]: 'REGEX' '(' Expression ',' Expression (',' Expression)? ')'
        case .keyword("REGEX"):
            advance()
            try expectSymbol("(")
            let text = try parseExpression()
            try expectSymbol(",")
            let patternExpr = try parseExpression()
            var flagsExpr: Expression?
            if isSymbol(",") {
                advance()
                flagsExpr = try parseExpression()
            }
            try expectSymbol(")")
            // If pattern is a string literal and flags (if present) is also a string literal,
            // use the dedicated .regex AST node. Otherwise fall through to generic function call.
            if case .literal(.string(let pattern)) = patternExpr {
                if let fExpr = flagsExpr {
                    if case .literal(.string(let f)) = fExpr {
                        return .regex(text, pattern: pattern, flags: f)
                    }
                    // flags is not a string literal — fall through to .function()
                } else {
                    return .regex(text, pattern: pattern, flags: nil)
                }
            }
            // Generic function call for non-literal pattern or non-literal flags
            var args: [Expression] = [text, patternExpr]
            if let f = flagsExpr { args.append(f) }
            return .function(FunctionCall(name: "REGEX", arguments: args))

        // Aggregate [127]: COUNT, SUM, AVG, MIN, MAX, SAMPLE
        case .keyword("COUNT"), .keyword("SUM"), .keyword("AVG"),
             .keyword("MIN"), .keyword("MAX"), .keyword("SAMPLE"):
            return try parseSPARQLAggregate()

        // Aggregate [127]: GROUP_CONCAT
        case .keyword("GROUP_CONCAT"):
            return try parseGroupConcat()

        // IF [121]: 'IF' '(' Expression ',' Expression ',' Expression ')'
        case .keyword("IF"):
            advance()
            try expectSymbol("(")
            let condition = try parseExpression()
            try expectSymbol(",")
            let thenExpr = try parseExpression()
            try expectSymbol(",")
            let elseExpr = try parseExpression()
            try expectSymbol(")")
            return .function(FunctionCall(name: "IF", arguments: [condition, thenExpr, elseExpr]))

        // COALESCE [121]: 'COALESCE' ExpressionList
        case .keyword("COALESCE"):
            advance()
            let args = try parseExpressionList()
            return .coalesce(args)

        // CONCAT [121]: 'CONCAT' ExpressionList
        case .keyword("CONCAT"):
            advance()
            let args = try parseExpressionList()
            return .function(FunctionCall(name: "CONCAT", arguments: args))

        // 0-arg functions [121]: NOW/RAND/UUID/STRUUID NIL
        case .keyword("NOW"), .keyword("RAND"), .keyword("UUID"), .keyword("STRUUID"):
            guard case .keyword(let name) = currentToken else {
                throw ParseError.invalidSyntax(message: "Expected function name", position: input.distance(from: input.startIndex, to: position))
            }
            advance()
            try expectSymbol("(")
            try expectSymbol(")")
            return .function(FunctionCall(name: name, arguments: []))

        // BNODE [121]: 'BNODE' ( '(' Expression ')' | NIL )
        case .keyword("BNODE"):
            advance()
            try expectSymbol("(")
            if isSymbol(")") {
                advance()
                return .function(FunctionCall(name: "BNODE", arguments: []))
            }
            let arg = try parseExpression()
            try expectSymbol(")")
            return .function(FunctionCall(name: "BNODE", arguments: [arg]))

        // Generic 1-arg functions [121]
        case .keyword("STR"), .keyword("LANG"), .keyword("DATATYPE"),
             .keyword("IRI"), .keyword("URI"),
             .keyword("ABS"), .keyword("CEIL"), .keyword("FLOOR"), .keyword("ROUND"),
             .keyword("STRLEN"), .keyword("UCASE"), .keyword("LCASE"),
             .keyword("ENCODE_FOR_URI"),
             .keyword("YEAR"), .keyword("MONTH"), .keyword("DAY"),
             .keyword("HOURS"), .keyword("MINUTES"), .keyword("SECONDS"),
             .keyword("TIMEZONE"), .keyword("TZ"),
             .keyword("MD5"), .keyword("SHA1"), .keyword("SHA256"),
             .keyword("SHA384"), .keyword("SHA512"),
             .keyword("ISIRI"), .keyword("ISURI"), .keyword("ISBLANK"),
             .keyword("ISLITERAL"), .keyword("ISNUMERIC"):
            return try parseGenericFunctionCall()

        // Generic 2-arg functions [121]
        case .keyword("LANGMATCHES"), .keyword("CONTAINS"),
             .keyword("STRSTARTS"), .keyword("STRENDS"),
             .keyword("STRBEFORE"), .keyword("STRAFTER"),
             .keyword("SAMETERM"), .keyword("STRDT"), .keyword("STRLANG"):
            return try parseGenericFunctionCall()

        // SubstringExpression [123]: 'SUBSTR' '(' Expression ',' Expression (',' Expression)? ')'
        case .keyword("SUBSTR"):
            return try parseGenericFunctionCall()

        // StrReplaceExpression [124]: 'REPLACE' '(' Expression ',' Expression ',' Expression (',' Expression)? ')'
        case .keyword("REPLACE"):
            return try parseGenericFunctionCall()

        // RDF-star (SPARQL-star) built-in functions
        case .keyword("ISTRIPLE"):
            advance()
            try expectSymbol("(")
            let arg = try parseExpression()
            try expectSymbol(")")
            return .isTriple(arg)

        case .keyword("TRIPLE"):
            advance()
            try expectSymbol("(")
            let s = try parseExpression()
            try expectSymbol(",")
            let p = try parseExpression()
            try expectSymbol(",")
            let o = try parseExpression()
            try expectSymbol(")")
            return .triple(subject: s, predicate: p, object: o)

        case .keyword("SUBJECT"):
            advance()
            try expectSymbol("(")
            let arg = try parseExpression()
            try expectSymbol(")")
            return .subject(arg)

        case .keyword("PREDICATE"):
            advance()
            try expectSymbol("(")
            let arg = try parseExpression()
            try expectSymbol(")")
            return .predicate(arg)

        case .keyword("OBJECT"):
            advance()
            try expectSymbol("(")
            let arg = try parseExpression()
            try expectSymbol(")")
            return .object(arg)

        default:
            throw ParseError.invalidSyntax(
                message: "Unknown built-in function",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    /// Generic function call parser for built-in functions with comma-separated arguments.
    /// Handles 1-arg, 2-arg, and variadic functions uniformly.
    /// keyword '(' Expression ( ',' Expression )* ')'
    private func parseGenericFunctionCall() throws -> Expression {
        guard case .keyword(let name) = currentToken else {
            throw ParseError.invalidSyntax(
                message: "Expected function name",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        advance()
        try expectSymbol("(")
        var args: [Expression] = []
        if !isSymbol(")") {
            args.append(try parseExpression())
            while isSymbol(",") {
                advance()
                args.append(try parseExpression())
            }
        }
        try expectSymbol(")")
        return .function(FunctionCall(name: name, arguments: args))
    }

    /// [127] Aggregate ::= 'COUNT' '(' 'DISTINCT'? ( '*' | Expression ) ')'
    ///                    | 'SUM'/'AVG'/'MIN'/'MAX'/'SAMPLE' '(' 'DISTINCT'? Expression ')'
    private func parseSPARQLAggregate() throws -> Expression {
        guard case .keyword(let funcName) = currentToken else {
            throw ParseError.invalidSyntax(
                message: "Expected aggregate function",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        advance()
        try expectSymbol("(")

        var distinct = false
        if case .keyword("DISTINCT") = currentToken {
            distinct = true
            advance()
        }

        var arg: Expression?
        if isSymbol("*") {
            advance()
            arg = nil
        } else {
            arg = try parseExpression()
        }

        try expectSymbol(")")

        switch funcName {
        case "COUNT":
            return .aggregate(.count(arg, distinct: distinct))
        case "SUM":
            return .aggregate(.sum(arg ?? .literal(.null), distinct: distinct))
        case "AVG":
            return .aggregate(.avg(arg ?? .literal(.null), distinct: distinct))
        case "MIN":
            return .aggregate(.min(arg ?? .literal(.null)))
        case "MAX":
            return .aggregate(.max(arg ?? .literal(.null)))
        case "SAMPLE":
            return .aggregate(.sample(arg ?? .literal(.null)))
        default:
            throw ParseError.invalidSyntax(
                message: "Unknown aggregate function: \(funcName)",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    /// [127] GROUP_CONCAT '(' 'DISTINCT'? Expression ( ';' 'SEPARATOR' '=' String )? ')'
    private func parseGroupConcat() throws -> Expression {
        advance() // consume GROUP_CONCAT
        try expectSymbol("(")

        var distinct = false
        if case .keyword("DISTINCT") = currentToken {
            distinct = true
            advance()
        }

        let expr = try parseExpression()

        var separator: String?
        if isSymbol(";") {
            advance()
            try expect("SEPARATOR")
            try expectSymbol("=")
            guard case .string(let sep, _, _) = currentToken else {
                throw ParseError.invalidSyntax(
                    message: "Expected separator string",
                    position: input.distance(from: input.startIndex, to: position)
                )
            }
            separator = sep
            advance()
        }

        try expectSymbol(")")
        return .aggregate(.groupConcat(expr, separator: separator, distinct: distinct))
    }

    /// [120] ExpressionList ::= NIL | '(' Expression ( ',' Expression )* ')'
    private func parseExpressionList() throws -> [Expression] {
        try expectSymbol("(")
        if isSymbol(")") {
            advance()
            return []
        }
        var args: [Expression] = []
        args.append(try parseExpression())
        while isSymbol(",") {
            advance()
            args.append(try parseExpression())
        }
        try expectSymbol(")")
        return args
    }

    /// [70] FunctionCall ::= iri ArgList
    /// [71] ArgList ::= NIL | '(' 'DISTINCT'? Expression ( ',' Expression )* ')'
    /// [128] iriOrFunction ::= iri ArgList?
    private func parseIRIFunctionCall(iri: String) throws -> Expression {
        try expectSymbol("(")
        var distinct = false
        if case .keyword("DISTINCT") = currentToken {
            distinct = true
            advance()
        }
        var args: [Expression] = []
        if !isSymbol(")") {
            args.append(try parseExpression())
            while isSymbol(",") {
                advance()
                args.append(try parseExpression())
            }
        }
        try expectSymbol(")")
        return .function(FunctionCall(name: iri, arguments: args, distinct: distinct))
    }

    private func parseExpression() throws -> Expression {
        try parseOrExpression()
    }

    private func parseOrExpression() throws -> Expression {
        var left = try parseAndExpression()
        while case .symbol("||") = currentToken {
            advance()
            let right = try parseAndExpression()
            left = .or(left, right)
        }
        return left
    }

    private func parseAndExpression() throws -> Expression {
        var left = try parseRelationalExpression()
        while case .symbol("&&") = currentToken {
            advance()
            let right = try parseRelationalExpression()
            left = .and(left, right)
        }
        return left
    }

    private func parseRelationalExpression() throws -> Expression {
        let left = try parseAdditiveExpression()

        switch currentToken {
        case .symbol("="):
            advance()
            return .equal(left, try parseAdditiveExpression())
        case .symbol("!="):
            advance()
            return .notEqual(left, try parseAdditiveExpression())
        case .symbol("<"):
            advance()
            return .lessThan(left, try parseAdditiveExpression())
        case .symbol(">"):
            advance()
            return .greaterThan(left, try parseAdditiveExpression())
        case .symbol("<="):
            advance()
            return .lessThanOrEqual(left, try parseAdditiveExpression())
        case .symbol(">="):
            advance()
            return .greaterThanOrEqual(left, try parseAdditiveExpression())
        case .keyword("IN"):
            advance()
            try expectSymbol("(")
            var values: [Expression] = []
            if !isSymbol(")") {
                var first = true
                while first || isSymbol(",") {
                    if !first { advance() }
                    first = false
                    values.append(try parseExpression())
                }
            }
            try expectSymbol(")")
            return .inList(left, values: values)
        default:
            return left
        }
    }

    private func parseAdditiveExpression() throws -> Expression {
        var left = try parseMultiplicativeExpression()
        while case .symbol(let s) = currentToken, ["+", "-"].contains(s) {
            advance()
            let right = try parseMultiplicativeExpression()
            if s == "+" {
                left = .add(left, right)
            } else {
                left = .subtract(left, right)
            }
        }
        return left
    }

    private func parseMultiplicativeExpression() throws -> Expression {
        var left = try parseUnaryExpression()
        while case .symbol(let s) = currentToken, ["*", "/"].contains(s) {
            advance()
            let right = try parseUnaryExpression()
            if s == "*" {
                left = .multiply(left, right)
            } else {
                left = .divide(left, right)
            }
        }
        return left
    }

    private func parseUnaryExpression() throws -> Expression {
        switch currentToken {
        case .symbol("!"):
            advance()
            return .not(try parseUnaryExpression())
        case .symbol("-"):
            advance()
            return .negate(try parseUnaryExpression())
        default:
            return try parsePrimaryExpression()
        }
    }

    /// [119] PrimaryExpression ::= BrackettedExpression | BuiltInCall | iriOrFunction
    ///                            | RDFLiteral | NumericLiteral | BooleanLiteral | Var
    private func parsePrimaryExpression() throws -> Expression {
        switch currentToken {
        // BrackettedExpression
        case .symbol("("):
            advance()
            let expr = try parseExpression()
            try expectSymbol(")")
            return expr

        // Var
        case .variable(let name):
            advance()
            return .variable(Variable(name))

        // iriOrFunction [128]: iri ArgList?
        case .iri(let iri):
            advance()
            if isSymbol("(") {
                return try parseIRIFunctionCall(iri: iri)
            }
            return .literal(.iri(iri))

        // iriOrFunction [128]: prefixedName ArgList?
        case .prefixedName(let prefix, let local):
            advance()
            let resolved = resolvePrefixedName(prefix: prefix, local: local)
            if isSymbol("(") {
                return try parseIRIFunctionCall(iri: resolved)
            }
            return .literal(.iri(resolved))

        // RDFLiteral
        case .string(let value, let language, let datatype):
            advance()
            if let lang = language {
                return .literal(.langLiteral(value: value, language: lang))
            } else if let dt = datatype {
                return .literal(.typedLiteral(value: value, datatype: dt))
            } else {
                return .literal(.string(value))
            }

        // NumericLiteral
        case .integer(let n):
            advance()
            return .literal(.int(Int64(n) ?? 0))

        case .decimal(let n), .double(let n):
            advance()
            return .literal(.double(Double(n) ?? 0))

        // BooleanLiteral
        case .keyword("TRUE"):
            advance()
            return .literal(.bool(true))

        case .keyword("FALSE"):
            advance()
            return .literal(.bool(false))

        // BuiltInCall [121]
        case .keyword(let kw) where Self.builtInFunctionKeywords.contains(kw):
            return try parseBuiltInCall()

        // RDF-star: << subject predicate object >> as expression
        case .symbol("<<"):
            advance()
            let subject = try parseTerm()
            let predicate = try parseTerm()
            let object = try parseTerm()
            try expectSymbol(">>")
            let qt = SPARQLTerm.quotedTriple(subject: subject, predicate: predicate, object: object)
            return qt.toExpression()

        default:
            throw ParseError.invalidSyntax(
                message: "Expected expression",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    private func parseGroupCondition() throws -> [Expression] {
        var expressions: [Expression] = []

        while isVariable() || isSymbol("(") {
            switch currentToken {
            case .variable(let name):
                expressions.append(.variable(Variable(name)))
                advance()
            case .symbol("("):
                advance()
                let expr = try parseExpression()
                if isKeyword("AS") {
                    advance()
                    // Skip alias variable
                    if isVariable() { advance() }
                }
                try expectSymbol(")")
                expressions.append(expr)
            default:
                break
            }
        }

        return expressions
    }

    private func parseOrderCondition() throws -> [SortKey] {
        var keys: [SortKey] = []

        while isVariable() || isKeyword("ASC") || isKeyword("DESC") || isSymbol("(") {
            var direction: SortDirection = .ascending
            if isKeyword("ASC") {
                advance()
            } else if isKeyword("DESC") {
                direction = .descending
                advance()
            }

            let expr: Expression
            if isSymbol("(") {
                advance()
                expr = try parseExpression()
                try expectSymbol(")")
            } else if case .variable(let name) = currentToken {
                expr = .variable(Variable(name))
                advance()
            } else {
                break
            }

            keys.append(SortKey(expr, direction: direction))
        }

        return keys
    }

    private func parseInlineData() throws -> GraphPattern {
        try expect("VALUES")

        var variables: [String] = []

        // Parse variable list
        if case .symbol("(") = currentToken {
            advance()
            while case .variable(let name) = currentToken {
                variables.append(name)
                advance()
            }
            try expectSymbol(")")
        } else if case .variable(let name) = currentToken {
            variables.append(name)
            advance()
        }

        // Parse data block
        try expectSymbol("{")
        var bindings: [[Literal?]] = []

        while case .symbol("(") = currentToken {
            advance()
            var row: [Literal?] = []
            for _ in variables {
                if case .keyword("UNDEF") = currentToken {
                    row.append(nil)
                    advance()
                } else {
                    let term = try parseTerm()
                    row.append(term.literalValue)
                }
            }
            try expectSymbol(")")
            bindings.append(row)
        }

        try expectSymbol("}")

        return .values(variables: variables, bindings: bindings)
    }

    private func parseConstructQuery() throws -> ConstructQuery {
        log("parseConstructQuery() START")
        try expect("CONSTRUCT")

        // Parse template
        try expectSymbol("{")
        var template: [TriplePattern] = []
        var loopCount = 0
        while currentToken != .symbol("}") {
            loopCount += 1
            log("parseConstructQuery() loop[\(loopCount)] token: \(currentToken)")
            if case .eof = currentToken {
                log("parseConstructQuery() unexpected EOF")
                throw ParseError.unexpectedEndOfInput(expected: "}")
            }
            if case .basic(let triples) = try parseTriplesBlock() {
                template.append(contentsOf: triples)
            }
            if case .symbol(".") = currentToken { advance() }
        }
        log("parseConstructQuery() END, template count: \(template.count)")
        try expectSymbol("}")

        // Parse WHERE
        var pattern: GraphPattern = .basic([])
        if case .keyword("WHERE") = currentToken {
            advance()
            pattern = try parseGroupGraphPattern()
        }

        return ConstructQuery(template: template, pattern: pattern)
    }

    private func parseAskQuery() throws -> AskQuery {
        try expect("ASK")
        let pattern = try parseGroupGraphPattern()
        return AskQuery(pattern: pattern)
    }

    private func parseDescribeQuery() throws -> DescribeQuery {
        log("parseDescribeQuery() START")
        try expect("DESCRIBE")

        var resources: [SPARQLTerm] = []
        var loopCount = 0

        if case .symbol("*") = currentToken {
            log("parseDescribeQuery() found '*'")
            advance()
        } else {
            while true {
                loopCount += 1
                log("parseDescribeQuery() loop[\(loopCount)] token: \(currentToken)")
                var madeProgress = false
                switch currentToken {
                case .variable(let name):
                    resources.append(.variable(name))
                    advance()
                    madeProgress = true
                case .iri(let iri):
                    resources.append(.iri(iri))
                    advance()
                    madeProgress = true
                case .prefixedName(let prefix, let local):
                    resources.append(.prefixedName(prefix: prefix, local: local))
                    advance()
                    madeProgress = true
                default:
                    log("parseDescribeQuery() default case")
                    break
                }
                if case .keyword("WHERE") = currentToken {
                    log("parseDescribeQuery() found WHERE, breaking")
                    break
                }
                if case .eof = currentToken {
                    log("parseDescribeQuery() found EOF, breaking")
                    break
                }
                if !madeProgress {
                    log("parseDescribeQuery() no progress, breaking")
                    break
                }
            }
        }
        log("parseDescribeQuery() resources count: \(resources.count)")

        var pattern: GraphPattern?
        if case .keyword("WHERE") = currentToken {
            advance()
            pattern = try parseGroupGraphPattern()
        }

        return DescribeQuery(resources: resources, pattern: pattern)
    }
}
