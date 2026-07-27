/// SPARQLParser.swift
/// SPARQL Query Parser
///
/// Reference:
/// - W3C SPARQL 1.1 Query Language
/// - W3C SPARQL 1.2 (Draft)

import DatabaseTypes
import DatabaseKit
import Synchronization

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
    private enum DatatypeReference: Sendable, Equatable {
        case iri(String)
        case prefixedName(prefix: String, local: String)
    }

    private enum Token: Sendable, Equatable {
        case keyword(String)
        case iri(String)
        case prefixedName(prefix: String, local: String)
        case variable(String)
        case string(
            String,
            language: String?,
            datatype: DatatypeReference?,
            direction: String?
        )
        case integer(String)
        case decimal(String)
        case double(String)
        case blankNode(String)
        case symbol(String)
        case eof
    }

    private struct NegatedPathPredicate {
        let iri: RDFPredicateIRI
        let isInverse: Bool
    }

    private struct GroupBinding {
        let variable: String
        let expression: Expression
    }

    private struct ParsedGroupConditions {
        var expressions: [Expression] = []
        var bindings: [GroupBinding] = []
    }

    private struct ParsedSolutionModifiers {
        let value: SPARQLSolutionModifiers
        let groupBindings: [GroupBinding]
    }

    private var input: String
    private var position: String.Index
    private var currentToken: Token
    private var prefixes: [String: String]
    private var baseIRI: String?
    private var blankNodeCounter: Int = 0
    private var pendingTriples: [TriplePattern] = []
    private var sparqlVersion: String?
    private var lexicalError: ParseError?
    private var structuralError: QueryStructuralValidationError?
    private let structuralLimits: QueryStructuralLimits
    private var structuralLedger: QueryStructuralResourceLedger

    public init(
        structuralLimits: QueryStructuralLimits = .default
    ) {
        self.input = ""
        self.position = "".startIndex
        self.currentToken = .eof
        self.prefixes = SPARQLTerm.commonPrefixes
        self.baseIRI = nil
        self.blankNodeCounter = 0
        self.pendingTriples = []
        self.sparqlVersion = nil
        self.lexicalError = nil
        self.structuralError = nil
        self.structuralLimits = structuralLimits
        self.structuralLedger = QueryStructuralResourceLedger(
            limits: structuralLimits
        )
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
        // SPARQL 1.2: Language direction functions
        "LANGDIR", "HASLANG", "HASLANGDIR", "STRLANGDIR",
        // 1-arg: IRI
        "IRI", "URI",
        // 1-arg: Numeric
        "ABS", "CEIL", "FLOOR", "ROUND",
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
    private static let debugEnabled = Mutex(false)
    public static func enableDebug(_ enabled: Bool) {
        debugEnabled.withLock { $0 = enabled }
    }
    private func log(_ message: String) {
        if Self.debugEnabled.withLock({ $0 }) {
            print("[SPARQLParser] \(message)")
        }
    }

    /// Parse one SPARQL query form or one non-empty SPARQL Update request.
    public func parse(_ sparql: String) throws -> QueryStatement {
        log("parse() START: '\(sparql.prefix(50))...'")
        resetParserState(for: sparql)

        return try withLexicalErrorPrecedence {
            advance()
            log("parse() initial token: \(currentToken)")

            // Parse prologue (PREFIX and BASE declarations)
            try parsePrologue()
            log("parse() after prologue, token: \(currentToken)")

            let statement: QueryStatement
            if isSPARQLUpdateStart {
                statement = .sparqlUpdate(try parseSPARQLUpdateRequest())
            } else {
                switch currentToken {
                case .keyword("SELECT"):
                    statement = .select(try parseSelectQuery())
                case .keyword("CONSTRUCT"):
                    statement = .construct(try parseConstructQuery())
                case .keyword("ASK"):
                    statement = .ask(try parseAskQuery())
                case .keyword("DESCRIBE"):
                    statement = .describe(try parseDescribeQuery())
                default:
                    throw ParseError.invalidSyntax(
                        message: "Expected query or non-empty SPARQL Update request",
                        position: input.distance(
                            from: input.startIndex,
                            to: position
                        )
                    )
                }
            }

            try expectEndOfInput()
            try validateBlankNodeScope(statement)
            return statement
        }
    }

    /// Parse a SPARQL SELECT query
    public func parseSelect(_ sparql: String) throws -> SelectQuery {
        resetParserState(for: sparql)

        return try withLexicalErrorPrecedence {
            advance()
            try parsePrologue()
            let query = try parseSelectQuery()
            try expectEndOfInput()
            try validateBlankNodeScope(query)
            return query
        }
    }

    /// Parse one non-empty, ordered SPARQL Update request.
    public func parseUpdate(_ sparql: String) throws -> SPARQLUpdateRequest {
        resetParserState(for: sparql)

        return try withLexicalErrorPrecedence {
            advance()
            try parsePrologue()
            guard isSPARQLUpdateStart else {
                throw ParseError.invalidSyntax(
                    message: "Expected a non-empty SPARQL Update request",
                    position: input.distance(
                        from: input.startIndex,
                        to: position
                    )
                )
            }
            let request = try parseSPARQLUpdateRequest()
            try expectEndOfInput()
            try validateBlankNodeScope(request)
            return request
        }
    }

    private func resetParserState(for sparql: String) {
        input = sparql
        position = input.startIndex
        prefixes = SPARQLTerm.commonPrefixes
        baseIRI = nil
        blankNodeCounter = 0
        pendingTriples = []
        sparqlVersion = nil
        lexicalError = nil
        structuralError = nil
        structuralLedger = QueryStructuralResourceLedger(
            limits: structuralLimits
        )
    }

    private func withLexicalErrorPrecedence<Result>(
        _ operation: () throws -> Result
    ) throws -> Result {
        do {
            let result = try operation()
            if let lexicalError {
                throw lexicalError
            }
            if let structuralError {
                throw structuralError
            }
            return result
        } catch {
            if let lexicalError {
                throw lexicalError
            }
            if let structuralError {
                throw structuralError
            }
            throw error
        }
    }

    private func validateBlankNodeScope(_ statement: QueryStatement) throws {
        do {
            try SPARQLSemanticValidator.validate(
                statement,
                limits: structuralLimits
            )
        } catch .structural(let error) {
            throw error
        } catch {
            throw ParseError.invalidSyntax(
                message: String(describing: error),
                position: 0
            )
        }
    }

    private func validateBlankNodeScope(_ query: SelectQuery) throws {
        do {
            try SPARQLSemanticValidator.validate(
                query,
                limits: structuralLimits
            )
        } catch .structural(let error) {
            throw error
        } catch {
            throw ParseError.invalidSyntax(
                message: String(describing: error),
                position: 0
            )
        }
    }

    private func validateBlankNodeScope(
        _ request: SPARQLUpdateRequest
    ) throws {
        do {
            try SPARQLSemanticValidator.validate(
                request,
                limits: structuralLimits
            )
        } catch .structural(let error) {
            throw error
        } catch {
            throw ParseError.invalidSyntax(
                message: String(describing: error),
                position: 0
            )
        }
    }

    private func enterStructuralNesting() throws {
        try structuralLedger.enterNesting()
    }

    private func leaveStructuralNesting() {
        do {
            try structuralLedger.leaveNesting()
        } catch {
            if structuralError == nil {
                structuralError = error
            }
        }
    }

    private func consumeValuesResource(
        _ amount: UInt64,
        resource: QueryStructuralValidationError.Resource
    ) throws {
        switch resource {
        case .valuesRows, .valuesVariables, .valuesCells:
            try structuralLedger.consume(resource, amount: amount)
        case .nestingDepth, .inputTokens, .totalNodes, .collectionElements,
             .basicGraphPatterns,
             .triplePatterns, .reifiedTripleExpansions:
            preconditionFailure("Unexpected parser resource")
        }
    }

    private func consumeStructuralResource(
        _ resource: QueryStructuralValidationError.Resource,
        amount: UInt64 = 1
    ) throws {
        try structuralLedger.consume(resource, amount: amount)
    }

    private func admitCollectionElement(
        amount: UInt64 = 1
    ) throws {
        try consumeStructuralResource(.collectionElements, amount: amount)
    }

    private func makeStructuralNode<Value>(
        _ value: @autoclosure () -> Value
    ) throws -> Value {
        try consumeStructuralResource(.totalNodes)
        return value()
    }

    private func makeLiteralTerm(
        _ literal: Literal
    ) throws -> SPARQLTerm {
        let admittedLiteral = try makeStructuralNode(literal)
        return try makeStructuralNode(.literal(admittedLiteral))
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

    private func makeExpression(
        from term: SPARQLTerm
    ) throws -> Expression {
        switch term {
        case .variable(let name):
            return try makeStructuralNode(.variable(Variable(name)))
        case .literal(let literal):
            return try makeLiteralExpression(literal)
        case .iri(let iri):
            return try makeLiteralExpression(.iri(iri))
        case .blankNode(let identifier):
            return try makeLiteralExpression(.blankNode(identifier))
        case .tripleTerm(let subject, let predicate, let object),
             .reifiedTriple(let subject, let predicate, let object, _):
            let subjectExpression = try makeExpression(from: subject)
            let predicateExpression = try makeExpression(from: predicate)
            let objectExpression = try makeExpression(from: object)
            return try makeStructuralNode(
                .triple(
                    subject: subjectExpression,
                    predicate: predicateExpression,
                    object: objectExpression
                )
            )
        }
    }

    private func makeExistsExpression(
        pattern: GraphPattern,
        negated: Bool
    ) throws -> Expression {
        let source = try makeStructuralNode(DataSource.graphPattern(pattern))
        let query = try makeStructuralNode(
            SelectQuery(projection: .all, source: source)
        )
        let exists = try makeStructuralNode(Expression.exists(query))
        if negated {
            return try makeStructuralNode(.not(exists))
        }
        return exists
    }

    private func makeTriplePattern(
        subject: SPARQLTerm,
        predicate: SPARQLTerm,
        object: SPARQLTerm
    ) throws -> TriplePattern {
        try consumeStructuralResource(.triplePatterns)
        try consumeStructuralResource(.totalNodes)
        try consumeStructuralResource(.collectionElements)
        return TriplePattern(
            subject: subject,
            predicate: predicate,
            object: object
        )
    }

    private func admitBasicGraphPattern() throws {
        try consumeStructuralResource(.basicGraphPatterns)
        try consumeStructuralResource(.totalNodes)
    }

    private func makeBasicGraphPattern(
        _ triples: [TriplePattern]
    ) throws -> GraphPattern {
        try makeBasicGraphPattern(
            BasicGraphPattern(triples: triples)
        )
    }

    private func makeBasicGraphPattern(
        _ pattern: consuming BasicGraphPattern
    ) throws -> GraphPattern {
        try admitBasicGraphPattern()
        return .basic(pattern)
    }

    private func makePropertyPathPattern(
        subject: SPARQLTerm,
        path: PropertyPath,
        object: SPARQLTerm
    ) throws -> SPARQLPropertyPathPattern {
        try consumeStructuralResource(.totalNodes)
        try consumeStructuralResource(.collectionElements)
        return SPARQLPropertyPathPattern(
            subject: subject,
            path: path,
            object: object
        )
    }

    private func requireTriplePatterns(
        from pattern: GraphPattern,
        errorMessage: String
    ) throws -> [TriplePattern] {
        guard case .basic(let basicGraphPattern) = pattern else {
            throw ParseError.invalidSyntax(
                message: errorMessage,
                position: input.distance(
                    from: input.startIndex,
                    to: position
                )
            )
        }
        do {
            return try basicGraphPattern.triplePatterns()
        } catch {
            throw ParseError.invalidSyntax(
                message: errorMessage,
                position: input.distance(
                    from: input.startIndex,
                    to: position
                )
            )
        }
    }

    private func makeReifiedTripleTerm(
        subject: SPARQLTerm,
        predicate: SPARQLTerm,
        object: SPARQLTerm,
        reifier: SPARQLTerm
    ) throws -> SPARQLTerm {
        try consumeStructuralResource(.reifiedTripleExpansions)
        try consumeStructuralResource(.totalNodes)
        return .reifiedTriple(
            subject: subject,
            predicate: predicate,
            object: object,
            reifier: reifier
        )
    }

    private func makeQuad(
        graph: SPARQLTerm?,
        triple: TriplePattern
    ) throws -> Quad {
        try consumeStructuralResource(.totalNodes)
        try admitCollectionElement()
        return Quad(graph: graph, triple: triple)
    }
}

// MARK: - Tokenizer

extension SPARQLParser {
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

        // Variables: ?var or $var (requires at least one varChar after ? or $)
        // Bare '?' without following varChar is a symbol (path modifier)
        if char == "?" || char == "$" {
            let nextIdx = input.index(after: position)
            if nextIdx < input.endIndex && isVarChar(input[nextIdx]) {
                position = nextIdx
                let start = position
                while position < input.endIndex && isVarChar(input[position]) {
                    position = input.index(after: position)
                }
                admitToken(.variable(String(input[start..<position])))
                return
            } else if char == "?" {
                // Bare '?' — property path ZeroOrOne modifier
                position = nextIdx
                admitToken(.symbol("?"))
                return
            }
            // '$' without varChar — fall through to symbol handling
        }

        // Multi-character symbols (must be checked before IRI to distinguish << from <iri>)
        let twoChar = String(input[position...].prefix(2))
        if ["<=", ">=", "!=", "&&", "||", "^^", "<<", ">>", "{|", "|}"].contains(twoChar) {
            position = input.index(position, offsetBy: 2)
            admitToken(.symbol(twoChar))
            return
        }

        // IRIs: <...>
        if char == "<" {
            let openingOffset = input.distance(
                from: input.startIndex,
                to: position
            )
            position = input.index(after: position)
            let start = position
            while position < input.endIndex && input[position] != ">" {
                if isForbiddenIRIReferenceCharacter(input[position]) {
                    lexicalError = .invalidIRI(
                        "Invalid character in IRI reference at position \(input.distance(from: input.startIndex, to: position))"
                    )
                }
                position = input.index(after: position)
            }
            let iri = String(input[start..<position])
            guard position < input.endIndex else {
                lexicalError = .invalidIRI(
                    "Unterminated IRI reference at position \(openingOffset)"
                )
                currentToken = .eof
                return
            }
            position = input.index(after: position)
            admitToken(.iri(iri))
            return
        }

        // Blank nodes: _:name
        if char == "_" && position < input.index(before: input.endIndex) && input[input.index(after: position)] == ":" {
            position = input.index(position, offsetBy: 2)
            let start = position
            while position < input.endIndex && isVarChar(input[position]) {
                position = input.index(after: position)
            }
            admitToken(.blankNode(String(input[start..<position])))
            return
        }

        // Strings
        if char == "\"" || char == "'" {
            let quote = char
            let tripleQuote = String(input[position...].prefix(3)) == String(repeating: quote, count: 3)

            if tripleQuote {
                position = input.index(position, offsetBy: 3)
                admitToken(parseLongString(quote: quote))
            } else {
                position = input.index(after: position)
                admitToken(parseShortString(quote: quote))
            }
            return
        }

        // Numbers
        if char.isNumber || (char == "." && position < input.index(before: input.endIndex) && input[input.index(after: position)].isNumber) {
            admitToken(parseNumber())
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
                    admitToken(.prefixedName(prefix: prefix, local: local))
                    return
                }
            }

            let upper = word.uppercased()
            if isKeywordString(upper) {
                admitToken(.keyword(upper))
            } else {
                // Bare word - treat as prefixed name with empty prefix
                admitToken(.prefixedName(prefix: "", local: word))
            }
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

    private func isForbiddenIRIReferenceCharacter(_ character: Character) -> Bool {
        if character.isWhitespace { return true }
        switch character {
        case "<", "\"", "{", "}", "|", "^", "`", "\\":
            return true
        default:
            return character.unicodeScalars.contains { $0.value <= 0x20 }
        }
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
                       "ISTRIPLE", "TRIPLE", "SUBJECT", "PREDICATE", "OBJECT",
                       // SPARQL 1.2
                       "VERSION", "LATERAL", "LANGDIR", "HASLANG", "HASLANGDIR", "STRLANGDIR",
                       // SPARQL Update
                       "INSERT", "DELETE", "DATA", "INTO", "WITH", "USING",
                       "LOAD", "CLEAR", "CREATE", "DROP", "DEFAULT", "ALL", "COPY", "MOVE", "ADD",
                       "TO"]
        return keywords.contains(word)
    }

    private func parseShortString(quote: Character) -> Token {
        var value = ""
        var terminated = false
        while position < input.endIndex && input[position] != quote {
            if input[position] == "\n" || input[position] == "\r" {
                recordLexicalError("Short string literals cannot contain an unescaped line break")
            }
            if input[position] == "\\" {
                position = input.index(after: position)
                if position < input.endIndex {
                    value.append(parseEscape())
                } else {
                    recordLexicalError("String literal ends with an incomplete escape")
                }
            } else {
                value.append(input[position])
                position = input.index(after: position)
            }
        }
        if position < input.endIndex {
            position = input.index(after: position)
            terminated = true
        }
        if !terminated {
            recordLexicalError("Unterminated short string literal")
        }

        let (language, datatype, direction) = parseLiteralSuffix()
        return .string(value, language: language, datatype: datatype, direction: direction)
    }

    private func parseLongString(quote: Character) -> Token {
        var value = ""
        let endQuote = String(repeating: quote, count: 3)
        var terminated = false

        while position < input.endIndex {
            if String(input[position...].prefix(3)) == endQuote {
                position = input.index(position, offsetBy: 3)
                terminated = true
                break
            }
            if input[position] == "\\" {
                position = input.index(after: position)
                if position < input.endIndex {
                    value.append(parseEscape())
                } else {
                    recordLexicalError("Long string literal ends with an incomplete escape")
                }
            } else {
                value.append(input[position])
                position = input.index(after: position)
            }
        }
        if !terminated {
            recordLexicalError("Unterminated long string literal")
        }

        let (language, datatype, direction) = parseLiteralSuffix()
        return .string(value, language: language, datatype: datatype, direction: direction)
    }

    /// Parse optional @language(--direction)? or ^^datatype suffix after a string literal
    private func parseLiteralSuffix() -> (
        language: String?,
        datatype: DatatypeReference?,
        direction: String?
    ) {
        var language: String?
        var datatype: DatatypeReference?
        var direction: String?

        if position < input.endIndex && input[position] == "@" {
            position = input.index(after: position)
            let langStart = position
            while position < input.endIndex
                && (input[position].isLetter
                    || input[position].isNumber
                    || input[position] == "-") {
                // Lookahead: stop at `--` (direction separator, SPARQL 1.2)
                if input[position] == "-" {
                    let next = input.index(after: position)
                    if next < input.endIndex && input[next] == "-" {
                        break
                    }
                }
                position = input.index(after: position)
            }
            let rawLanguage = String(input[langStart..<position])
            language = rawLanguage
            do {
                let validated = try RDFLanguageTag(rawLanguage)
                language = validated.rawValue
            } catch {
                recordLexicalError("Invalid RDF language tag: \(rawLanguage)")
            }
            // Check for direction suffix: --ltr or --rtl (SPARQL 1.2)
            let dirRemaining = input[position...]
            if dirRemaining.prefix(2) == "--" {
                position = input.index(position, offsetBy: 2)
                let dirStart = position
                while position < input.endIndex && input[position].isLetter {
                    position = input.index(after: position)
                }
                let rawDirection = String(input[dirStart..<position])
                let normalizedDirection = rawDirection.lowercased()
                if let validated = RDFDirection(
                    rawValue: normalizedDirection
                ) {
                    direction = validated.rawValue
                } else {
                    direction = rawDirection
                    recordLexicalError(
                        "Invalid RDF base direction: \(rawDirection)"
                    )
                }
            }
        } else if position < input.endIndex {
            let remaining = input[position...]
            if remaining.prefix(2) == "^^" {
                position = input.index(position, offsetBy: 2)
                advance()
                if case .iri(let iri) = currentToken {
                    datatype = .iri(iri)
                } else if case .prefixedName(let prefix, let local) = currentToken {
                    datatype = .prefixedName(prefix: prefix, local: local)
                } else {
                    recordLexicalError("Expected an IRI after the datatype marker")
                }
            }
        }

        return (language, datatype, direction)
    }

    private func parseEscape() -> Character {
        let char = input[position]
        position = input.index(after: position)
        switch char {
        case "b": return "\u{8}"
        case "f": return "\u{c}"
        case "n": return "\n"
        case "r": return "\r"
        case "t": return "\t"
        case "\\": return "\\"
        case "\"": return "\""
        case "'": return "'"
        case "u":
            // \uXXXX — 4 hex digits
            if let scalar = parseHexScalar(digitCount: 4) {
                return Character(scalar)
            }
            recordLexicalError("Invalid \\u escape in string literal")
            return char
        case "U":
            // \UXXXXXXXX — 8 hex digits
            if let scalar = parseHexScalar(digitCount: 8) {
                return Character(scalar)
            }
            recordLexicalError("Invalid \\U escape in string literal")
            return char
        default:
            recordLexicalError("Invalid string escape: \\(char)")
            return char
        }
    }

    private func recordLexicalError(_ message: String) {
        guard lexicalError == nil else { return }
        lexicalError = .invalidSyntax(
            message: message,
            position: input.distance(from: input.startIndex, to: position)
        )
    }

    /// Parse `digitCount` hex characters and return the corresponding Unicode scalar
    private func parseHexScalar(digitCount: Int) -> Unicode.Scalar? {
        var hex = ""
        for _ in 0..<digitCount {
            guard position < input.endIndex else { return nil }
            let c = input[position]
            guard c.isHexDigit else { return nil }
            hex.append(c)
            position = input.index(after: position)
        }
        guard let codePoint = UInt32(hex, radix: 16),
              let scalar = Unicode.Scalar(codePoint) else {
            return nil
        }
        return scalar
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

    private func resolveRequiredPrefixedName(
        prefix: String,
        local: String
    ) throws -> String {
        guard let base = prefixes[prefix] else {
            throw ParseError.invalidSyntax(
                message: "Undefined prefix: \(prefix)",
                position: input.distance(
                    from: input.startIndex,
                    to: position
                )
            )
        }
        return try requireAbsoluteIRI(base + local)
    }

    private func resolveDatatype(
        _ reference: DatatypeReference
    ) throws -> String {
        switch reference {
        case .iri(let iri):
            return try resolveAbsoluteIRI(iri)
        case .prefixedName(let prefix, let local):
            return try resolveRequiredPrefixedName(
                prefix: prefix,
                local: local
            )
        }
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

    private func expectEndOfInput() throws {
        guard case .eof = currentToken else {
            throw ParseError.unexpectedToken(
                expected: "end of input",
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    private func parseResolvedIRI(expected: String) throws -> String {
        let iri: String
        switch currentToken {
        case .iri(let rawValue):
            iri = try resolveAbsoluteIRI(rawValue)
        case .prefixedName(let prefix, let local):
            iri = try resolveRequiredPrefixedName(
                prefix: prefix,
                local: local
            )
        default:
            throw ParseError.unexpectedToken(
                expected: expected,
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        advance()
        return iri
    }

    private func resolveAbsoluteIRI(_ rawValue: String) throws -> String {
        try requireAbsoluteIRI(resolveIRI(rawValue))
    }

    private func requireAbsoluteIRI(_ iri: String) throws -> String {
        guard RDFIRISyntax.isAbsolute(iri) else {
            throw ParseError.invalidIRI(iri)
        }
        return iri
    }

    private func tokenDescription(_ token: Token) -> String {
        switch token {
        case .keyword(let k): return "keyword '\(k)'"
        case .iri(let i): return "IRI <\(i)>"
        case .prefixedName(let p, let l): return "prefixed name '\(p):\(l)'"
        case .variable(let v): return "variable ?\(v)"
        case .string(let s, _, _, _): return "string \"\(s)\""
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

    private func isBuiltInFunctionKeyword() -> Bool {
        guard case .keyword(let kw) = currentToken else { return false }
        return Self.builtInFunctionKeywords.contains(kw)
    }

    private func isIRIOrPrefixedName() -> Bool {
        switch currentToken {
        case .iri: return true
        case .prefixedName: return true
        default: return false
        }
    }

    /// Generate a fresh blank node identifier
    private func freshBlankNode() -> String {
        let id = "_:anon_\(blankNodeCounter)"
        blankNodeCounter += 1
        return id
    }

    /// Parse SPARQL 1.2 annotation syntax: {| predicate object (;  predicate object)* |}
    /// The annotation subject is <<( s p o )>> (quoted triple of the annotated triple).
    private func parseAnnotation(subject: SPARQLTerm, predicate: SPARQLTerm, object: SPARQLTerm) throws -> [TriplePattern] {
        try expectSymbol("{|")
        let annotationSubject = try makeStructuralNode(
            SPARQLTerm.tripleTerm(
                subject: subject,
                predicate: predicate,
                object: object
            )
        )
        var annotationTriples: [TriplePattern] = []

        var firstPred = true
        while !isSymbol("|}") {
            if !firstPred {
                guard isSymbol(";") else { break }
                advance()
                if isSymbol("|}") { break } // trailing semicolon
            }
            firstPred = false
            let annPred = try parseTerm()
            let annObj = try parseTerm()
            annotationTriples.append(
                try makeTriplePattern(
                    subject: annotationSubject,
                    predicate: annPred,
                    object: annObj
                )
            )
            if !pendingTriples.isEmpty {
                annotationTriples.append(contentsOf: pendingTriples)
                pendingTriples.removeAll()
            }
            while isSymbol(",") {
                advance()
                let nextObj = try parseTerm()
                annotationTriples.append(
                    try makeTriplePattern(
                        subject: annotationSubject,
                        predicate: annPred,
                        object: nextObj
                    )
                )
                if !pendingTriples.isEmpty {
                    annotationTriples.append(contentsOf: pendingTriples)
                    pendingTriples.removeAll()
                }
            }
        }
        try expectSymbol("|}")
        return annotationTriples
    }

    /// Resolve a relative IRI against the active BASE without changing IRI spelling.
    private func resolveIRI(_ iri: String) throws -> String {
        do {
            return try SPARQLIRIResolver.resolve(iri, against: baseIRI)
        } catch {
            throw ParseError.invalidIRI(iri)
        }
    }

    // MARK: - Lookahead Helpers (LL(1) decision)

    /// Check if current token can start a TriplesSameSubjectPath
    /// SPARQL 1.1 Grammar [75] VarOrTerm, [97] RDFLiteral, etc.
    private func canStartTriple() -> Bool {
        switch currentToken {
        case .variable, .iri, .prefixedName, .blankNode,
             .string, .integer, .decimal, .double:
            return true
        case .symbol("<<"):  // RDF-star quoted triple
            return true
        case .symbol("["):  // Anonymous blank node
            return true
        case .symbol("("):  // RDF collection
            return true
        case .keyword("TRUE"), .keyword("FALSE"):
            return true
        default:
            return false
        }
    }

    /// Check if current token starts a GraphPatternNotTriples [57]
    private func canStartGraphPatternNotTriples() -> Bool {
        switch currentToken {
        case .symbol("{"):
            return true
        case .keyword("OPTIONAL"), .keyword("MINUS"), .keyword("GRAPH"),
             .keyword("SERVICE"), .keyword("FILTER"), .keyword("BIND"),
             .keyword("VALUES"), .keyword("LATERAL"):
            return true
        default:
            return false
        }
    }

    /// Check if current token can start a Verb or PropertyPath (predicate position)
    /// VerbPath = Path | Var  [SPARQL 1.1 Grammar [78]]
    private func canStartVerb() -> Bool {
        switch currentToken {
        case .variable, .iri, .prefixedName:
            return true
        case .keyword("A"):
            return true
        case .symbol("^"), .symbol("!"), .symbol("("):
            return true
        default:
            return false
        }
    }

    // MARK: - Property Path Parsing

    /// Check if current token is a path modifier (*, +, ?)
    private func isPathModifier() -> Bool {
        switch currentToken {
        case .symbol("*"), .symbol("+"), .symbol("?"):
            return true
        default:
            return false
        }
    }

    /// Check if current token is a path operator (/, |)
    private func isPathOperator() -> Bool {
        switch currentToken {
        case .symbol("/"), .symbol("|"):
            return true
        default:
            return false
        }
    }

    /// [88] Path ::= PathAlternative
    private func parsePropertyPath() throws -> PropertyPath {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }
        return try parsePathAlternative()
    }

    /// [89] PathAlternative ::= PathSequence ( '|' PathSequence )*
    private func parsePathAlternative() throws -> PropertyPath {
        var path = try parsePathSequence()
        while isSymbol("|") {
            advance()
            let right = try parsePathSequence()
            path = try makeStructuralNode(.alternative(path, right))
        }
        return path
    }

    /// [90] PathSequence ::= PathEltOrInverse ( '/' PathEltOrInverse )*
    private func parsePathSequence() throws -> PropertyPath {
        var path = try parsePathEltOrInverse()
        while isSymbol("/") {
            advance()
            let right = try parsePathEltOrInverse()
            path = try makeStructuralNode(.sequence(path, right))
        }
        return path
    }

    /// [92] PathEltOrInverse ::= PathElt | '^' PathElt
    private func parsePathEltOrInverse() throws -> PropertyPath {
        if isSymbol("^") {
            advance()
            let elt = try parsePathElt()
            return try makeStructuralNode(.inverse(elt))
        }
        return try parsePathElt()
    }

    /// [91] PathElt ::= PathPrimary PathMod?
    /// [94] PathMod ::= '*' | '?' | '+'
    private func parsePathElt() throws -> PropertyPath {
        var path = try parsePathPrimary()
        switch currentToken {
        case .symbol("*"):
            advance()
            path = try makeStructuralNode(.zeroOrMore(path))
        case .symbol("+"):
            advance()
            path = try makeStructuralNode(.oneOrMore(path))
        case .symbol("?"):
            advance()
            path = try makeStructuralNode(.zeroOrOne(path))
        default:
            break
        }
        return path
    }

    /// [95] PathPrimary ::= iri | 'a' | '!' PathNegatedPropertySet | '(' Path ')'
    private func parsePathPrimary() throws -> PropertyPath {
        switch currentToken {
        case .iri(let iri):
            advance()
            let resolved = try resolveIRI(iri)
            let predicate = try parsePredicateIRI(resolved)
            return try makeStructuralNode(.iri(predicate))
        case .prefixedName(let prefix, let local):
            advance()
            let resolved = try resolveRequiredPrefixedName(
                prefix: prefix,
                local: local
            )
            let predicate = try parsePredicateIRI(resolved)
            return try makeStructuralNode(.iri(predicate))
        case .keyword("A"):
            advance()
            let predicate = try parsePredicateIRI(
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
            )
            return try makeStructuralNode(.iri(predicate))
        case .symbol("!"):
            advance()
            return try parsePathNegatedPropertySet()
        case .symbol("("):
            advance()
            let path = try parsePropertyPath()
            try expectSymbol(")")
            return path
        default:
            throw ParseError.invalidSyntax(
                message: "Expected property path element (IRI, 'a', '!', or '(')",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    /// [96] PathNegatedPropertySet ::= PathOneInPropertySet
    ///      | '(' ( PathOneInPropertySet ( '|' PathOneInPropertySet )* )? ')'
    /// [97] PathOneInPropertySet ::= iri | 'a' | '^' ( iri | 'a' )
    private func parsePathNegatedPropertySet() throws -> PropertyPath {
        var forward: Set<RDFPredicateIRI>?
        var inverse: Set<RDFPredicateIRI>?

        if isSymbol("(") {
            advance()
            if !isSymbol(")") {
                try add(
                    try parsePathOneInPropertySetIRI(),
                    forward: &forward,
                    inverse: &inverse
                )
                while isSymbol("|") {
                    advance()
                    try add(
                        try parsePathOneInPropertySetIRI(),
                        forward: &forward,
                        inverse: &inverse
                    )
                }
            } else {
                forward = []
            }
            try expectSymbol(")")
        } else {
            try add(
                try parsePathOneInPropertySetIRI(),
                forward: &forward,
                inverse: &inverse
            )
        }

        let set: PropertyPathNegatedSet
        do {
            set = try PropertyPathNegatedSet(
                forward: forward,
                inverse: inverse
            )
        } catch {
            throw ParseError.invalidSyntax(
                message: "Negated property set requires a traversal direction",
                position: input.distance(
                    from: input.startIndex,
                    to: position
                )
            )
        }
        return try makeStructuralNode(.negatedPropertySet(set))
    }

    /// Parse a single IRI from PathOneInPropertySet (iri | 'a' | '^'(iri|'a'))
    private func parsePathOneInPropertySetIRI() throws -> NegatedPathPredicate {
        let isInverse: Bool
        if isSymbol("^") {
            advance()
            isInverse = true
        } else {
            isInverse = false
        }
        let rawIRI: String
        switch currentToken {
        case .iri(let iri):
            advance()
            rawIRI = try resolveIRI(iri)
        case .prefixedName(let prefix, let local):
            advance()
            rawIRI = try resolveRequiredPrefixedName(
                prefix: prefix,
                local: local
            )
        case .keyword("A"):
            advance()
            rawIRI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        default:
            throw ParseError.invalidSyntax(
                message: "Expected IRI or 'a' in negated property set",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        return NegatedPathPredicate(
            iri: try parsePredicateIRI(rawIRI),
            isInverse: isInverse
        )
    }

    private func add(
        _ predicate: NegatedPathPredicate,
        forward: inout Set<RDFPredicateIRI>?,
        inverse: inout Set<RDFPredicateIRI>?
    ) throws {
        if predicate.isInverse {
            if inverse == nil { inverse = [] }
            if inverse?.contains(predicate.iri) != true {
                try admitCollectionElement()
            }
            inverse?.insert(predicate.iri)
        } else {
            if forward == nil { forward = [] }
            if forward?.contains(predicate.iri) != true {
                try admitCollectionElement()
            }
            forward?.insert(predicate.iri)
        }
    }

    private func parsePredicateIRI(
        _ rawValue: String
    ) throws -> RDFPredicateIRI {
        do {
            return try RDFPredicateIRI(rawValue)
        } catch {
            throw ParseError.invalidIRI(rawValue)
        }
    }

    /// Result of parsing a predicate position — either a simple term or a property path
    private enum VerbOrPath {
        case term(SPARQLTerm)
        case path(PropertyPath)
    }

    /// Parse predicate position: simple verb or property path
    /// If the predicate is a single IRI without path operators, returns .term
    /// If path operators follow, returns .path
    private func parseVerbOrPath() throws -> VerbOrPath {
        // Variable in predicate position is always a simple term
        if case .variable(let name) = currentToken {
            advance()
            return .term(try makeStructuralNode(.variable(name)))
        }

        // For IRI/prefixedName/a, check if path operators follow
        // ^ and ! and ( always start a path
        if isSymbol("^") || isSymbol("!") || isSymbol("(") {
            return .path(try parsePropertyPath())
        }

        // Parse the IRI-like token
        let savedPos = position
        let savedTok = currentToken
        let savedStructuralLedger = structuralLedger
        let savedStructuralError = structuralError
        let savedLexicalError = lexicalError

        let iri: String
        switch currentToken {
        case .keyword("A"):
            advance()
            // Check if path operator follows 'a'
            if isPathModifier() || isPathOperator() || isSymbol("^") {
                position = savedPos
                currentToken = savedTok
                structuralLedger = savedStructuralLedger
                structuralError = savedStructuralError
                lexicalError = savedLexicalError
                return .path(try parsePropertyPath())
            }
            return .term(try makeStructuralNode(.rdfType))
        case .iri(let i):
            iri = try resolveAbsoluteIRI(i)
        case .prefixedName(let prefix, let local):
            iri = try resolveRequiredPrefixedName(
                prefix: prefix,
                local: local
            )
        default:
            throw ParseError.invalidSyntax(
                message: "Expected verb or property path",
                position: input.distance(from: input.startIndex, to: position)
            )
        }

        // Consume the IRI token
        advance()

        // Check if path operators follow — if so, backtrack and parse as full path
        if isPathModifier() || isPathOperator() {
            position = savedPos
            currentToken = savedTok
            structuralLedger = savedStructuralLedger
            structuralError = savedStructuralError
            lexicalError = savedLexicalError
            return .path(try parsePropertyPath())
        }

        // Simple IRI verb — use resolved iri
        switch savedTok {
        case .iri:
            return .term(try makeStructuralNode(.iri(iri)))
        case .prefixedName:
            return .term(try makeStructuralNode(.iri(iri)))
        default:
            return .term(try makeStructuralNode(.iri(iri)))
        }
    }
}

// MARK: - Query Parsing

extension SPARQLParser {
    private func parsePrologue() throws {
        while true {
            switch currentToken {
            case .keyword("PREFIX"):
                advance()
                guard case .prefixedName(let prefix, let local) = currentToken,
                      local.isEmpty else {
                    throw ParseError.invalidSyntax(message: "Expected prefix name", position: input.distance(from: input.startIndex, to: position))
                }
                advance()
                guard case .iri(let iri) = currentToken else {
                    throw ParseError.invalidSyntax(message: "Expected IRI", position: input.distance(from: input.startIndex, to: position))
                }
                prefixes[prefix] = try resolveAbsoluteIRI(iri)
                advance()

            case .keyword("BASE"):
                advance()
                guard case .iri(let iri) = currentToken else {
                    throw ParseError.invalidSyntax(message: "Expected IRI", position: input.distance(from: input.startIndex, to: position))
                }
                self.baseIRI = try resolveAbsoluteIRI(iri)
                advance()

            // SPARQL 1.2: VERSION declaration
            case .keyword("VERSION"):
                advance()
                guard case .string(let version, _, _, _) = currentToken else {
                    throw ParseError.invalidSyntax(message: "Expected version string", position: input.distance(from: input.startIndex, to: position))
                }
                self.sparqlVersion = version
                advance()

            default:
                return
            }
        }
    }

    private func parseSelectQuery() throws -> SelectQuery {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }
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

        let dataset = try parseDatasetClauses()

        // WHERE is optional in the grammar, but its group graph pattern is required.
        if case .keyword("WHERE") = currentToken {
            advance()
        }
        let ungroupedPattern = try parseGroupGraphPattern()
        let parsedModifiers = try parseSolutionModifiers()
        let pattern = try applying(
            parsedModifiers.groupBindings,
            to: ungroupedPattern
        )
        let modifiers = parsedModifiers.value

        let source = try makeStructuralNode(DataSource.graphPattern(pattern))
        return try makeStructuralNode(
            SelectQuery(
                projection: projection,
                source: source,
                filter: nil,
                groupBy: modifiers.groupBy.isEmpty ? nil : modifiers.groupBy,
                having: modifiers.combinedHaving,
                orderBy: modifiers.orderBy.isEmpty ? nil : modifiers.orderBy,
                limit: modifiers.limit,
                offset: modifiers.offset,
                distinct: distinct,
                reduced: reduced,
                dataset: dataset
            )
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
                try admitCollectionElement()
                let expression = try makeStructuralNode(
                    Expression.variable(Variable(name))
                )
                items.append(
                    try makeStructuralNode(ProjectionItem(expression))
                )
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
                try admitCollectionElement()
                items.append(
                    try makeStructuralNode(
                        ProjectionItem(expr, alias: alias)
                    )
                )
            default:
                break
            }

            // Check if next token starts a new select item
            if case .variable = currentToken { continue }
            if case .symbol("(") = currentToken { continue }
            break
        }

        guard !items.isEmpty else {
            throw ParseError.invalidSyntax(
                message: "SELECT requires '*' or at least one projection item",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        return .items(items)
    }

    private func parseDatasetClauses() throws -> SPARQLDataset {
        var defaultGraphs: [String] = []
        var namedGraphs: [String] = []
        var foundClause = false

        while isKeyword("FROM") {
            foundClause = true
            advance()
            if isKeyword("NAMED") {
                advance()
                try admitCollectionElement()
                namedGraphs.append(
                    try parseResolvedIRI(expected: "IRI after FROM NAMED")
                )
            } else {
                try admitCollectionElement()
                defaultGraphs.append(
                    try parseResolvedIRI(expected: "IRI after FROM")
                )
            }
        }

        guard foundClause else { return .implicit }
        return .explicit(
            defaultGraphs: defaultGraphs,
            namedGraphs: namedGraphs
        )
    }

    private func parseSolutionModifiers() throws -> ParsedSolutionModifiers {
        var groupBy: [Expression] = []
        var groupBindings: [GroupBinding] = []
        if isKeyword("GROUP") {
            advance()
            try expect("BY")
            let conditions = try parseGroupCondition()
            groupBy = conditions.expressions
            groupBindings = conditions.bindings
            guard !groupBy.isEmpty else {
                throw ParseError.invalidSyntax(
                    message: "GROUP BY requires at least one condition",
                    position: input.distance(from: input.startIndex, to: position)
                )
            }
        }

        var having: [Expression] = []
        if isKeyword("HAVING") {
            advance()
            while isConstraintStart() {
                try admitCollectionElement()
                having.append(try parseConstraint())
            }
            guard !having.isEmpty else {
                throw ParseError.invalidSyntax(
                    message: "HAVING requires at least one condition",
                    position: input.distance(from: input.startIndex, to: position)
                )
            }
        }

        var orderBy: [SortKey] = []
        if isKeyword("ORDER") {
            advance()
            try expect("BY")
            orderBy = try parseOrderCondition()
            guard !orderBy.isEmpty else {
                throw ParseError.invalidSyntax(
                    message: "ORDER BY requires at least one condition",
                    position: input.distance(from: input.startIndex, to: position)
                )
            }
        }

        let limitOffset = try parseLimitOffsetClauses()
        return ParsedSolutionModifiers(
            value: SPARQLSolutionModifiers(
                groupBy: groupBy,
                having: having,
                orderBy: orderBy,
                limit: limitOffset.limit,
                offset: limitOffset.offset
            ),
            groupBindings: groupBindings
        )
    }

    private func applying(
        _ bindings: [GroupBinding],
        to pattern: GraphPattern
    ) throws -> GraphPattern {
        var result = pattern
        for binding in bindings {
            result = try makeStructuralNode(
                .bind(
                    result,
                    variable: binding.variable,
                    expression: binding.expression
                )
            )
        }
        return result
    }

    private func isConstraintStart() -> Bool {
        isSymbol("(") || isBuiltInFunctionKeyword() || isIRIOrPrefixedName()
    }

    private func parseLimitOffsetClauses() throws -> (
        limit: UInt64?,
        offset: UInt64?
    ) {
        var limit: UInt64?
        var offset: UInt64?

        while isKeyword("LIMIT") || isKeyword("OFFSET") {
            let keyword: String
            if isKeyword("LIMIT") {
                keyword = "LIMIT"
                guard limit == nil else {
                    throw duplicateSolutionModifier(keyword)
                }
            } else {
                keyword = "OFFSET"
                guard offset == nil else {
                    throw duplicateSolutionModifier(keyword)
                }
            }

            advance()
            let value = try parseSolutionModifierInteger(after: keyword)
            if keyword == "LIMIT" {
                limit = value
            } else {
                offset = value
            }
        }

        return (limit, offset)
    }

    private func parseSolutionModifierInteger(
        after keyword: String
    ) throws -> UInt64 {
        guard case .integer(let lexicalForm) = currentToken,
              let value = UInt64(lexicalForm) else {
            throw ParseError.invalidSyntax(
                message: "\(keyword) requires a non-negative UInt64 integer",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        advance()
        return value
    }

    private func duplicateSolutionModifier(_ keyword: String) -> ParseError {
        .invalidSyntax(
            message: "Duplicate \(keyword) clause",
            position: input.distance(from: input.startIndex, to: position)
        )
    }

    /// SPARQL 1.1 Grammar [54]
    /// GroupGraphPattern ::= '{' ( SubSelect | GroupGraphPatternSub ) '}'
    private func parseGroupGraphPattern() throws -> GraphPattern {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }
        try expectSymbol("{")
        let pattern: GraphPattern
        if isKeyword("SELECT") {
            // SubSelect: full SELECT query as subquery
            let query = try parseSelectQuery()
            pattern = try makeStructuralNode(.subquery(query))
        } else {
            pattern = try parseGroupGraphPatternSub()
        }
        try expectSymbol("}")
        return pattern
    }

    /// SPARQL 1.1 Grammar [67]
    /// GroupOrUnionGraphPattern ::= GroupGraphPattern ( 'UNION' GroupGraphPattern )*
    private func parseGroupOrUnionGraphPattern() throws -> GraphPattern {
        var result = try parseGroupGraphPattern()
        while isKeyword("UNION") {
            advance() // consume 'UNION'
            let right = try parseGroupGraphPattern()
            result = try makeStructuralNode(.union(result, right))
        }
        return result
    }

    /// SPARQL 1.1 Grammar [55]
    /// GroupGraphPatternSub ::= TriplesBlock? ( GraphPatternNotTriples '.'? TriplesBlock? )*
    ///
    /// Algebra translation per W3C §18.2.2:
    /// - TriplesBlock        → Join(accumulated, BGP)
    /// - OPTIONAL            → LeftJoin(accumulated, opt)
    /// - MINUS               → Minus(accumulated, minus)
    /// - FILTER              → Collected, then applied to the completed group
    /// - BIND                → Extend(accumulated, var, expr)
    /// - GroupOrUnion/Graph/Service/Values → Join(accumulated, pattern)
    private func parseGroupGraphPatternSub() throws -> GraphPattern {
        log("parseGroupGraphPatternSub() START, token: \(currentToken)")

        var accumulated: GraphPattern? = nil
        var filters: [Expression] = []
        var pendingBasicGraphPatternElements: [BasicGraphPatternElement] = []

        func joinAccumulated(_ pattern: GraphPattern) throws {
            guard let existing = accumulated else {
                accumulated = pattern
                return
            }
            try consumeStructuralResource(.totalNodes)
            accumulated = .join(existing, pattern)
        }

        func flushPendingBasicGraphPattern() throws {
            guard !pendingBasicGraphPatternElements.isEmpty else {
                return
            }
            let elements = pendingBasicGraphPatternElements
            pendingBasicGraphPatternElements = []
            try joinAccumulated(
                try makeBasicGraphPattern(
                    BasicGraphPattern(elements: elements)
                )
            )
        }

        func accumulatedOrEmpty() throws -> GraphPattern {
            if let accumulated {
                return accumulated
            }
            return try makeBasicGraphPattern([])
        }

        // 1. Optional leading TriplesBlock
        if canStartTriple() {
            try parseTriplesBlock(
                appendingTo: &pendingBasicGraphPatternElements
            )
            log("parseGroupGraphPatternSub() parsed leading TriplesBlock")
        }

        // 2. ( GraphPatternNotTriples '.'? TriplesBlock? )*
        while canStartGraphPatternNotTriples() {
            log("parseGroupGraphPatternSub() GraphPatternNotTriples, token: \(currentToken)")

            // FILTER is scoped to the completed group and does not terminate
            // the surrounding basic graph pattern. Every other graph-pattern
            // form is a real BGP boundary.
            if !isKeyword("FILTER") {
                try flushPendingBasicGraphPattern()
            }

            // Parse GraphPatternNotTriples [57]
            switch currentToken {
            case .symbol("{"):
                // GroupOrUnionGraphPattern [67]
                let unionPattern = try parseGroupOrUnionGraphPattern()
                try joinAccumulated(unionPattern)

            case .keyword("OPTIONAL"):
                advance()
                let optPattern = try parseGroupGraphPattern()
                try consumeStructuralResource(.totalNodes)
                accumulated = .optional(
                    try accumulatedOrEmpty(),
                    optPattern
                )

            case .keyword("MINUS"):
                advance()
                let minusPattern = try parseGroupGraphPattern()
                try consumeStructuralResource(.totalNodes)
                accumulated = .minus(
                    try accumulatedOrEmpty(),
                    minusPattern
                )

            case .keyword("LATERAL"):
                advance()
                let lateralPattern = try parseGroupGraphPattern()
                try consumeStructuralResource(.totalNodes)
                accumulated = .lateral(
                    try accumulatedOrEmpty(),
                    lateralPattern
                )

            case .keyword("FILTER"):
                advance()
                try admitCollectionElement()
                filters.append(try parseConstraint())

            case .keyword("BIND"):
                advance()
                try expectSymbol("(")
                let expr = try parseExpression()
                try expect("AS")
                guard case .variable(let varName) = currentToken else {
                    throw ParseError.invalidSyntax(
                        message: "Expected variable after AS",
                        position: input.distance(from: input.startIndex, to: position)
                    )
                }
                advance()
                try expectSymbol(")")
                try consumeStructuralResource(.totalNodes)
                accumulated = .bind(
                    try accumulatedOrEmpty(),
                    variable: varName,
                    expression: expr
                )

            case .keyword("VALUES"):
                let valuesPattern = try parseInlineData()
                try joinAccumulated(valuesPattern)

            case .keyword("GRAPH"):
                advance()
                let graphName = try parseGraphSelector()
                let graphPattern = try parseGroupGraphPattern()
                try consumeStructuralResource(.totalNodes)
                try joinAccumulated(
                    .graph(name: graphName, pattern: graphPattern)
                )

            case .keyword("SERVICE"):
                advance()
                var silent = false
                if isKeyword("SILENT") {
                    silent = true
                    advance()
                }
                let endpoint = try parseResolvedIRI(
                    expected: "IRI for SERVICE"
                )
                let servicePattern = try parseGroupGraphPattern()
                try consumeStructuralResource(.totalNodes)
                try joinAccumulated(
                    .service(
                        endpoint: endpoint,
                        pattern: servicePattern,
                        silent: silent
                    )
                )

            default:
                break
            }

            // '.'? — Consume optional dot after GraphPatternNotTriples
            if isSymbol(".") {
                log("parseGroupGraphPatternSub() consuming optional dot after GraphPatternNotTriples")
                advance()
            }

            // TriplesBlock? — Optional TriplesBlock after GraphPatternNotTriples
            if canStartTriple() {
                try parseTriplesBlock(
                    appendingTo: &pendingBasicGraphPatternElements
                )
                log("parseGroupGraphPatternSub() parsed TriplesBlock after GraphPatternNotTriples")
            }
        }

        log("parseGroupGraphPatternSub() END, accumulated: \(accumulated != nil)")
        try flushPendingBasicGraphPattern()
        var result = try accumulatedOrEmpty()
        for filter in filters {
            try consumeStructuralResource(.totalNodes)
            result = .filter(result, filter)
        }
        return result
    }

    /// SPARQL 1.1 Grammar [56]
    /// TriplesBlock ::= TriplesSameSubjectPath ( '.' TriplesBlock? )?
    private func parseTriplesBlock() throws -> GraphPattern {
        var elements: [BasicGraphPatternElement] = []
        try parseTriplesBlock(appendingTo: &elements)
        return try makeBasicGraphPattern(
            BasicGraphPattern(elements: elements)
        )
    }

    /// Parses one syntactic TriplesBlock directly into its owning BGP buffer.
    /// FILTER-separated blocks share this buffer because FILTER does not form
    /// a basic-graph-pattern boundary.
    private func parseTriplesBlock(
        appendingTo elements: inout [BasicGraphPatternElement]
    ) throws {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }
        log("parseTriplesBlock() START, token: \(currentToken)")

        // Parse first TriplesSameSubjectPath
        try parseTriplesSameSubjectPath(
            appendingTo: &elements
        )

        // ( '.' TriplesBlock? )?  — recursive via loop
        while isSymbol(".") {
            advance() // consume '.'
            guard canStartTriple() else { break }
            try parseTriplesSameSubjectPath(
                appendingTo: &elements
            )
        }

        log("parseTriplesBlock() END, next token: \(currentToken)")
    }

    /// Parse one subject with its predicate-object lists
    /// SPARQL 1.1 Grammar [75] TriplesSameSubjectPath ::= VarOrTerm PropertyListPathNotEmpty
    /// Appends triples and property paths in source order to one BGP.
    private func parseTriplesSameSubjectPath(
        appendingTo elements: inout [BasicGraphPatternElement]
    ) throws {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }
        let initialElementCount = elements.count
        let subject = try parseTerm()
        // Collect pending triples generated by blank node / collection parsing
        appendPendingTriples(to: &elements)
        log("parseTriplesSameSubjectPath() subject: \(subject), next token: \(currentToken)")

        // PropertyListPathNotEmpty [77]:
        //   (VerbPath | VerbSimple) ObjectList ( ';' ( (VerbPath | VerbSimple) ObjectList )? )*
        //
        // Special case: blank node subjects like `[] :p :o` already have triples
        // from parseTerm(), but may also have a property list following.
        // Empty blank nodes `[]` used as subject will have a property list.
        // Blank nodes with inline properties `[ :p :o ]` may have no property list.
        var firstPredicate = true
        while true {
            if !firstPredicate {
                guard isSymbol(";") else { break }
                advance() // consume ';'
                if !canStartVerb() { break }
            }

            // If there's no verb following, this subject has no (more) property list
            if !canStartVerb() { break }
            firstPredicate = false

            let verbOrPath = try parseVerbOrPath()

            // ObjectList [79]: Object ( ',' Object )*
            switch verbOrPath {
            case .term(let predicate):
                let object = try parseTerm()
                elements.append(
                    .triple(
                        try makeTriplePattern(
                            subject: subject,
                            predicate: predicate,
                            object: object
                        )
                    )
                )
                // Collect pending triples from object-position blank nodes / collections
                appendPendingTriples(to: &elements)
                // SPARQL 1.2: Annotation syntax {| annPred annObj |}
                if isSymbol("{|") {
                    let annotationTriples = try parseAnnotation(
                        subject: subject,
                        predicate: predicate,
                        object: object
                    )
                    for triple in annotationTriples {
                        elements.append(.triple(triple))
                    }
                }
                while isSymbol(",") {
                    advance()
                    let nextObj = try parseTerm()
                    elements.append(
                        .triple(
                            try makeTriplePattern(
                                subject: subject,
                                predicate: predicate,
                                object: nextObj
                            )
                        )
                    )
                    appendPendingTriples(to: &elements)
                    // Check for annotation on comma-separated objects too
                    if isSymbol("{|") {
                        let annotationTriples = try parseAnnotation(
                            subject: subject,
                            predicate: predicate,
                            object: nextObj
                        )
                        for triple in annotationTriples {
                            elements.append(.triple(triple))
                        }
                    }
                }
            case .path(let path):
                let object = try parseTerm()
                elements.append(
                    .propertyPath(
                        try makePropertyPathPattern(
                            subject: subject,
                            path: path,
                            object: object
                        )
                    )
                )
                appendPendingTriples(to: &elements)
                while isSymbol(",") {
                    advance()
                    let nextObj = try parseTerm()
                    elements.append(
                        .propertyPath(
                            try makePropertyPathPattern(
                                subject: subject,
                                path: path,
                                object: nextObj
                            )
                        )
                    )
                    appendPendingTriples(to: &elements)
                }
            }
        }

        guard !firstPredicate
                || elements.count > initialElementCount else {
            throw ParseError.invalidSyntax(
                message: "A triple subject must be followed by a predicate and object",
                position: input.distance(from: input.startIndex, to: position)
            )
        }

    }

    private func appendPendingTriples(
        to elements: inout [BasicGraphPatternElement]
    ) {
        for triple in pendingTriples {
            elements.append(.triple(triple))
        }
        pendingTriples.removeAll(keepingCapacity: true)
    }

    private func parseGraphSelector() throws -> SPARQLTerm {
        if case .variable(let name) = currentToken {
            advance()
            return try makeStructuralNode(.variable(name))
        }
        let iri = try parseResolvedIRI(expected: "graph IRI or variable")
        return try makeStructuralNode(.iri(iri))
    }

    private func parseTerm() throws -> SPARQLTerm {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }
        switch currentToken {
        // RDF-star / SPARQL 1.2: << ... >>
        case .symbol("<<"):
            advance() // consume <<
            // SPARQL 1.2 triple term: <<( s p o )>>
            if isSymbol("(") {
                advance() // consume (
                let subject = try parseTerm()
                let predicate = try parseTerm()
                let object = try parseTerm()
                try expectSymbol(")")
                try expectSymbol(">>")
                return try makeStructuralNode(
                    .tripleTerm(
                        subject: subject,
                        predicate: predicate,
                        object: object
                    )
                )
            }
            // RDF-star quoted triple: << s p o >> or reified: << s p o ~?r >>
            let subject = try parseTerm()
            let predicate = try parseTerm()
            let object = try parseTerm()
            // Check for reifier ~
            if isSymbol("~") {
                advance() // consume ~
                let reifier = try parseTerm()
                try expectSymbol(">>")
                return try makeReifiedTripleTerm(
                    subject: subject,
                    predicate: predicate,
                    object: object,
                    reifier: reifier
                )
            }
            try expectSymbol(">>")
            return try makeStructuralNode(
                .tripleTerm(
                    subject: subject,
                    predicate: predicate,
                    object: object
                )
            )

        // Anonymous blank node: [] or [ predicate object ; ... ]
        case .symbol("["):
            advance() // consume '['
            let bnId = freshBlankNode()
            if isSymbol("]") {
                advance() // empty blank node []
                return try makeStructuralNode(.blankNode(bnId))
            }
            // [ predicate-object list ]
            let bnTerm = try makeStructuralNode(SPARQLTerm.blankNode(bnId))
            var firstPred = true
            while true {
                if !firstPred {
                    guard isSymbol(";") else { break }
                    advance()
                    if isSymbol("]") { break }  // trailing semicolon
                }
                firstPred = false
                let predicate = try parseTerm()
                let object = try parseTerm()
                pendingTriples.append(
                    try makeTriplePattern(
                        subject: bnTerm,
                        predicate: predicate,
                        object: object
                    )
                )
                while isSymbol(",") {
                    advance()
                    let nextObj = try parseTerm()
                    pendingTriples.append(
                        try makeTriplePattern(
                            subject: bnTerm,
                            predicate: predicate,
                            object: nextObj
                        )
                    )
                }
            }
            try expectSymbol("]")
            return bnTerm

        // RDF Collection: (term1 term2 ...)
        case .symbol("("):
            advance() // consume '('
            if isSymbol(")") {
                advance()
                return try makeStructuralNode(
                    .iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil")
                )
            }
            // Build rdf:first/rdf:rest chain
            let rdfFirst = try makeStructuralNode(
                SPARQLTerm.iri(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#first"
                )
            )
            let rdfRest = try makeStructuralNode(
                SPARQLTerm.iri(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest"
                )
            )
            let rdfNil = try makeStructuralNode(
                SPARQLTerm.iri(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil"
                )
            )

            let headId = freshBlankNode()
            var currentBnId = headId
            var isFirst = true
            while !isSymbol(")") {
                if !isFirst {
                    let nextBnId = freshBlankNode()
                    let currentBlankNode = try makeStructuralNode(
                        SPARQLTerm.blankNode(currentBnId)
                    )
                    let nextBlankNode = try makeStructuralNode(
                        SPARQLTerm.blankNode(nextBnId)
                    )
                    pendingTriples.append(
                        try makeTriplePattern(
                            subject: currentBlankNode,
                            predicate: rdfRest,
                            object: nextBlankNode
                        )
                    )
                    currentBnId = nextBnId
                }
                isFirst = false
                let element = try parseTerm()
                let currentBlankNode = try makeStructuralNode(
                    SPARQLTerm.blankNode(currentBnId)
                )
                pendingTriples.append(
                    try makeTriplePattern(
                        subject: currentBlankNode,
                        predicate: rdfFirst,
                        object: element
                    )
                )
            }
            // Close the list
            let tailBlankNode = try makeStructuralNode(
                SPARQLTerm.blankNode(currentBnId)
            )
            pendingTriples.append(
                try makeTriplePattern(
                    subject: tailBlankNode,
                    predicate: rdfRest,
                    object: rdfNil
                )
            )
            try expectSymbol(")")
            return try makeStructuralNode(.blankNode(headId))

        case .variable(let name):
            advance()
            return try makeStructuralNode(.variable(name))

        case .iri(let iri):
            advance()
            let resolved = try resolveAbsoluteIRI(iri)
            return try makeStructuralNode(.iri(resolved))

        case .prefixedName(let prefix, let local):
            advance()
            let resolved = try resolveRequiredPrefixedName(
                prefix: prefix,
                local: local
            )
            return try makeStructuralNode(.iri(resolved))

        case .string(let value, let language, let datatype, let direction):
            advance()
            if let lang = language, let dir = direction {
                return try makeLiteralTerm(
                    .dirLangLiteral(
                        value: value,
                        language: lang,
                        direction: dir
                    )
                )
            } else if let lang = language {
                return try makeLiteralTerm(
                    .langLiteral(value: value, language: lang)
                )
            } else if let datatype {
                return try makeLiteralTerm(
                    .typedLiteral(
                        value: value,
                        datatype: try resolveDatatype(datatype)
                    )
                )
            } else {
                return try makeLiteralTerm(.string(value))
            }

        case .integer(let n):
            advance()
            return try makeLiteralTerm(try parseIntegerLiteral(n))

        case .decimal(let n):
            advance()
            return try makeLiteralTerm(try parseDecimalLiteral(n))

        case .double(let n):
            advance()
            return try makeLiteralTerm(try parseDoubleLiteral(n))

        case .blankNode(let id):
            advance()
            return try makeStructuralNode(.blankNode(id))

        case .keyword("TRUE"):
            advance()
            return try makeLiteralTerm(.bool(true))

        case .keyword("FALSE"):
            advance()
            return try makeLiteralTerm(.bool(false))

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
            return try parseIRIFunctionCall(
                iri: resolveAbsoluteIRI(iri)
            )
        }
        if case .prefixedName(let prefix, let local) = currentToken {
            advance()
            let resolved = try resolveRequiredPrefixedName(
                prefix: prefix,
                local: local
            )
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
            return try makeStructuralNode(.bound(Variable(varName)))

        // NotExistsFunc [126]: 'NOT' 'EXISTS' GroupGraphPattern
        case .keyword("NOT"):
            advance()
            if case .keyword("EXISTS") = currentToken {
                advance()
                let pattern = try parseGroupGraphPattern()
                return try makeExistsExpression(
                    pattern: pattern,
                    negated: true
                )
            }
            let constraint = try parseConstraint()
            return try makeStructuralNode(.not(constraint))

        // ExistsFunc [125]: 'EXISTS' GroupGraphPattern
        case .keyword("EXISTS"):
            advance()
            let pattern = try parseGroupGraphPattern()
            return try makeExistsExpression(
                pattern: pattern,
                negated: false
            )

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
                        return try makeStructuralNode(
                            .regex(text, pattern: pattern, flags: f)
                        )
                    }
                    // flags is not a string literal — fall through to .function()
                } else {
                    return try makeStructuralNode(
                        .regex(text, pattern: pattern, flags: nil)
                    )
                }
            }
            // Generic function call for non-literal pattern or non-literal flags
            try admitCollectionElement(amount: 2)
            var args: [Expression] = [text, patternExpr]
            if let f = flagsExpr {
                try admitCollectionElement()
                args.append(f)
            }
            return try makeStructuralNode(
                .function(FunctionCall(name: "REGEX", arguments: args))
            )

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
            try admitCollectionElement(amount: 3)
            return try makeStructuralNode(
                .function(
                    FunctionCall(
                        name: "IF",
                        arguments: [condition, thenExpr, elseExpr]
                    )
                )
            )

        // COALESCE [121]: 'COALESCE' ExpressionList
        case .keyword("COALESCE"):
            advance()
            let args = try parseExpressionList()
            return try makeStructuralNode(.coalesce(args))

        // CONCAT [121]: 'CONCAT' ExpressionList
        case .keyword("CONCAT"):
            advance()
            let args = try parseExpressionList()
            return try makeStructuralNode(
                .function(FunctionCall(name: "CONCAT", arguments: args))
            )

        // 0-arg functions [121]: NOW/RAND/UUID/STRUUID NIL
        case .keyword("NOW"), .keyword("RAND"), .keyword("UUID"), .keyword("STRUUID"):
            guard case .keyword(let name) = currentToken else {
                throw ParseError.invalidSyntax(message: "Expected function name", position: input.distance(from: input.startIndex, to: position))
            }
            advance()
            try expectSymbol("(")
            try expectSymbol(")")
            return try makeStructuralNode(
                .function(FunctionCall(name: name, arguments: []))
            )

        // BNODE [121]: 'BNODE' ( '(' Expression ')' | NIL )
        case .keyword("BNODE"):
            advance()
            try expectSymbol("(")
            if isSymbol(")") {
                advance()
                return try makeStructuralNode(
                    .function(FunctionCall(name: "BNODE", arguments: []))
                )
            }
            let arg = try parseExpression()
            try expectSymbol(")")
            try admitCollectionElement()
            return try makeStructuralNode(
                .function(FunctionCall(name: "BNODE", arguments: [arg]))
            )

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
             .keyword("ISLITERAL"), .keyword("ISNUMERIC"),
             // SPARQL 1.2: language direction functions (1-arg)
             .keyword("LANGDIR"), .keyword("HASLANG"), .keyword("HASLANGDIR"):
            return try parseGenericFunctionCall()

        // SPARQL 1.2: STRLANGDIR(string, lang, dir) — 3-arg function
        case .keyword("STRLANGDIR"):
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
            return try makeStructuralNode(.isTriple(arg))

        case .keyword("TRIPLE"):
            advance()
            try expectSymbol("(")
            let s = try parseExpression()
            try expectSymbol(",")
            let p = try parseExpression()
            try expectSymbol(",")
            let o = try parseExpression()
            try expectSymbol(")")
            return try makeStructuralNode(
                .triple(subject: s, predicate: p, object: o)
            )

        case .keyword("SUBJECT"):
            advance()
            try expectSymbol("(")
            let arg = try parseExpression()
            try expectSymbol(")")
            return try makeStructuralNode(.subject(arg))

        case .keyword("PREDICATE"):
            advance()
            try expectSymbol("(")
            let arg = try parseExpression()
            try expectSymbol(")")
            return try makeStructuralNode(.predicate(arg))

        case .keyword("OBJECT"):
            advance()
            try expectSymbol("(")
            let arg = try parseExpression()
            try expectSymbol(")")
            return try makeStructuralNode(.object(arg))

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
            try admitCollectionElement()
            args.append(try parseExpression())
            while isSymbol(",") {
                advance()
                try admitCollectionElement()
                args.append(try parseExpression())
            }
        }
        try expectSymbol(")")
        return try makeStructuralNode(
            .function(FunctionCall(name: name, arguments: args))
        )
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
            return try makeAggregateExpression(
                .count(arg, distinct: distinct)
            )
        case "SUM":
            let operand = try arg ?? makeLiteralExpression(.null)
            return try makeAggregateExpression(
                .sum(operand, distinct: distinct)
            )
        case "AVG":
            let operand = try arg ?? makeLiteralExpression(.null)
            return try makeAggregateExpression(
                .avg(operand, distinct: distinct)
            )
        case "MIN":
            let operand = try arg ?? makeLiteralExpression(.null)
            return try makeAggregateExpression(.min(operand))
        case "MAX":
            let operand = try arg ?? makeLiteralExpression(.null)
            return try makeAggregateExpression(.max(operand))
        case "SAMPLE":
            let operand = try arg ?? makeLiteralExpression(.null)
            return try makeAggregateExpression(.sample(operand))
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
            guard case .string(let sep, _, _, _) = currentToken else {
                throw ParseError.invalidSyntax(
                    message: "Expected separator string",
                    position: input.distance(from: input.startIndex, to: position)
                )
            }
            separator = sep
            advance()
        }

        try expectSymbol(")")
        return try makeAggregateExpression(
            .groupConcat(
                expr,
                separator: separator,
                distinct: distinct
            )
        )
    }

    /// [120] ExpressionList ::= NIL | '(' Expression ( ',' Expression )* ')'
    private func parseExpressionList() throws -> [Expression] {
        try expectSymbol("(")
        if isSymbol(")") {
            advance()
            return []
        }
        var args: [Expression] = []
        try admitCollectionElement()
        args.append(try parseExpression())
        while isSymbol(",") {
            advance()
            try admitCollectionElement()
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
            try admitCollectionElement()
            args.append(try parseExpression())
            while isSymbol(",") {
                advance()
                try admitCollectionElement()
                args.append(try parseExpression())
            }
        }
        try expectSymbol(")")
        return try makeStructuralNode(
            .function(
                FunctionCall(
                    name: iri,
                    arguments: args,
                    distinct: distinct
                )
            )
        )
    }

    private func parseExpression() throws -> Expression {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }
        return try parseOrExpression()
    }

    private func parseOrExpression() throws -> Expression {
        var left = try parseAndExpression()
        while case .symbol("||") = currentToken {
            advance()
            let right = try parseAndExpression()
            left = try makeStructuralNode(.or(left, right))
        }
        return left
    }

    private func parseAndExpression() throws -> Expression {
        var left = try parseRelationalExpression()
        while case .symbol("&&") = currentToken {
            advance()
            let right = try parseRelationalExpression()
            left = try makeStructuralNode(.and(left, right))
        }
        return left
    }

    private func parseRelationalExpression() throws -> Expression {
        let left = try parseAdditiveExpression()

        switch currentToken {
        case .symbol("="):
            advance()
            let right = try parseAdditiveExpression()
            return try makeStructuralNode(.equal(left, right))
        case .symbol("!="):
            advance()
            let right = try parseAdditiveExpression()
            return try makeStructuralNode(.notEqual(left, right))
        case .symbol("<"):
            advance()
            let right = try parseAdditiveExpression()
            return try makeStructuralNode(.lessThan(left, right))
        case .symbol(">"):
            advance()
            let right = try parseAdditiveExpression()
            return try makeStructuralNode(.greaterThan(left, right))
        case .symbol("<="):
            advance()
            let right = try parseAdditiveExpression()
            return try makeStructuralNode(.lessThanOrEqual(left, right))
        case .symbol(">="):
            advance()
            let right = try parseAdditiveExpression()
            return try makeStructuralNode(.greaterThanOrEqual(left, right))
        case .keyword("IN"):
            advance()
            try expectSymbol("(")
            var values: [Expression] = []
            if !isSymbol(")") {
                var first = true
                while first || isSymbol(",") {
                    if !first { advance() }
                    first = false
                    try admitCollectionElement()
                    values.append(try parseExpression())
                }
            }
            try expectSymbol(")")
            return try makeStructuralNode(.inList(left, values: values))
        case .keyword("NOT"):
            let savedPos = position
            let savedTok = currentToken
            advance()
            if case .keyword("IN") = currentToken {
                advance()
                try expectSymbol("(")
                var values: [Expression] = []
                if !isSymbol(")") {
                    var first = true
                    while first || isSymbol(",") {
                        if !first { advance() }
                        first = false
                        try admitCollectionElement()
                        values.append(try parseExpression())
                    }
                }
                try expectSymbol(")")
                return try makeStructuralNode(
                    .notInList(left, values: values)
                )
            } else {
                position = savedPos
                currentToken = savedTok
                return left
            }
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
                left = try makeStructuralNode(.add(left, right))
            } else {
                left = try makeStructuralNode(.subtract(left, right))
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
                left = try makeStructuralNode(.multiply(left, right))
            } else {
                left = try makeStructuralNode(.divide(left, right))
            }
        }
        return left
    }

    private func parseUnaryExpression() throws -> Expression {
        var operators: [String] = []
        var enteredNestingCount = 0
        defer {
            for _ in 0..<enteredNestingCount {
                leaveStructuralNesting()
            }
        }
        while case .symbol(let symbol) = currentToken,
              symbol == "!" || symbol == "-" || symbol == "+" {
            try enterStructuralNesting()
            enteredNestingCount += 1
            try admitCollectionElement()
            operators.append(symbol)
            advance()
        }

        var expression = try parsePrimaryExpression()
        for symbol in operators.reversed() {
            switch symbol {
            case "!":
                expression = try makeStructuralNode(.not(expression))
            case "-":
                expression = try makeStructuralNode(.negate(expression))
            case "+":
                break
            default:
                preconditionFailure("Unexpected unary operator")
            }
        }
        return expression
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
            return try makeStructuralNode(.variable(Variable(name)))

        // iriOrFunction [128]: iri ArgList?
        case .iri(let iri):
            advance()
            let resolved = try resolveAbsoluteIRI(iri)
            if isSymbol("(") {
                return try parseIRIFunctionCall(iri: resolved)
            }
            return try makeLiteralExpression(.iri(resolved))

        // iriOrFunction [128]: prefixedName ArgList?
        case .prefixedName(let prefix, let local):
            advance()
            let resolved = try resolveRequiredPrefixedName(
                prefix: prefix,
                local: local
            )
            if isSymbol("(") {
                return try parseIRIFunctionCall(iri: resolved)
            }
            return try makeLiteralExpression(.iri(resolved))

        // RDFLiteral
        case .string(let value, let language, let datatype, let direction):
            advance()
            if let lang = language, let dir = direction {
                return try makeLiteralExpression(
                    .dirLangLiteral(
                        value: value,
                        language: lang,
                        direction: dir
                    )
                )
            } else if let lang = language {
                return try makeLiteralExpression(
                    .langLiteral(value: value, language: lang)
                )
            } else if let datatype {
                return try makeLiteralExpression(
                    .typedLiteral(
                        value: value,
                        datatype: try resolveDatatype(datatype)
                    )
                )
            } else {
                return try makeLiteralExpression(.string(value))
            }

        // NumericLiteral
        case .integer(let n):
            advance()
            return try makeLiteralExpression(try parseIntegerLiteral(n))

        case .decimal(let n):
            advance()
            return try makeLiteralExpression(try parseDecimalLiteral(n))

        case .double(let n):
            advance()
            return try makeLiteralExpression(try parseDoubleLiteral(n))

        // BooleanLiteral
        case .keyword("TRUE"):
            advance()
            return try makeLiteralExpression(.bool(true))

        case .keyword("FALSE"):
            advance()
            return try makeLiteralExpression(.bool(false))

        // BuiltInCall [121]
        case .keyword(let kw) where Self.builtInFunctionKeywords.contains(kw):
            return try parseBuiltInCall()

        // RDF-star / SPARQL 1.2: << ... >> as expression
        case .symbol("<<"):
            advance()
            // SPARQL 1.2 triple term: <<( s p o )>>
            if isSymbol("(") {
                advance()
                let subject = try parseTerm()
                let predicate = try parseTerm()
                let object = try parseTerm()
                try expectSymbol(")")
                try expectSymbol(">>")
                let tripleTerm = try makeStructuralNode(
                    SPARQLTerm.tripleTerm(
                        subject: subject,
                        predicate: predicate,
                        object: object
                    )
                )
                return try makeExpression(from: tripleTerm)
            }
            // RDF-star quoted triple or reified triple
            let subject = try parseTerm()
            let predicate = try parseTerm()
            let object = try parseTerm()
            if isSymbol("~") {
                advance()
                let reifier = try parseTerm()
                try expectSymbol(">>")
                let reifiedTerm = try makeReifiedTripleTerm(
                    subject: subject,
                    predicate: predicate,
                    object: object,
                    reifier: reifier
                )
                return try makeExpression(from: reifiedTerm)
            }
            try expectSymbol(">>")
            let tripleTerm = try makeStructuralNode(
                SPARQLTerm.tripleTerm(
                    subject: subject,
                    predicate: predicate,
                    object: object
                )
            )
            return try makeExpression(from: tripleTerm)

        default:
            throw ParseError.invalidSyntax(
                message: "Expected expression",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    private func parseIntegerLiteral(_ lexicalForm: String) throws -> Literal {
        guard let literal = Literal.parseInteger(lexicalForm) else {
            throw invalidNumericLiteral(lexicalForm)
        }
        return literal
    }

    private func parseDecimalLiteral(_ lexicalForm: String) throws -> Literal {
        guard let literal = Literal.parseDecimal(lexicalForm) else {
            throw invalidNumericLiteral(lexicalForm)
        }
        return literal
    }

    private func parseDoubleLiteral(_ lexicalForm: String) throws -> Literal {
        guard let value = Double(lexicalForm), value.isFinite else {
            throw invalidNumericLiteral(lexicalForm)
        }
        return .double(value)
    }

    private func invalidNumericLiteral(_ lexicalForm: String) -> ParseError {
        .invalidSyntax(
            message: "Invalid or out-of-range numeric literal: \(lexicalForm)",
            position: input.distance(from: input.startIndex, to: position)
        )
    }

    private func parseGroupCondition() throws -> ParsedGroupConditions {
        var result = ParsedGroupConditions()

        while isVariable() || isSymbol("(") || isBuiltInFunctionKeyword() || isIRIOrPrefixedName() {
            switch currentToken {
            case .variable(let name):
                try admitCollectionElement()
                result.expressions.append(
                    try makeStructuralNode(.variable(Variable(name)))
                )
                advance()
            case .symbol("("):
                advance()
                let expr = try parseExpression()
                if isKeyword("AS") {
                    advance()
                    guard case .variable(let alias) = currentToken else {
                        throw ParseError.invalidSyntax(
                            message: "GROUP BY AS requires a variable",
                            position: input.distance(
                                from: input.startIndex,
                                to: position
                            )
                        )
                    }
                    advance()
                    try admitCollectionElement(amount: 2)
                    result.bindings.append(
                        try makeStructuralNode(
                            GroupBinding(variable: alias, expression: expr)
                        )
                    )
                    result.expressions.append(
                        try makeStructuralNode(.variable(Variable(alias)))
                    )
                } else {
                    try admitCollectionElement()
                    result.expressions.append(expr)
                }
                try expectSymbol(")")
            default:
                // Bare built-in function call or IRI function call
                let expr = try parseConstraint()
                try admitCollectionElement()
                result.expressions.append(expr)
            }
        }

        return result
    }

    private func parseOrderCondition() throws -> [SortKey] {
        var keys: [SortKey] = []

        while isVariable() || isKeyword("ASC") || isKeyword("DESC") || isSymbol("(")
              || isBuiltInFunctionKeyword() || isIRIOrPrefixedName() {
            var direction: SortDirection = .ascending
            var consumedDirection = false
            if isKeyword("ASC") {
                consumedDirection = true
                advance()
            } else if isKeyword("DESC") {
                consumedDirection = true
                direction = .descending
                advance()
            }

            let expr: Expression
            if isSymbol("(") {
                advance()
                expr = try parseExpression()
                try expectSymbol(")")
            } else if case .variable(let name) = currentToken {
                expr = try makeStructuralNode(.variable(Variable(name)))
                advance()
            } else if isBuiltInFunctionKeyword() || isIRIOrPrefixedName() {
                // Bare function call: STRLEN(?name), <iri>(?x), prefix:func(?x)
                expr = try parseConstraint()
            } else {
                if consumedDirection {
                    throw ParseError.invalidSyntax(
                        message: "ORDER BY direction requires an expression",
                        position: input.distance(from: input.startIndex, to: position)
                    )
                }
                break
            }

            try admitCollectionElement()
            keys.append(
                try makeStructuralNode(
                    SortKey(expr, direction: direction)
                )
            )
        }

        return keys
    }

    private func parseInlineData() throws -> GraphPattern {
        try expect("VALUES")

        if case .variable(let name) = currentToken {
            try consumeValuesResource(1, resource: .valuesVariables)
            try admitCollectionElement()
            advance()
            try expectSymbol("{")
            var bindings: [[Literal?]] = []
            while !isSymbol("}") {
                if case .eof = currentToken {
                    throw ParseError.unexpectedEndOfInput(expected: "}")
                }
                try consumeValuesResource(1, resource: .valuesRows)
                try consumeValuesResource(1, resource: .valuesCells)
                try admitCollectionElement(amount: 2)
                bindings.append([try parseDataBlockValue()])
            }
            try expectSymbol("}")
            return try makeStructuralNode(
                .values(variables: [name], bindings: bindings)
            )
        }

        var variables: [String] = []
        if case .symbol("(") = currentToken {
            advance()
            while case .variable(let name) = currentToken {
                try consumeValuesResource(1, resource: .valuesVariables)
                try admitCollectionElement()
                variables.append(name)
                advance()
            }
            try expectSymbol(")")
        } else {
            throw ParseError.unexpectedToken(
                expected: "a variable or parenthesized variable list",
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }

        try expectSymbol("{")
        var bindings: [[Literal?]] = []
        while !isSymbol("}") {
            if case .eof = currentToken {
                throw ParseError.unexpectedEndOfInput(expected: "}")
            }
            try consumeValuesResource(1, resource: .valuesRows)
            try consumeValuesResource(
                UInt64(variables.count),
                resource: .valuesCells
            )
            try admitCollectionElement(
                amount: UInt64(variables.count) + 1
            )
            try expectSymbol("(")
            var row: [Literal?] = []
            row.reserveCapacity(variables.count)
            for _ in variables {
                row.append(try parseDataBlockValue())
            }
            try expectSymbol(")")
            bindings.append(row)
        }
        try expectSymbol("}")

        return try makeStructuralNode(
            .values(variables: variables, bindings: bindings)
        )
    }

    private func parseDataBlockValue() throws -> Literal? {
        if case .keyword("UNDEF") = currentToken {
            advance()
            return nil
        }

        let term = try parseTerm()
        switch term {
        case .iri(let iri):
            return try makeStructuralNode(.iri(iri))
        case .literal(let literal):
            return literal
        case .variable, .blankNode, .tripleTerm,
             .reifiedTriple:
            throw ParseError.invalidSyntax(
                message: "VALUES accepts only IRIs, literals, and UNDEF",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    private func parseConstructQuery() throws -> ConstructQuery {
        log("parseConstructQuery() START")
        try expect("CONSTRUCT")

        let template: [TriplePattern]
        var pattern: GraphPattern
        let dataset: SPARQLDataset

        if isSymbol("{") {
            template = try parseConstructTemplate()
            dataset = try parseDatasetClauses()
            if isKeyword("WHERE") {
                advance()
            }
            pattern = try parseGroupGraphPattern()
        } else {
            dataset = try parseDatasetClauses()
            try expect("WHERE")
            template = try parseConstructTemplate()
            pattern = try makeBasicGraphPattern(template)
        }

        let parsedModifiers = try parseSolutionModifiers()
        pattern = try applying(parsedModifiers.groupBindings, to: pattern)

        return try makeStructuralNode(
            ConstructQuery(
                template: template,
                pattern: pattern,
                dataset: dataset,
                modifiers: parsedModifiers.value
            )
        )
    }

    private func parseConstructTemplate() throws -> [TriplePattern] {
        try expectSymbol("{")
        var template: [TriplePattern] = []

        while !isSymbol("}") {
            if case .eof = currentToken {
                throw ParseError.unexpectedEndOfInput(expected: "}")
            }
            guard canStartTriple() else {
                throw ParseError.unexpectedToken(
                    expected: "CONSTRUCT triple template or }",
                    found: tokenDescription(currentToken),
                    position: input.distance(from: input.startIndex, to: position)
                )
            }
            let triples = try requireTriplePatterns(
                from: parseTriplesBlock(),
                errorMessage: "CONSTRUCT templates cannot contain property paths or graph operators"
            )
            template.append(contentsOf: triples)
        }

        try expectSymbol("}")
        return template
    }

    private func parseAskQuery() throws -> AskQuery {
        try expect("ASK")
        let dataset = try parseDatasetClauses()
        if isKeyword("WHERE") {
            advance()
        }
        let ungroupedPattern = try parseGroupGraphPattern()
        let parsedModifiers = try parseSolutionModifiers()
        let pattern = try applying(
            parsedModifiers.groupBindings,
            to: ungroupedPattern
        )
        return try makeStructuralNode(
            AskQuery(
                pattern: pattern,
                dataset: dataset,
                modifiers: parsedModifiers.value
            )
        )
    }

    private func parseDescribeQuery() throws -> DescribeQuery {
        log("parseDescribeQuery() START")
        try expect("DESCRIBE")

        let selection: DescribeSelection
        if case .symbol("*") = currentToken {
            log("parseDescribeQuery() found '*'")
            advance()
            selection = .all
        } else {
            var firstResource: SPARQLTerm?
            var additionalResources: [SPARQLTerm] = []
            while isVariable() || isIRIOrPrefixedName() {
                let resource: SPARQLTerm
                switch currentToken {
                case .variable(let name):
                    resource = try makeStructuralNode(.variable(name))
                    advance()
                case .iri, .prefixedName:
                    let iri = try parseResolvedIRI(
                        expected: "DESCRIBE resource"
                    )
                    resource = try makeStructuralNode(.iri(iri))
                default:
                    preconditionFailure("DESCRIBE resource lookahead mismatch")
                }
                if firstResource == nil {
                    firstResource = resource
                } else {
                    try admitCollectionElement()
                    additionalResources.append(resource)
                }
            }
            guard let firstResource else {
                throw ParseError.invalidSyntax(
                    message: "DESCRIBE requires '*' or at least one resource",
                    position: input.distance(
                        from: input.startIndex,
                        to: position
                    )
                )
            }
            selection = .resources(
                first: firstResource,
                additional: consume additionalResources
            )
        }

        let dataset = try parseDatasetClauses()

        var pattern: GraphPattern?
        if isKeyword("WHERE") || isSymbol("{") {
            if isKeyword("WHERE") {
                advance()
            }
            pattern = try parseGroupGraphPattern()
        }

        let parsedModifiers = try parseSolutionModifiers()
        if !parsedModifiers.groupBindings.isEmpty {
            let basePattern: GraphPattern
            if let pattern {
                basePattern = pattern
            } else {
                basePattern = try makeBasicGraphPattern([])
            }
            pattern = try applying(
                parsedModifiers.groupBindings,
                to: basePattern
            )
        }

        return try makeStructuralNode(
            DescribeQuery(
                selection: selection,
                pattern: pattern,
                dataset: dataset,
                modifiers: parsedModifiers.value
            )
        )
    }

    // MARK: - SPARQL Update Parsing (B12)

    private var isSPARQLUpdateStart: Bool {
        switch currentToken {
        case .keyword("INSERT"), .keyword("DELETE"), .keyword("WITH"),
             .keyword("LOAD"), .keyword("CLEAR"), .keyword("CREATE"),
             .keyword("DROP"), .keyword("ADD"), .keyword("COPY"),
             .keyword("MOVE"):
            return true
        default:
            return false
        }
    }

    /// Parses a semicolon-delimited, ordered SPARQL Update request.
    /// A final semicolon is accepted, but empty operations between separators
    /// and query forms inside an update request are rejected.
    private func parseSPARQLUpdateRequest() throws -> SPARQLUpdateRequest {
        let firstOperation = try parseSPARQLUpdateOperation()
        var additionalOperations: [SPARQLUpdateOperation] = []

        while isSymbol(";") {
            advance()
            if case .eof = currentToken {
                break
            }

            try parsePrologue()
            guard isSPARQLUpdateStart else {
                throw ParseError.unexpectedToken(
                    expected: "SPARQL Update operation after ';'",
                    found: tokenDescription(currentToken),
                    position: input.distance(
                        from: input.startIndex,
                        to: position
                    )
                )
            }
            try admitCollectionElement()
            additionalOperations.append(try parseSPARQLUpdateOperation())
        }

        return SPARQLUpdateRequest(
            firstOperation: firstOperation,
            additionalOperations: consume additionalOperations
        )
    }

    private func parseSPARQLUpdateOperation() throws -> SPARQLUpdateOperation {
        let operation: SPARQLUpdateOperation
        switch currentToken {
        case .keyword("INSERT"):
            operation = try parseInsertOrModify()
        case .keyword("DELETE"):
            operation = try parseDeleteOrModify()
        case .keyword("WITH"):
            operation = try parseWithModify()
        case .keyword("LOAD"):
            operation = .load(try parseLoadQuery())
        case .keyword("CLEAR"):
            operation = .clear(try parseClearQuery())
        case .keyword("CREATE"):
            operation = .createGraph(try parseCreateGraph())
        case .keyword("DROP"):
            operation = .drop(try parseDropGraph())
        case .keyword("ADD"), .keyword("COPY"), .keyword("MOVE"):
            operation = .graphTransfer(try parseGraphTransfer())
        default:
            throw ParseError.unexpectedToken(
                expected: "SPARQL Update operation",
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        return try makeStructuralNode(operation)
    }

    /// Parses WITH <iri> followed by a DELETE/INSERT Modify operation.
    private func parseWithModify() throws -> SPARQLUpdateOperation {
        try expect("WITH")
        let graph = try parseResolvedIRI(expected: "graph IRI after WITH")
        if isKeyword("DELETE") {
            return try parseDeleteOrModify(withGraph: graph)
        }
        if isKeyword("INSERT") {
            return try parseInsertOrModify(withGraph: graph)
        }
        throw ParseError.unexpectedToken(
            expected: "DELETE or INSERT after WITH graph IRI",
            found: tokenDescription(currentToken),
            position: input.distance(from: input.startIndex, to: position)
        )
    }

    /// Parse INSERT DATA or DELETE/INSERT WHERE.
    private func parseInsertOrModify(
        withGraph: String? = nil
    ) throws -> SPARQLUpdateOperation {
        try expect("INSERT")
        if isKeyword("DATA") {
            guard withGraph == nil else {
                throw ParseError.invalidSyntax(
                    message: "WITH cannot be applied to INSERT DATA",
                    position: input.distance(from: input.startIndex, to: position)
                )
            }
            advance()
            return .insertData(try parseInsertDataQuery())
        }
        // INSERT { ... } WHERE { ... }
        return try parseSPARQLModifyOperation(
            withGraph: withGraph,
            deletePattern: nil
        )
    }

    /// Parse DELETE DATA, DELETE WHERE, or DELETE/INSERT WHERE.
    private func parseDeleteOrModify(
        withGraph: String? = nil
    ) throws -> SPARQLUpdateOperation {
        try expect("DELETE")
        if isKeyword("DATA") {
            guard withGraph == nil else {
                throw ParseError.invalidSyntax(
                    message: "WITH cannot be applied to DELETE DATA",
                    position: input.distance(from: input.startIndex, to: position)
                )
            }
            advance()
            return .deleteData(try parseDeleteDataQuery())
        }
        if isKeyword("WHERE") {
            guard withGraph == nil else {
                throw ParseError.invalidSyntax(
                    message: "WITH cannot be applied to DELETE WHERE",
                    position: input.distance(from: input.startIndex, to: position)
                )
            }
            advance()
            return .deleteWhere(try parseDeleteWhereQuery())
        }
        // DELETE { ... } [INSERT { ... }] WHERE { ... }
        let deleteQuads = try parseQuadBlock(allowsGraphVariables: true)
        try validateUpdateQuads(
            deleteQuads,
            context: "DELETE template",
            allowsVariables: true,
            allowsBlankNodes: false
        )
        var insertQuads: [Quad]?
        if isKeyword("INSERT") {
            advance()
            insertQuads = try parseQuadBlock(allowsGraphVariables: true)
        }
        return try parseSPARQLModifyOperation(
            withGraph: withGraph,
            deletePattern: deleteQuads,
            insertPattern: insertQuads
        )
    }

    /// Parse INSERT DATA { quads }
    private func parseInsertDataQuery() throws -> InsertDataQuery {
        let quads = try parseQuadBlock(allowsGraphVariables: false)
        try validateUpdateQuads(
            quads,
            context: "INSERT DATA",
            allowsVariables: false,
            allowsBlankNodes: true
        )
        return InsertDataQuery(quads: quads)
    }

    /// Parse DELETE DATA { quads }
    private func parseDeleteDataQuery() throws -> DeleteDataQuery {
        let quads = try parseQuadBlock(allowsGraphVariables: false)
        try validateUpdateQuads(
            quads,
            context: "DELETE DATA",
            allowsVariables: false,
            allowsBlankNodes: false
        )
        return DeleteDataQuery(quads: quads)
    }

    /// Parses DELETE WHERE from one canonical QuadPattern value.
    private func parseDeleteWhereQuery() throws -> DeleteWhereQuery {
        let pattern = try parseQuadBlock(allowsGraphVariables: true)
        try validateUpdateQuads(
            pattern,
            context: "DELETE WHERE",
            allowsVariables: true,
            allowsBlankNodes: false
        )
        return DeleteWhereQuery(pattern: consume pattern)
    }

    /// Parse DELETE { } INSERT { } WHERE { }
    private func parseSPARQLModifyOperation(
        withGraph: String? = nil,
        deletePattern: [Quad]? = nil,
        insertPattern: [Quad]? = nil
    ) throws -> SPARQLUpdateOperation {
        let delPat = deletePattern
        var insPat = insertPattern

        // If we haven't parsed delete/insert blocks yet (INSERT-only form)
        if delPat == nil && insPat == nil {
            insPat = try parseQuadBlock(allowsGraphVariables: true)
        }

        if let insPat {
            try validateUpdateQuads(
                insPat,
                context: "INSERT template",
                allowsVariables: true,
                allowsBlankNodes: true
            )
        }

        // Parse USING clauses
        var using: [GraphRef] = []
        while isKeyword("USING") {
            advance()
            if isKeyword("NAMED") {
                advance()
                let iri = try parseResolvedIRI(expected: "IRI after USING NAMED")
                try admitCollectionElement()
                using.append(GraphRef(iri: iri, isNamed: true))
            } else {
                let iri = try parseResolvedIRI(expected: "IRI after USING")
                try admitCollectionElement()
                using.append(GraphRef(iri: iri, isNamed: false))
            }
        }

        // Parse WHERE
        try expect("WHERE")
        let wherePattern = try parseGroupGraphPattern()

        let action: SPARQLModifyAction
        switch (delPat, insPat) {
        case (.some(let deleteQuads), .some(let insertQuads)):
            action = .deleteAndInsert(
                delete: deleteQuads,
                insert: insertQuads
            )
        case (.some(let deleteQuads), .none):
            action = .delete(deleteQuads)
        case (.none, .some(let insertQuads)):
            action = .insert(insertQuads)
        case (.none, .none):
            throw ParseError.invalidSyntax(
                message: "A Modify operation requires DELETE or INSERT",
                position: input.distance(from: input.startIndex, to: position)
            )
        }

        return .modify(
            SPARQLModifyOperation(
                withGraph: withGraph,
                action: action,
                using: consume using,
                wherePattern: wherePattern
            )
        )
    }

    /// Parses QuadData or QuadPattern according to its graph-name grammar.
    private func parseQuadBlock(
        allowsGraphVariables: Bool
    ) throws -> [Quad] {
        try expectSymbol("{")
        var quads: [Quad] = []

        while !isSymbol("}") {
            if case .eof = currentToken {
                throw ParseError.unexpectedEndOfInput(expected: "}")
            }

            if isKeyword("GRAPH") {
                advance()
                let graphTerm: SPARQLTerm
                if allowsGraphVariables,
                   case .variable(let name) = currentToken {
                    graphTerm = try makeStructuralNode(.variable(name))
                    advance()
                } else {
                    let graphIRI = try parseResolvedIRI(
                        expected: allowsGraphVariables
                            ? "graph IRI or variable after GRAPH"
                            : "graph IRI after GRAPH"
                    )
                    graphTerm = try makeStructuralNode(.iri(graphIRI))
                }
                try expectSymbol("{")
                while !isSymbol("}") {
                    if case .eof = currentToken {
                        throw ParseError.unexpectedEndOfInput(expected: "}")
                    }
                    let triples = try requireTriplePatterns(
                        from: parseTriplesBlock(),
                        errorMessage: "SPARQL update quad blocks cannot contain property paths"
                    )
                    for triple in triples {
                        quads.append(
                            try makeQuad(graph: graphTerm, triple: triple)
                        )
                    }
                    if isSymbol(".") { advance() }
                }
                try expectSymbol("}")
            } else {
                let triples = try requireTriplePatterns(
                    from: parseTriplesBlock(),
                    errorMessage: "SPARQL update quad blocks cannot contain property paths"
                )
                for triple in triples {
                    quads.append(try makeQuad(graph: nil, triple: triple))
                }
            }
            if isSymbol(".") { advance() }
        }
        try expectSymbol("}")
        return quads
    }

    private func validateUpdateQuads(
        _ quads: [Quad],
        context: String,
        allowsVariables: Bool,
        allowsBlankNodes: Bool
    ) throws {
        for quad in quads {
            let terms = [
                quad.graph,
                quad.triple.subject,
                quad.triple.predicate,
                quad.triple.object,
            ]
            for term in terms.compactMap({ $0 }) {
                if !allowsVariables && containsVariable(term) {
                    throw ParseError.invalidSyntax(
                        message: "\(context) must be ground and cannot contain variables",
                        position: input.distance(from: input.startIndex, to: position)
                    )
                }
                if !allowsBlankNodes && containsBlankNode(term) {
                    throw ParseError.invalidSyntax(
                        message: "\(context) cannot contain blank nodes",
                        position: input.distance(from: input.startIndex, to: position)
                    )
                }
            }
        }
    }

    private func containsVariable(_ term: SPARQLTerm) -> Bool {
        switch term {
        case .variable:
            return true
        case .tripleTerm(let subject, let predicate, let object):
            return containsVariable(subject)
                || containsVariable(predicate)
                || containsVariable(object)
        case .reifiedTriple(let subject, let predicate, let object, let reifier):
            return containsVariable(subject)
                || containsVariable(predicate)
                || containsVariable(object)
                || containsVariable(reifier)
        case .iri, .literal, .blankNode:
            return false
        }
    }

    private func containsBlankNode(_ term: SPARQLTerm) -> Bool {
        switch term {
        case .blankNode:
            return true
        case .literal(let literal):
            return containsBlankNode(literal)
        case .tripleTerm(let subject, let predicate, let object):
            return containsBlankNode(subject)
                || containsBlankNode(predicate)
                || containsBlankNode(object)
        case .reifiedTriple(let subject, let predicate, let object, let reifier):
            return containsBlankNode(subject)
                || containsBlankNode(predicate)
                || containsBlankNode(object)
                || containsBlankNode(reifier)
        case .variable, .iri:
            return false
        }
    }

    private func containsBlankNode(_ literal: Literal) -> Bool {
        switch literal {
        case .blankNode:
            return true
        case .array(let elements):
            return elements.contains(where: containsBlankNode(_:))
        case .rdfTerm(let term):
            return containsBlankNode(term)
        default:
            return false
        }
    }

    private func containsBlankNode(_ term: RDFTerm) -> Bool {
        switch term {
        case .blankNode:
            return true
        case .tripleTerm(let subject, _, let object):
            return containsBlankNode(subject)
                || containsBlankNode(object)
        case .iri, .literal:
            return false
        }
    }

    private func containsBlankNode(_ subject: RDFSubject) -> Bool {
        switch subject {
        case .blankNode:
            return true
        case .iri:
            return false
        }
    }

    /// Parse LOAD [SILENT] <iri> [INTO GRAPH <iri>]
    private func parseLoadQuery() throws -> LoadQuery {
        try expect("LOAD")
        let silent = parseSilent()
        let source = try parseResolvedIRI(expected: "source IRI after LOAD")

        var destination: String?
        if isKeyword("INTO") {
            advance()
            try expect("GRAPH")
            destination = try parseResolvedIRI(
                expected: "destination IRI after INTO GRAPH"
            )
        }

        return LoadQuery(source: source, destination: destination, silent: silent)
    }

    /// Parse CLEAR [SILENT] (GRAPH <iri> | DEFAULT | NAMED | ALL)
    private func parseClearQuery() throws -> ClearQuery {
        try expect("CLEAR")
        let silent = parseSilent()
        return ClearQuery(
            target: try parseSPARQLGraphTarget(operation: "CLEAR"),
            silent: silent
        )
    }

    private func parseSPARQLGraphTarget(
        operation: String
    ) throws -> SPARQLGraphTarget {
        switch currentToken {
        case .keyword("GRAPH"):
            advance()
            return .graph(
                try parseResolvedIRI(
                    expected: "graph IRI after \(operation) GRAPH"
                )
            )
        case .keyword("DEFAULT"):
            advance()
            return .default
        case .keyword("NAMED"):
            advance()
            return .named
        case .keyword("ALL"):
            advance()
            return .all
        default:
            throw ParseError.unexpectedToken(
                expected: "GRAPH, DEFAULT, NAMED, or ALL after \(operation)",
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    /// Parse CREATE [SILENT] GRAPH <iri>
    private func parseCreateGraph() throws -> CreateSPARQLGraphQuery {
        try expect("CREATE")
        let silent = parseSilent()
        try expect("GRAPH")
        let iri = try parseResolvedIRI(expected: "graph IRI after CREATE GRAPH")
        return CreateSPARQLGraphQuery(graph: iri, silent: silent)
    }

    /// Parse DROP [SILENT] (GRAPH <iri> | DEFAULT | NAMED | ALL).
    private func parseDropGraph() throws -> DropQuery {
        try expect("DROP")
        let silent = parseSilent()
        return DropQuery(
            target: try parseSPARQLGraphTarget(operation: "DROP"),
            silent: silent
        )
    }

    /// Parse ADD/COPY/MOVE [SILENT] source TO destination.
    private func parseGraphTransfer() throws -> GraphTransferQuery {
        let operation: SPARQLGraphTransferOperation
        switch currentToken {
        case .keyword("ADD"):
            operation = .add
        case .keyword("COPY"):
            operation = .copy
        case .keyword("MOVE"):
            operation = .move
        default:
            throw ParseError.unexpectedToken(
                expected: "ADD, COPY, or MOVE",
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        advance()
        let silent = parseSilent()
        let source = try parseGraphTransferEndpoint(role: "source")
        try expect("TO")
        let destination = try parseGraphTransferEndpoint(role: "destination")
        return GraphTransferQuery(
            operation: operation,
            source: source,
            destination: destination,
            silent: silent
        )
    }

    private func parseGraphTransferEndpoint(
        role: String
    ) throws -> SPARQLGraphTransferEndpoint {
        if isKeyword("DEFAULT") {
            advance()
            return .default
        }
        if isKeyword("GRAPH") {
            advance()
        }
        guard isIRIOrPrefixedName() else {
            throw ParseError.unexpectedToken(
                expected: "DEFAULT or optional GRAPH followed by an IRI as transfer \(role)",
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        return .graph(
            try parseResolvedIRI(
                expected: "graph IRI for transfer \(role)"
            )
        )
    }

    /// Parse optional SILENT keyword, returns true if present
    private func parseSilent() -> Bool {
        if isKeyword("SILENT") {
            advance()
            return true
        }
        return false
    }
}
