/// SQLEscape.swift
/// SQL and SPARQL identifier and string escaping utilities
///
/// Reference:
/// - ISO/IEC 9075:2023 (SQL identifier quoting)
/// - W3C SPARQL 1.1 (NCName validation)
/// - W3C XML Namespaces (NCName production)

import Foundation

// MARK: - SQL Escaping

/// SQL identifier and string escaping utilities
public enum SQLEscape {
    /// Quote SQL identifier (table/column names)
    /// Uses double-quote escaping per SQL standard
    ///
    /// Reference: ISO/IEC 9075:2023 Section 5.2 <delimited identifier>
    public static func identifier(_ name: String) -> String {
        "\"\(name.replacingOccurrences(of: "\"", with: "\"\""))\""
    }

    /// Escape SQL string literal
    /// Uses single-quote escaping per SQL standard
    ///
    /// Reference: ISO/IEC 9075:2023 Section 5.3 <character string literal>
    public static func string(_ value: String) -> String {
        "'\(value.replacingOccurrences(of: "'", with: "''"))'"
    }

    /// Quote identifier only if it contains special characters
    /// Returns unquoted if it's a simple identifier (letters, digits, underscore)
    public static func identifierIfNeeded(_ name: String) -> String {
        // SQL standard simple identifier: starts with letter, contains only letters, digits, underscore
        let simpleIdentifierPattern = "^[a-zA-Z_][a-zA-Z0-9_]*$"
        if name.range(of: simpleIdentifierPattern, options: .regularExpression) != nil {
            // Also check against reserved words
            if !sqlReservedWords.contains(name.uppercased()) {
                return name
            }
        }
        return identifier(name)
    }

    /// Common SQL reserved words that require quoting
    private static let sqlReservedWords: Set<String> = [
        "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN",
        "IS", "NULL", "TRUE", "FALSE", "AS", "JOIN", "INNER", "LEFT", "RIGHT",
        "FULL", "CROSS", "ON", "USING", "GROUP", "BY", "HAVING", "ORDER", "ASC",
        "DESC", "LIMIT", "OFFSET", "DISTINCT", "ALL", "UNION", "INTERSECT",
        "EXCEPT", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
        "CREATE", "DROP", "TABLE", "INDEX", "GRAPH", "PROPERTY", "MATCH",
        "WITH", "CASE", "WHEN", "THEN", "ELSE", "END", "CAST", "COUNT",
        "SUM", "AVG", "MIN", "MAX", "EXISTS", "ANY", "SOME"
    ]
}

// MARK: - SPARQL Escaping

/// SPARQL identifier and IRI escaping utilities
public enum SPARQLEscape {
    /// Validate and return NCName (prefix/local name)
    ///
    /// Reference: W3C XML Namespaces 1.0 NCName production
    /// NCName ::= Name - (Char* ':' Char*)
    /// Name ::= NameStartChar (NameChar)*
    /// NameStartChar ::= ":" | [A-Z] | "_" | [a-z] | ...
    /// NameChar ::= NameStartChar | "-" | "." | [0-9] | ...
    ///
    /// Simplified pattern for common use cases
    public static func ncName(_ name: String) throws -> String {
        guard !name.isEmpty else {
            throw SPARQLEscapeError.emptyNCName
        }

        let pattern = "^[a-zA-Z_][a-zA-Z0-9_.-]*$"
        guard name.range(of: pattern, options: .regularExpression) != nil else {
            throw SPARQLEscapeError.invalidNCName(name)
        }

        return name
    }

    /// Validate NCName and return it, or return nil if invalid
    public static func ncNameOrNil(_ name: String) -> String? {
        try? ncName(name)
    }

    /// Escape IRI for SPARQL
    /// Escapes characters that are not allowed in IRIs
    ///
    /// Reference: RFC 3987 (IRI), SPARQL 1.1 Section 19.5
    public static func iri(_ value: String) -> String {
        // Escape characters not allowed in IRIs: < > " { } | ^ ` \
        let escaped = value
            .replacingOccurrences(of: "\\", with: "%5C")
            .replacingOccurrences(of: "<", with: "%3C")
            .replacingOccurrences(of: ">", with: "%3E")
            .replacingOccurrences(of: "\"", with: "%22")
            .replacingOccurrences(of: "{", with: "%7B")
            .replacingOccurrences(of: "}", with: "%7D")
            .replacingOccurrences(of: "|", with: "%7C")
            .replacingOccurrences(of: "^", with: "%5E")
            .replacingOccurrences(of: "`", with: "%60")
        return "<\(escaped)>"
    }

    /// Escape string for SPARQL literal
    /// Escapes special characters in string literals
    ///
    /// Reference: SPARQL 1.1 Section 19.5
    public static func string(_ value: String) -> String {
        let escaped = value
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\"", with: "\\\"")
            .replacingOccurrences(of: "\n", with: "\\n")
            .replacingOccurrences(of: "\r", with: "\\r")
            .replacingOccurrences(of: "\t", with: "\\t")
        return "\"\(escaped)\""
    }

    /// Validate and format a prefixed name
    /// Returns "prefix:local" with validated components
    public static func prefixedName(prefix: String, local: String) throws -> String {
        let validatedPrefix = try ncName(prefix)
        // Local part can be empty or valid NCName characters
        if !local.isEmpty {
            // Local part allows more characters than prefix
            let localPattern = "^[a-zA-Z0-9_.-]*$"
            guard local.range(of: localPattern, options: .regularExpression) != nil else {
                throw SPARQLEscapeError.invalidLocalName(local)
            }
        }
        return "\(validatedPrefix):\(local)"
    }
}

/// SPARQL escaping errors
public enum SPARQLEscapeError: Error, Sendable, Equatable {
    case emptyNCName
    case invalidNCName(String)
    case invalidLocalName(String)
    case invalidIRI(String)
}
