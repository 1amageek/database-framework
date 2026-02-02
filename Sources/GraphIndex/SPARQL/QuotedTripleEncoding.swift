// QuotedTripleEncoding.swift
// GraphIndex - RDF-star quoted triple encoding/decoding
//
// Encodes/decodes quoted triples to/from canonical string representation
// for storage in FieldValue.string without modifying database-kit.
//
// Format: "<<tag:value\ttag:value\ttag:value>>"
//
// Each component is type-tagged:
//   S:...     → .string (IRIs, literals — most common in RDF)
//   I:...     → .int64
//   D:...     → .double
//   B:...     → .bool
//   Y:...     → .data (base64)
//   N         → .null
//   Q:<<...>> → nested quoted triple (recursive)
//
// Tab (\t) is the component separator.
//
// Safety guarantees:
// - S: payloads percent-encode <, >, %, and \t so they cannot interfere
//   with the << >> delimiters or the tab separator.
// - I:, D:, B:, Y:, N payloads contain only alphanumeric/base64 chars.
// - Therefore, << and >> in the inner content can ONLY come from Q: tags,
//   making the nesting depth tracking in splitComponents fully reliable.
//
// Reference: W3C RDF-star and SPARQL-star (Community Group Report)

import Foundation
import Core

/// Canonical string encoding for RDF-star quoted triples.
///
/// Quoted triples are stored as `FieldValue.string` using a tagged canonical format.
/// This preserves structure and type information in storage without modifying
/// FieldValue's cases. At execution time, the string is decoded back into
/// structured components.
public enum QuotedTripleEncoding {

    private static let separator: Character = "\t"
    private static let openDelimiter = "<<"
    private static let closeDelimiter = ">>"

    // MARK: - Encode

    /// Encode three FieldValues into a canonical quoted triple string.
    public static func encode(subject: FieldValue, predicate: FieldValue, object: FieldValue) -> String {
        let s = encodeComponent(subject)
        let p = encodeComponent(predicate)
        let o = encodeComponent(object)
        return "\(openDelimiter)\(s)\(separator)\(p)\(separator)\(o)\(closeDelimiter)"
    }

    // MARK: - Decode

    /// Decode a canonical quoted triple string into three FieldValues.
    ///
    /// - Parameter string: The canonical string to decode
    /// - Returns: Tuple of (subject, predicate, object) or nil if invalid
    public static func decode(_ string: String) -> (subject: FieldValue, predicate: FieldValue, object: FieldValue)? {
        guard let inner = stripDelimiters(string) else { return nil }

        let components = splitComponents(inner)
        guard components.count == 3 else { return nil }

        guard let subject = decodeComponent(components[0]),
              let predicate = decodeComponent(components[1]),
              let object = decodeComponent(components[2]) else {
            return nil
        }

        return (subject: subject, predicate: predicate, object: object)
    }

    // MARK: - Predicate

    /// Check if a string is a valid canonical quoted triple encoding.
    ///
    /// Validates both the delimiters and the internal structure
    /// (3 correctly tagged components).
    public static func isQuotedTriple(_ string: String) -> Bool {
        decode(string) != nil
    }

    // MARK: - Component Encoding

    /// Encode a single FieldValue to a tagged string.
    ///
    /// String payloads are percent-encoded for <, >, %, and \t to ensure
    /// they cannot interfere with the << >> delimiters or the tab separator.
    private static func encodeComponent(_ value: FieldValue) -> String {
        switch value {
        case .string(let s):
            // Check for nested quoted triple (recursive)
            if s.hasPrefix(openDelimiter), s.hasSuffix(closeDelimiter), decode(s) != nil {
                return "Q:\(s)"
            }
            return "S:\(escapePayload(s))"
        case .int64(let i):
            return "I:\(i)"
        case .double(let d):
            return "D:\(d)"
        case .bool(let b):
            return "B:\(b)"
        case .data(let d):
            return "Y:\(d.base64EncodedString())"
        case .null:
            return "N"
        case .array:
            // Arrays are not representable in RDF triples.
            return "N"
        }
    }

    /// Decode a tagged component string back to FieldValue.
    ///
    /// Returns nil if the tag is unrecognized (defensive).
    private static func decodeComponent(_ component: String) -> FieldValue? {
        guard !component.isEmpty else { return nil }

        // Single-char tag (no colon)
        if component == "N" {
            return .null
        }

        // Two-char prefix tags "X:"
        guard component.count >= 2 else { return nil }
        let tagEnd = component.index(component.startIndex, offsetBy: 2)
        let tag = component[..<tagEnd]
        let payload = String(component[tagEnd...])

        switch tag {
        case "S:":
            return .string(unescapePayload(payload))
        case "I:":
            guard let val = Int64(payload) else { return nil }
            return .int64(val)
        case "D:":
            guard let val = Double(payload) else { return nil }
            return .double(val)
        case "B:":
            return .bool(payload == "true")
        case "Y:":
            guard let data = Data(base64Encoded: payload) else { return nil }
            return .data(data)
        case "Q:":
            // Nested quoted triple: payload is the full <<...>> string
            return .string(payload)
        default:
            return nil
        }
    }

    // MARK: - String Payload Escaping

    /// Percent-encode characters that would interfere with the encoding format.
    ///
    /// Only 4 characters are encoded:
    /// - `%` → `%25` (escape character itself, must be first)
    /// - `<` → `%3C` (delimiter character)
    /// - `>` → `%3E` (delimiter character)
    /// - `\t` → `%09` (separator character)
    ///
    /// For the common case (IRIs like `ex:Toyota`, `rdf:type`), no characters
    /// are affected and the string passes through unchanged.
    private static func escapePayload(_ s: String) -> String {
        // Fast path: most RDF IRIs/literals contain none of these characters
        guard s.contains(where: { $0 == "%" || $0 == "<" || $0 == ">" || $0 == "\t" }) else {
            return s
        }
        var result = ""
        result.reserveCapacity(s.count)
        for char in s {
            switch char {
            case "%": result += "%25"
            case "<": result += "%3C"
            case ">": result += "%3E"
            case "\t": result += "%09"
            default: result.append(char)
            }
        }
        return result
    }

    /// Reverse percent-encoding for the 4 encoded characters.
    private static func unescapePayload(_ s: String) -> String {
        guard s.contains("%") else { return s }
        var result = ""
        result.reserveCapacity(s.count)
        var i = s.startIndex
        while i < s.endIndex {
            if s[i] == "%" {
                let next1 = s.index(after: i)
                let next2 = s.index(next1, offsetBy: 2, limitedBy: s.endIndex)
                if let n2 = next2 {
                    let hex = s[next1..<n2]
                    switch hex {
                    case "25": result.append("%")
                    case "3C": result.append("<")
                    case "3E": result.append(">")
                    case "09": result.append("\t")
                    default:
                        // Unknown escape: keep as-is
                        result.append("%")
                        i = next1
                        continue
                    }
                    i = n2
                    continue
                }
            }
            result.append(s[i])
            i = s.index(after: i)
        }
        return result
    }

    // MARK: - Delimiter Handling

    /// Strip << >> delimiters and return inner content, or nil if not valid.
    private static func stripDelimiters(_ string: String) -> String? {
        guard string.hasPrefix(openDelimiter), string.hasSuffix(closeDelimiter) else {
            return nil
        }
        let startIdx = string.index(string.startIndex, offsetBy: openDelimiter.count)
        let endIdx = string.index(string.endIndex, offsetBy: -closeDelimiter.count)
        guard startIdx <= endIdx else { return nil }
        return String(string[startIdx..<endIdx])
    }

    // MARK: - Splitting

    /// Split inner content by tab separator, respecting nested << >> pairs.
    ///
    /// Because S: payloads have < and > percent-encoded, the << and >>
    /// character sequences in the inner content can ONLY come from Q: tags
    /// (nested quoted triples). This makes depth tracking fully reliable.
    private static func splitComponents(_ inner: String) -> [String] {
        var components: [String] = []
        var current = ""
        var depth = 0

        for char in inner {
            if char == "<" && current.hasSuffix("<") {
                depth += 1
                current.append(char)
            } else if char == ">" && current.hasSuffix(">") && depth > 0 {
                depth -= 1
                current.append(char)
            } else if char == separator && depth == 0 {
                components.append(current)
                current = ""
            } else {
                current.append(char)
            }
        }

        if !current.isEmpty {
            components.append(current)
        }

        return components
    }
}
