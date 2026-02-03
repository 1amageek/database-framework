/// GraphTableError.swift
/// Error types for SQL/PGQ GRAPH_TABLE operations

import Foundation

/// Errors that can occur during GRAPH_TABLE execution
public enum GraphTableError: Error, Sendable {
    /// Property expression is too complex to push to index scan
    case complexPropertyExpression(String)

    /// Graph index not found for the specified type
    case indexNotFound(String)

    /// Invalid graph pattern in MATCH clause
    case invalidGraphPattern(String)

    /// Invalid column expression in COLUMNS clause
    case invalidColumnExpression(String)

    /// Type mismatch in expression evaluation
    case typeMismatch(String)
}

extension GraphTableError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .complexPropertyExpression(let message):
            return "Complex property expression: \(message). Use WHERE clause instead."
        case .indexNotFound(let name):
            return "Graph index '\(name)' not found"
        case .invalidGraphPattern(let message):
            return "Invalid graph pattern: \(message)"
        case .invalidColumnExpression(let message):
            return "Invalid column expression: \(message)"
        case .typeMismatch(let message):
            return "Type mismatch: \(message)"
        }
    }
}
