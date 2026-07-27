// TypeResolver.swift
// DatabaseEngine - Type resolution from Schema

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit

/// Resolves persisted type names to schema entities.
///
/// Used by SPARQLFunctionRewriter to dynamically resolve types
/// from SQL SPARQL() function calls.
///
/// **Usage**:
/// ```swift
/// let resolver = TypeResolver(schema: container.schema)
/// let entity = try resolver.resolve(typeName: "RDFTriple")
/// ```
public struct TypeResolver: Sendable {
    private let schema: Schema

    /// Initialize with a schema
    ///
    /// - Parameter schema: The schema to resolve types from
    public init(schema: Schema) {
        self.schema = schema
    }

    /// Resolve type name to Schema.Entity
    ///
    /// - Parameter typeName: Name of the Persistable type
    /// - Returns: Entity definition
    /// - Throws: `SPARQLFunctionError.typeNotFound` if type not in schema
    public func resolve(typeName: String) throws -> Schema.Entity {
        guard let entity = schema.entity(named: typeName) else {
            throw SPARQLFunctionError.typeNotFound(typeName)
        }
        return entity
    }
}
