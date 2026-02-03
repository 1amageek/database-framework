// TypeResolver.swift
// DatabaseEngine - Type resolution from Schema

import Foundation
import Core

/// Resolves type names to Schema.Entity and finds graph indexes
///
/// Used by SPARQLFunctionRewriter to dynamically resolve types
/// from SQL SPARQL() function calls.
///
/// **Usage**:
/// ```swift
/// let resolver = TypeResolver(schema: container.schema)
/// let entity = try resolver.resolve(typeName: "RDFTriple")
/// let graphIndex = try resolver.findGraphIndex(for: entity)
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

    /// Find graph index descriptor for an entity
    ///
    /// - Parameter entity: The entity to search
    /// - Returns: First graph index descriptor found
    /// - Throws: `SPARQLFunctionError.graphIndexNotFound` if no graph index exists
    public func findGraphIndex(for entity: Schema.Entity) throws -> IndexDescriptor {
        // indexDescriptors is [IndexDescriptor], find first graph index
        for descriptor in entity.indexDescriptors {
            if type(of: descriptor.kind).identifier == "graph" {
                return descriptor
            }
        }
        throw SPARQLFunctionError.graphIndexNotFound(entity.name)
    }
}
