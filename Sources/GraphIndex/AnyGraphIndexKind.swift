// AnyGraphIndexKind.swift
// GraphIndex - Type-erased protocol for accessing graph index metadata

import Foundation
import Core
import Graph

/// Type-erased protocol for accessing graph index metadata
///
/// **Purpose**: Allows access to GraphIndexKind properties without knowing the Root type parameter.
///
/// **Design Rationale**:
/// GraphIndexKind<Root> has a generic Root parameter, but its metadata properties
/// (fromField, edgeField, toField, strategy) are just Strings and enums - they don't
/// depend on Root. This protocol exposes those properties in a type-erased way.
///
/// **Usage**:
/// ```swift
/// // Type-erased access to graph index metadata
/// let descriptor: IndexDescriptor = ...
/// if let graphKind = descriptor.kind as? AnyGraphIndexKind {
///     print("From field: \(graphKind.fromFieldName)")
///     print("Edge field: \(graphKind.edgeFieldName)")
///     print("To field: \(graphKind.toFieldName)")
///     print("Strategy: \(graphKind.strategy)")
/// }
/// ```
///
/// **Use Cases**:
/// 1. SPARQL() SQL function: Extract metadata from IndexDescriptor without knowing Root
/// 2. CLI tools: Display graph index configuration
/// 3. Schema introspection: Analyze graph indexes dynamically
package protocol AnyGraphIndexKind: IndexKind {
    /// Graph storage strategy
    var strategy: GraphIndexStrategy { get }

    /// From node field name (RDF: Subject, Graph: Source)
    var fromFieldName: String { get }

    /// Edge label field name (RDF: Predicate, Graph: Label)
    var edgeFieldName: String { get }

    /// To node field name (RDF: Object, Graph: Target)
    var toFieldName: String { get }

    /// Graph field name (RDF: Named Graph)
    /// nil if not using named graphs
    var graphFieldName: String? { get }
}

// MARK: - GraphIndexKind Extension

extension GraphIndexKind: AnyGraphIndexKind {
    /// From node field name (type-erased accessor)
    package var fromFieldName: String { fromField }

    /// Edge label field name (type-erased accessor)
    package var edgeFieldName: String { edgeField }

    /// To node field name (type-erased accessor)
    package var toFieldName: String { toField }

    /// Graph field name (type-erased accessor)
    package var graphFieldName: String? { graphField }
}
