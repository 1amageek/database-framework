// OntologyMetadata.swift
// GraphIndex - Metadata types for ontology storage
//
// Provides metadata structures for persistent ontology storage.
//
// Reference: W3C OWL 2 Syntax https://www.w3.org/TR/owl2-syntax/#Ontologies

import Foundation

/// Schema version for ontology storage format
///
/// Enables schema evolution and migration when storage format changes.
///
/// **Semantic Versioning**:
/// - Major: Breaking changes to storage format
/// - Minor: Backward-compatible additions
/// - Patch: Bug fixes without format changes
public struct SchemaVersion: Codable, Sendable, Hashable, Comparable {

    /// Major version (breaking changes)
    public let major: Int

    /// Minor version (new features)
    public let minor: Int

    /// Patch version (bug fixes)
    public let patch: Int

    public init(major: Int, minor: Int, patch: Int) {
        self.major = major
        self.minor = minor
        self.patch = patch
    }

    /// Current storage schema version
    public static let current = SchemaVersion(major: 1, minor: 0, patch: 0)

    public static func < (lhs: SchemaVersion, rhs: SchemaVersion) -> Bool {
        if lhs.major != rhs.major { return lhs.major < rhs.major }
        if lhs.minor != rhs.minor { return lhs.minor < rhs.minor }
        return lhs.patch < rhs.patch
    }
}

extension SchemaVersion: CustomStringConvertible {
    public var description: String {
        "\(major).\(minor).\(patch)"
    }
}

/// Ontology metadata stored in FoundationDB
///
/// Contains all metadata necessary to identify and manage an ontology.
///
/// **Example**:
/// ```swift
/// let metadata = OntologyMetadata(
///     iri: "http://example.org/family",
///     versionIRI: "http://example.org/family/1.0",
///     imports: ["http://www.w3.org/2002/07/owl#"],
///     prefixes: ["ex": "http://example.org/"]
/// )
/// ```
public struct OntologyMetadata: Codable, Sendable, Hashable {

    /// Ontology IRI (unique identifier)
    public let iri: String

    /// Version IRI (optional, for versioned ontologies)
    public let versionIRI: String?

    /// Storage schema version
    public let schemaVersion: SchemaVersion

    /// Creation timestamp
    public let createdAt: Date

    /// Last update timestamp
    public var updatedAt: Date

    /// Imported ontology IRIs
    public var imports: [String]

    /// Namespace prefix mappings
    public var prefixes: [String: String]

    /// Statistics about the ontology content
    public var statistics: OntologyStatistics

    /// Initialization
    public init(
        iri: String,
        versionIRI: String? = nil,
        schemaVersion: SchemaVersion = .current,
        createdAt: Date = Date(),
        updatedAt: Date = Date(),
        imports: [String] = [],
        prefixes: [String: String] = [:],
        statistics: OntologyStatistics = OntologyStatistics()
    ) {
        self.iri = iri
        self.versionIRI = versionIRI
        self.schemaVersion = schemaVersion
        self.createdAt = createdAt
        self.updatedAt = updatedAt
        self.imports = imports
        self.prefixes = prefixes
        self.statistics = statistics
    }

    /// Update timestamp and return new metadata
    public func withUpdatedTimestamp() -> OntologyMetadata {
        var copy = self
        copy.updatedAt = Date()
        return copy
    }

    /// Update statistics and return new metadata
    public func withStatistics(_ stats: OntologyStatistics) -> OntologyMetadata {
        var copy = self
        copy.statistics = stats
        copy.updatedAt = Date()
        return copy
    }
}

/// Statistics about ontology content
public struct OntologyStatistics: Codable, Sendable, Hashable {

    /// Number of class definitions
    public var classCount: Int

    /// Number of property definitions
    public var propertyCount: Int

    /// Number of axioms
    public var axiomCount: Int

    /// Number of entries in class hierarchy (transitive closure size)
    public var classHierarchySize: Int

    /// Number of entries in property hierarchy (transitive closure size)
    public var propertyHierarchySize: Int

    /// Number of transitive properties
    public var transitivePropertyCount: Int

    /// Number of property chains
    public var propertyChainCount: Int

    /// Number of owl:sameAs equivalence classes
    public var sameAsEquivalenceClassCount: Int

    public init(
        classCount: Int = 0,
        propertyCount: Int = 0,
        axiomCount: Int = 0,
        classHierarchySize: Int = 0,
        propertyHierarchySize: Int = 0,
        transitivePropertyCount: Int = 0,
        propertyChainCount: Int = 0,
        sameAsEquivalenceClassCount: Int = 0
    ) {
        self.classCount = classCount
        self.propertyCount = propertyCount
        self.axiomCount = axiomCount
        self.classHierarchySize = classHierarchySize
        self.propertyHierarchySize = propertyHierarchySize
        self.transitivePropertyCount = transitivePropertyCount
        self.propertyChainCount = propertyChainCount
        self.sameAsEquivalenceClassCount = sameAsEquivalenceClassCount
    }
}

/// Ontology loading status
public enum OntologyStatus: String, Codable, Sendable {
    /// Ontology is being loaded/indexed
    case loading

    /// Ontology is ready for queries
    case ready

    /// Ontology loading failed
    case failed

    /// Ontology is being updated
    case updating

    /// Ontology marked for deletion
    case deleted
}

/// Extended metadata with status tracking
public struct OntologyStatusMetadata: Codable, Sendable {

    /// Core metadata
    public let metadata: OntologyMetadata

    /// Current status
    public var status: OntologyStatus

    /// Error message if status is .failed
    public var errorMessage: String?

    /// Progress (0.0 to 1.0) during loading/updating
    public var progress: Double?

    public init(
        metadata: OntologyMetadata,
        status: OntologyStatus = .loading,
        errorMessage: String? = nil,
        progress: Double? = nil
    ) {
        self.metadata = metadata
        self.status = status
        self.errorMessage = errorMessage
        self.progress = progress
    }
}
