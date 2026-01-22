// OntologyStore.swift
// GraphIndex - Persistent ontology storage operations
//
// Provides CRUD operations for ontology storage in FoundationDB.
//
// Reference: W3C OWL 2 https://www.w3.org/TR/owl2-syntax/

import Foundation
import FoundationDB
import Graph

/// Ontology store for persistent TBox/RBox storage
///
/// **Design**: Operations receive transactions as parameters.
/// Does NOT create transactions - follows FDBDataStore pattern.
///
/// **Thread Safety**: This struct is Sendable and all methods are async.
///
/// **Example**:
/// ```swift
/// let store = OntologyStore(subspace: ontologySubspace)
///
/// try await context.withTransaction { tx in
///     // Load ontology
///     try await store.saveOntology(ontology, transaction: tx)
///
///     // Query class hierarchy
///     let superClasses = try await store.getSuperClasses(
///         of: "ex:Employee",
///         ontologyIRI: ontologyIRI,
///         transaction: tx
///     )
/// }
/// ```
public struct OntologyStore: Sendable {

    // MARK: - Properties

    /// Subspace for ontology storage
    public let subspace: OntologySubspace

    // MARK: - Initialization

    /// Create ontology store with subspace
    ///
    /// - Parameter subspace: The ontology subspace
    public init(subspace: OntologySubspace) {
        self.subspace = subspace
    }

    // MARK: - Metadata Operations

    /// Get ontology metadata
    ///
    /// - Parameters:
    ///   - ontologyIRI: The ontology IRI
    ///   - transaction: The transaction to use
    /// - Returns: Metadata if exists, nil otherwise
    public func getMetadata(
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> OntologyMetadata? {
        let key = subspace.metadata(ontologyIRI).pack(Tuple())
        guard let data = try await transaction.getValue(for: key, snapshot: true) else {
            return nil
        }
        return try JSONDecoder().decode(OntologyMetadata.self, from: Data(data))
    }

    /// Save ontology metadata
    ///
    /// - Parameters:
    ///   - metadata: The metadata to save
    ///   - transaction: The transaction to use
    public func saveMetadata(
        _ metadata: OntologyMetadata,
        transaction: any TransactionProtocol
    ) async throws {
        let key = subspace.metadata(metadata.iri).pack(Tuple())
        let data = try JSONEncoder().encode(metadata)
        transaction.setValue(Array(data), for: key)
    }

    /// Delete ontology metadata
    public func deleteMetadata(
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) {
        let key = subspace.metadata(ontologyIRI).pack(Tuple())
        transaction.clear(key: key)
    }

    /// List all ontology IRIs
    public func listOntologies(
        transaction: any TransactionProtocol
    ) async throws -> [String] {
        let (beginKey, endKey) = subspace.base.range()
        var ontologies: [String] = []

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (key, _) in stream {
            if let tuple = try? subspace.base.unpack(key),
               let ontologyIRI = tuple[0] as? String {
                if !ontologies.contains(ontologyIRI) {
                    ontologies.append(ontologyIRI)
                }
            }
        }

        return ontologies
    }

    // MARK: - Class Operations

    /// Get class definition
    ///
    /// - Parameters:
    ///   - classIRI: The class IRI
    ///   - ontologyIRI: The ontology IRI
    ///   - transaction: The transaction to use
    /// - Returns: Class definition if exists
    public func getClass(
        _ classIRI: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> StoredClassDefinition? {
        let key = subspace.classKey(ontologyIRI, classIRI: classIRI)
        guard let data = try await transaction.getValue(for: key, snapshot: true) else {
            return nil
        }
        return try StoredClassDefinition.decode(from: Data(data))
    }

    /// Save class definition
    public func saveClass(
        _ classDef: StoredClassDefinition,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws {
        let key = subspace.classKey(ontologyIRI, classIRI: classDef.iri)
        let data = try classDef.encode()
        transaction.setValue(Array(data), for: key)
    }

    /// Delete class definition
    public func deleteClass(
        _ classIRI: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) {
        let key = subspace.classKey(ontologyIRI, classIRI: classIRI)
        transaction.clear(key: key)
    }

    /// List all classes in an ontology
    public func listClasses(
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> [StoredClassDefinition] {
        let (beginKey, endKey) = subspace.classes(ontologyIRI).range()
        var classes: [StoredClassDefinition] = []

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (_, value) in stream {
            let classDef = try StoredClassDefinition.decode(from: Data(value))
            classes.append(classDef)
        }

        return classes
    }

    // MARK: - Property Operations

    /// Get property definition
    public func getProperty(
        _ propertyIRI: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> StoredPropertyDefinition? {
        let key = subspace.propertyKey(ontologyIRI, propertyIRI: propertyIRI)
        guard let data = try await transaction.getValue(for: key, snapshot: true) else {
            return nil
        }
        return try StoredPropertyDefinition.decode(from: Data(data))
    }

    /// Save property definition
    public func saveProperty(
        _ propDef: StoredPropertyDefinition,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws {
        let key = subspace.propertyKey(ontologyIRI, propertyIRI: propDef.iri)
        let data = try propDef.encode()
        transaction.setValue(Array(data), for: key)
    }

    /// Delete property definition
    public func deleteProperty(
        _ propertyIRI: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) {
        let key = subspace.propertyKey(ontologyIRI, propertyIRI: propertyIRI)
        transaction.clear(key: key)
    }

    /// List all properties in an ontology
    public func listProperties(
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> [StoredPropertyDefinition] {
        let (beginKey, endKey) = subspace.properties(ontologyIRI).range()
        var properties: [StoredPropertyDefinition] = []

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (_, value) in stream {
            let propDef = try StoredPropertyDefinition.decode(from: Data(value))
            properties.append(propDef)
        }

        return properties
    }

    // MARK: - Class Hierarchy Operations

    /// Add class hierarchy entry (materialized)
    ///
    /// Stores both directions for efficient lookup:
    /// - superOf: subClass → superClass
    /// - subOf: superClass → subClass
    public func addClassHierarchyEntry(
        subClass: String,
        superClass: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) {
        // superOf direction
        let superKey = subspace.classSuperOfKey(ontologyIRI, subClass: subClass, superClass: superClass)
        transaction.setValue([], for: superKey)

        // subOf direction
        let subKey = subspace.classSubOfKey(ontologyIRI, superClass: superClass, subClass: subClass)
        transaction.setValue([], for: subKey)
    }

    /// Remove class hierarchy entry
    public func removeClassHierarchyEntry(
        subClass: String,
        superClass: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) {
        let superKey = subspace.classSuperOfKey(ontologyIRI, subClass: subClass, superClass: superClass)
        transaction.clear(key: superKey)

        let subKey = subspace.classSubOfKey(ontologyIRI, superClass: superClass, subClass: subClass)
        transaction.clear(key: subKey)
    }

    /// Get all superclasses of a class (from materialized hierarchy)
    public func getSuperClasses(
        of classIRI: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> Set<String> {
        let (beginKey, endKey) = subspace.classSuperOf(ontologyIRI).subspace(classIRI).range()
        var superClasses: Set<String> = []

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (key, _) in stream {
            if let tuple = try? subspace.classSuperOf(ontologyIRI).subspace(classIRI).unpack(key),
               let superClass = tuple[0] as? String {
                superClasses.insert(superClass)
            }
        }

        return superClasses
    }

    /// Get all subclasses of a class (from materialized hierarchy)
    public func getSubClasses(
        of classIRI: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> Set<String> {
        let (beginKey, endKey) = subspace.classSubOf(ontologyIRI).subspace(classIRI).range()
        var subClasses: Set<String> = []

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (key, _) in stream {
            if let tuple = try? subspace.classSubOf(ontologyIRI).subspace(classIRI).unpack(key),
               let subClass = tuple[0] as? String {
                subClasses.insert(subClass)
            }
        }

        return subClasses
    }

    // MARK: - Property Hierarchy Operations

    /// Add property hierarchy entry
    public func addPropertyHierarchyEntry(
        subProperty: String,
        superProperty: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) {
        let superKey = subspace.propertySuperOfKey(ontologyIRI, subProp: subProperty, superProp: superProperty)
        transaction.setValue([], for: superKey)

        let subKey = subspace.propertySubOfKey(ontologyIRI, superProp: superProperty, subProp: subProperty)
        transaction.setValue([], for: subKey)
    }

    /// Get all superproperties
    public func getSuperProperties(
        of propertyIRI: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> Set<String> {
        let (beginKey, endKey) = subspace.propertySuperOf(ontologyIRI).subspace(propertyIRI).range()
        var superProperties: Set<String> = []

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (key, _) in stream {
            if let tuple = try? subspace.propertySuperOf(ontologyIRI).subspace(propertyIRI).unpack(key),
               let superProp = tuple[0] as? String {
                superProperties.insert(superProp)
            }
        }

        return superProperties
    }

    /// Get all subproperties
    public func getSubProperties(
        of propertyIRI: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> Set<String> {
        let (beginKey, endKey) = subspace.propertySubOf(ontologyIRI).subspace(propertyIRI).range()
        var subProperties: Set<String> = []

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (key, _) in stream {
            if let tuple = try? subspace.propertySubOf(ontologyIRI).subspace(propertyIRI).unpack(key),
               let subProp = tuple[0] as? String {
                subProperties.insert(subProp)
            }
        }

        return subProperties
    }

    // MARK: - Inverse Property Operations

    /// Set inverse property mapping
    public func setInverse(
        property: String,
        inverseProperty: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) {
        // Bidirectional mapping
        let key1 = subspace.inverseKey(ontologyIRI, property: property)
        transaction.setValue(Array(inverseProperty.utf8), for: key1)

        let key2 = subspace.inverseKey(ontologyIRI, property: inverseProperty)
        transaction.setValue(Array(property.utf8), for: key2)
    }

    /// Get inverse property
    public func getInverse(
        of property: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> String? {
        let key = subspace.inverseKey(ontologyIRI, property: property)
        guard let data = try await transaction.getValue(for: key, snapshot: true) else {
            return nil
        }
        return String(decoding: data, as: UTF8.self)
    }

    // MARK: - Transitive Property Operations

    /// Mark property as transitive
    public func markTransitive(
        property: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) {
        let key = subspace.transitiveKey(ontologyIRI, property: property)
        transaction.setValue([], for: key)
    }

    /// Check if property is transitive
    public func isTransitive(
        property: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> Bool {
        let key = subspace.transitiveKey(ontologyIRI, property: property)
        return try await transaction.getValue(for: key, snapshot: true) != nil
    }

    /// Get all transitive properties
    public func getTransitiveProperties(
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> Set<String> {
        let (beginKey, endKey) = subspace.transitive(ontologyIRI).range()
        var properties: Set<String> = []

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (key, _) in stream {
            if let tuple = try? subspace.transitive(ontologyIRI).unpack(key),
               let prop = tuple[0] as? String {
                properties.insert(prop)
            }
        }

        return properties
    }

    // MARK: - Property Chain Operations

    /// Add property chain
    ///
    /// - Parameters:
    ///   - targetProperty: The property implied by the chain
    ///   - chain: Sequence of properties [P1, P2, ...] such that P1 o P2 o ... → targetProperty
    public func addPropertyChain(
        targetProperty: String,
        chain: [String],
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws {
        // Get next chain ID
        let (beginKey, endKey) = subspace.chains(ontologyIRI).subspace(targetProperty).range()
        var maxID = -1

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (key, _) in stream {
            if let tuple = try? subspace.chains(ontologyIRI).subspace(targetProperty).unpack(key),
               let chainID = tuple[0] as? Int {
                maxID = max(maxID, chainID)
            }
        }

        let chainID = maxID + 1
        let key = subspace.chainKey(ontologyIRI, targetProperty: targetProperty, chainID: chainID)
        let data = try JSONEncoder().encode(chain)
        transaction.setValue(Array(data), for: key)
    }

    /// Get property chains for a target property
    public func getPropertyChains(
        for targetProperty: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> [[String]] {
        let (beginKey, endKey) = subspace.chains(ontologyIRI).subspace(targetProperty).range()
        var chains: [[String]] = []

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (_, value) in stream {
            let chain = try JSONDecoder().decode([String].self, from: Data(value))
            chains.append(chain)
        }

        return chains
    }

    /// Get all property chains in an ontology
    public func getAllPropertyChains(
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> [String: [[String]]] {
        let (beginKey, endKey) = subspace.chains(ontologyIRI).range()
        var result: [String: [[String]]] = [:]

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (key, value) in stream {
            if let tuple = try? subspace.chains(ontologyIRI).unpack(key),
               let targetProp = tuple[0] as? String {
                let chain = try JSONDecoder().decode([String].self, from: Data(value))
                result[targetProp, default: []].append(chain)
            }
        }

        return result
    }

    // MARK: - Bulk Operations

    /// Load an OWLOntology into the store
    ///
    /// This performs full materialization of class and property hierarchies.
    public func loadOntology(
        _ ontology: OWLOntology,
        transaction: any TransactionProtocol
    ) async throws {
        // Save metadata
        let metadata = OntologyMetadata(
            iri: ontology.iri,
            versionIRI: ontology.versionIRI,
            imports: ontology.imports,
            prefixes: ontology.prefixes
        )
        try await saveMetadata(metadata, transaction: transaction)

        // Save classes
        for owlClass in ontology.classes {
            let classDef = StoredClassDefinition.from(owlClass)
            try await saveClass(classDef, ontologyIRI: ontology.iri, transaction: transaction)
        }

        // Save properties
        for owlProp in ontology.objectProperties {
            var propDef = StoredPropertyDefinition.from(owlProp)

            // Extract hierarchy from axioms
            for axiom in ontology.axioms {
                if case .subObjectPropertyOf(let sub, let sup) = axiom {
                    if sub == owlProp.iri {
                        propDef.addSuperProperty(sup)
                    }
                }
            }

            try await saveProperty(propDef, ontologyIRI: ontology.iri, transaction: transaction)

            // Mark transitive
            if owlProp.isTransitive {
                markTransitive(property: owlProp.iri, ontologyIRI: ontology.iri, transaction: transaction)
            }

            // Set inverse
            if let inverse = owlProp.inverseOf {
                setInverse(property: owlProp.iri, inverseProperty: inverse, ontologyIRI: ontology.iri, transaction: transaction)
            }

            // Add property chains
            for chain in owlProp.propertyChains {
                try await addPropertyChain(
                    targetProperty: owlProp.iri,
                    chain: chain,
                    ontologyIRI: ontology.iri,
                    transaction: transaction
                )
            }
        }

        for owlProp in ontology.dataProperties {
            let propDef = StoredPropertyDefinition.from(owlProp)
            try await saveProperty(propDef, ontologyIRI: ontology.iri, transaction: transaction)
        }

        // Materialize class hierarchy
        try await materializeClassHierarchy(from: ontology, transaction: transaction)

        // Materialize property hierarchy
        try await materializePropertyHierarchy(from: ontology, transaction: transaction)
    }

    /// Materialize class hierarchy (transitive closure)
    private func materializeClassHierarchy(
        from ontology: OWLOntology,
        transaction: any TransactionProtocol
    ) async throws {
        // Build adjacency list from axioms
        var directSupers: [String: Set<String>] = [:]

        for axiom in ontology.axioms {
            if case .subClassOf(let sub, let sup) = axiom {
                if case .named(let subIRI) = sub, case .named(let supIRI) = sup {
                    directSupers[subIRI, default: []].insert(supIRI)
                }
            }
            if case .equivalentClasses(let exprs) = axiom {
                let namedClasses = exprs.compactMap { expr -> String? in
                    if case .named(let iri) = expr { return iri }
                    return nil
                }
                // Equivalent classes are mutual subclasses
                for i in 0..<namedClasses.count {
                    for j in 0..<namedClasses.count where i != j {
                        directSupers[namedClasses[i], default: []].insert(namedClasses[j])
                    }
                }
            }
        }

        // Compute transitive closure
        for classIRI in directSupers.keys {
            let closure = computeTransitiveClosure(from: classIRI, adjacency: directSupers)
            for superClass in closure {
                addClassHierarchyEntry(
                    subClass: classIRI,
                    superClass: superClass,
                    ontologyIRI: ontology.iri,
                    transaction: transaction
                )
            }
        }
    }

    /// Materialize property hierarchy (transitive closure)
    private func materializePropertyHierarchy(
        from ontology: OWLOntology,
        transaction: any TransactionProtocol
    ) async throws {
        var directSupers: [String: Set<String>] = [:]

        for axiom in ontology.axioms {
            if case .subObjectPropertyOf(let sub, let sup) = axiom {
                directSupers[sub, default: []].insert(sup)
            }
            if case .equivalentObjectProperties(let props) = axiom {
                for i in 0..<props.count {
                    for j in 0..<props.count where i != j {
                        directSupers[props[i], default: []].insert(props[j])
                    }
                }
            }
        }

        for propIRI in directSupers.keys {
            let closure = computeTransitiveClosure(from: propIRI, adjacency: directSupers)
            for superProp in closure {
                addPropertyHierarchyEntry(
                    subProperty: propIRI,
                    superProperty: superProp,
                    ontologyIRI: ontology.iri,
                    transaction: transaction
                )
            }
        }
    }

    /// Compute transitive closure using BFS
    ///
    /// Note: The result may include self-references when equivalentClass
    /// relationships create cycles (A ≡ B implies A ⊑ B and B ⊑ A).
    /// This is semantically correct but may produce redundant entries.
    private func computeTransitiveClosure(
        from start: String,
        adjacency: [String: Set<String>]
    ) -> Set<String> {
        var visited: Set<String> = []
        var queue: [String] = Array(adjacency[start] ?? [])

        while !queue.isEmpty {
            let current = queue.removeFirst()
            if visited.contains(current) { continue }
            visited.insert(current)

            if let nexts = adjacency[current] {
                for next in nexts where !visited.contains(next) {
                    queue.append(next)
                }
            }
        }

        // Remove self-reference if present (start class should not be its own superclass)
        visited.remove(start)

        return visited
    }

    /// Delete entire ontology
    public func deleteOntology(
        _ ontologyIRI: String,
        transaction: any TransactionProtocol
    ) {
        let (beginKey, endKey) = subspace.ontology(ontologyIRI).range()
        transaction.clearRange(beginKey: beginKey, endKey: endKey)
    }
}
