/// OntologyCommands - OWL Ontology introspection from OntologyStore

import Foundation
import FoundationDB
import DatabaseEngine
import GraphIndex
import Core
import Graph

public struct OntologyCommands: Sendable {

    nonisolated(unsafe) private let database: any DatabaseProtocol
    private let output: OutputFormatter

    public init(database: any DatabaseProtocol, output: OutputFormatter) {
        self.database = database
        self.output = output
    }

    // MARK: - Show

    public func show() async throws {
        let ontology = try await loadOntology()

        let stats = ontology.statistics
        output.header("Ontology")
        output.line("  IRI:            \(ontology.iri)")
        if let version = ontology.versionIRI {
            output.line("  Version:        \(version)")
        }
        output.line("  Classes:        \(stats.classCount)")
        output.line("  Object Props:   \(stats.objectPropertyCount)")
        output.line("  Data Props:     \(stats.dataPropertyCount)")
        output.line("  Individuals:    \(stats.individualCount)")
        output.line("  Axioms:         \(stats.axiomCount) (TBox: \(stats.tboxAxiomCount), RBox: \(stats.rboxAxiomCount), ABox: \(stats.aboxAxiomCount))")

        if !ontology.classes.isEmpty {
            output.line("")
            printClassHierarchy(ontology)
        }
    }

    // MARK: - Class Hierarchy

    private func printClassHierarchy(_ ontology: OWLOntology) {
        // 各クラスの子クラス IRI を収集
        var childrenMap: [String: [String]] = [:]
        var hasParent: Set<String> = []

        for axiom in ontology.axioms {
            if case .subClassOf(let sub, let sup) = axiom,
               case .named(let subIRI) = sub,
               case .named(let supIRI) = sup {
                childrenMap[supIRI, default: []].append(subIRI)
                hasParent.insert(subIRI)
            }
        }

        // ルートクラス = 親を持たないクラス
        let roots = ontology.classes
            .filter { !hasParent.contains($0.iri) }
            .sorted { displayName($0) < displayName($1) }

        output.header("Class Hierarchy")
        for (index, root) in roots.enumerated() {
            let isLast = index == roots.count - 1
            printTreeNode(
                iri: root.iri,
                ontology: ontology,
                childrenMap: childrenMap,
                prefix: "  ",
                isLast: isLast
            )
        }
    }

    private func printTreeNode(
        iri: String,
        ontology: OWLOntology,
        childrenMap: [String: [String]],
        prefix: String,
        isLast: Bool
    ) {
        let name: String
        if let cls = ontology.findClass(iri) {
            name = displayName(cls)
        } else {
            name = localName(iri)
        }

        output.line("\(prefix)\(name)")

        let children = (childrenMap[iri] ?? []).sorted { localName($0) < localName($1) }
        for (childIndex, childIRI) in children.enumerated() {
            let childIsLast = childIndex == children.count - 1
            let connector = childIsLast ? "└─ " : "├─ "
            let childPrefix = childIsLast ? "   " : "│  "

            let childName: String
            if let cls = ontology.findClass(childIRI) {
                childName = displayName(cls)
            } else {
                childName = localName(childIRI)
            }

            output.line("\(prefix)\(connector)\(childName)")

            // 再帰的に子の子を表示
            let grandChildren = childrenMap[childIRI] ?? []
            if !grandChildren.isEmpty {
                printTreeChildren(
                    iri: childIRI,
                    ontology: ontology,
                    childrenMap: childrenMap,
                    prefix: prefix + childPrefix
                )
            }
        }
    }

    private func printTreeChildren(
        iri: String,
        ontology: OWLOntology,
        childrenMap: [String: [String]],
        prefix: String
    ) {
        let children = (childrenMap[iri] ?? []).sorted { localName($0) < localName($1) }
        for (index, childIRI) in children.enumerated() {
            let isLast = index == children.count - 1
            let connector = isLast ? "└─ " : "├─ "
            let childPrefix = isLast ? "   " : "│  "

            let name: String
            if let cls = ontology.findClass(childIRI) {
                name = displayName(cls)
            } else {
                name = localName(childIRI)
            }

            output.line("\(prefix)\(connector)\(name)")

            let grandChildren = childrenMap[childIRI] ?? []
            if !grandChildren.isEmpty {
                printTreeChildren(
                    iri: childIRI,
                    ontology: ontology,
                    childrenMap: childrenMap,
                    prefix: prefix + childPrefix
                )
            }
        }
    }

    // MARK: - Helpers

    private func loadOntology() async throws -> OWLOntology {
        let store = OntologyStore.default()
        let iris = try await database.withTransaction { tx in
            try await store.listOntologies(transaction: tx)
        }
        guard let firstIRI = iris.first else {
            output.info("(no ontology registered)")
            throw CLIError.invalidArguments("No ontology found in ontology store.")
        }
        guard let ontology = try await database.withTransaction({ tx in
            try await store.reconstruct(iri: firstIRI, transaction: tx)
        }) else {
            throw CLIError.invalidArguments("Failed to reconstruct ontology: \(firstIRI)")
        }
        return ontology
    }

    private func displayName(_ cls: OWLClass) -> String {
        cls.label ?? localName(cls.iri)
    }

    private func localName(_ iri: String) -> String {
        if let hashIndex = iri.lastIndex(of: "#") {
            return String(iri[iri.index(after: hashIndex)...])
        }
        if let slashIndex = iri.lastIndex(of: "/") {
            return String(iri[iri.index(after: slashIndex)...])
        }
        return iri
    }
}

// MARK: - Help

extension OntologyCommands {
    static var helpText: String {
        """
          schema ontology                Show ontology statistics and class hierarchy
        """
    }
}
