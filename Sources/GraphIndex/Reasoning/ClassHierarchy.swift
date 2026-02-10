// ClassHierarchy.swift
// GraphIndex - Class hierarchy management for OWL reasoning
//
// Manages class hierarchy computed from TBox axioms.
// Used for classification and subsumption checking.
//
// Reference: Baader, F., et al. (2003).
// "The Description Logic Handbook: Theory, Implementation, and Applications."

import Foundation
import Graph

/// Class Hierarchy Manager
///
/// Manages the hierarchy of classes including:
/// - Named class declarations
/// - SubClassOf relationships
/// - EquivalentClasses relationships
/// - DisjointClasses relationships
///
/// **Example**:
/// ```swift
/// var hierarchy = ClassHierarchy()
/// hierarchy.load(from: ontology)
///
/// // Query hierarchy
/// let superClasses = hierarchy.superClasses(of: "ex:Employee")
/// let subClasses = hierarchy.subClasses(of: "ex:Person")
/// let isSubclass = hierarchy.subsumes("ex:Person", "ex:Employee")
///
/// // Check disjointness
/// let areDisjoint = hierarchy.areDisjoint("ex:Male", "ex:Female")
/// ```
public struct ClassHierarchy: Sendable {

    // MARK: - Types

    /// Class information
    public struct ClassInfo: Sendable {
        public let iri: String
        public var directSuperClasses: Set<String> = []
        public var directSubClasses: Set<String> = []
        public var equivalentClasses: Set<String> = []
        public var disjointClasses: Set<String> = []
        public var definedAs: OWLClassExpression?  // For defined classes

        public init(iri: String) {
            self.iri = iri
        }
    }

    // MARK: - Properties

    /// Class information indexed by IRI
    private var classes: [String: ClassInfo] = [:]

    /// Cached transitive closure of super-classes
    private var superClassClosure: [String: Set<String>] = [:]

    /// Cached transitive closure of sub-classes
    private var subClassClosure: [String: Set<String>] = [:]

    /// Disjoint class pairs (for efficient lookup)
    private var disjointPairs: Set<DisjointPair> = []

    /// Whether closures are computed
    private var closuresComputed: Bool = false

    // MARK: - Disjoint Pair

    private struct DisjointPair: Hashable, Sendable {
        let class1: String
        let class2: String

        init(_ c1: String, _ c2: String) {
            // Normalize order for consistent hashing
            if c1 < c2 {
                self.class1 = c1
                self.class2 = c2
            } else {
                self.class1 = c2
                self.class2 = c1
            }
        }
    }

    // MARK: - Initialization

    public init() {}

    /// Initialize from ontology
    public init(ontology: OWLOntology) {
        var hierarchy = ClassHierarchy()
        hierarchy.load(from: ontology)
        self = hierarchy
    }

    /// Initialize from ontology with pre-built index (avoids redundant O(n) scans)
    public init(ontology: OWLOntology, index: OntologyIndex) {
        var hierarchy = ClassHierarchy()
        hierarchy.loadWithIndex(from: ontology, index: index)
        self = hierarchy
    }

    // MARK: - Loading

    /// Load class hierarchy from ontology
    public mutating func load(from ontology: OWLOntology) {
        // Clear existing data
        classes = [:]
        superClassClosure = [:]
        subClassClosure = [:]
        disjointPairs = []
        closuresComputed = false

        // Add owl:Thing as top class
        classes["owl:Thing"] = ClassInfo(iri: "owl:Thing")

        // Add owl:Nothing as bottom class
        var nothingInfo = ClassInfo(iri: "owl:Nothing")
        nothingInfo.directSuperClasses.insert("owl:Thing")
        classes["owl:Nothing"] = nothingInfo

        // Load from class declarations
        for cls in ontology.classes {
            var info = getOrCreateClass(cls.iri)
            // All named classes are subclasses of owl:Thing
            if cls.iri != "owl:Thing" && cls.iri != "owl:Nothing" {
                info.directSuperClasses.insert("owl:Thing")
            }
            classes[cls.iri] = info
        }

        // Load from axioms
        for axiom in ontology.axioms {
            switch axiom {
            case .subClassOf(let sub, let sup):
                processSubClassOf(sub: sub, sup: sup)

            case .equivalentClasses(let exprs):
                processEquivalentClasses(exprs)

            case .disjointClasses(let exprs):
                processDisjointClasses(exprs)

            case .disjointUnion(let cls, let disjuncts):
                processDisjointUnion(class_: cls, disjuncts: disjuncts)

            default:
                break
            }
        }
    }

    /// Load class hierarchy using pre-built OntologyIndex
    /// Iterates over pre-classified axiom arrays instead of full axioms
    public mutating func loadWithIndex(from ontology: OWLOntology, index: OntologyIndex) {
        // Clear existing data
        classes = [:]
        superClassClosure = [:]
        subClassClosure = [:]
        disjointPairs = []
        closuresComputed = false

        // Add owl:Thing as top class
        classes["owl:Thing"] = ClassInfo(iri: "owl:Thing")

        // Add owl:Nothing as bottom class
        var nothingInfo = ClassInfo(iri: "owl:Nothing")
        nothingInfo.directSuperClasses.insert("owl:Thing")
        classes["owl:Nothing"] = nothingInfo

        // Load from class declarations
        for cls in ontology.classes {
            var info = getOrCreateClass(cls.iri)
            if cls.iri != "owl:Thing" && cls.iri != "owl:Nothing" {
                info.directSuperClasses.insert("owl:Thing")
            }
            classes[cls.iri] = info
        }

        // Load from TBox axioms only (pre-filtered by index)
        for axiom in index.tboxAxioms {
            switch axiom {
            case .subClassOf(let sub, let sup):
                processSubClassOf(sub: sub, sup: sup)

            case .equivalentClasses(let exprs):
                processEquivalentClasses(exprs)

            case .disjointClasses(let exprs):
                processDisjointClasses(exprs)

            case .disjointUnion(let cls, let disjuncts):
                processDisjointUnion(class_: cls, disjuncts: disjuncts)

            default:
                break
            }
        }
    }

    private mutating func getOrCreateClass(_ iri: String) -> ClassInfo {
        if let existing = classes[iri] {
            return existing
        }
        return ClassInfo(iri: iri)
    }

    private mutating func processSubClassOf(sub: OWLClassExpression, sup: OWLClassExpression) {
        // Handle named class to named class case
        if case .named(let subIRI) = sub, case .named(let supIRI) = sup {
            var subInfo = getOrCreateClass(subIRI)
            subInfo.directSuperClasses.insert(supIRI)
            // Remove owl:Thing if we have a more specific super class
            if supIRI != "owl:Thing" {
                subInfo.directSuperClasses.remove("owl:Thing")
            }
            classes[subIRI] = subInfo

            var supInfo = getOrCreateClass(supIRI)
            supInfo.directSubClasses.insert(subIRI)
            classes[supIRI] = supInfo
        }
        // For complex expressions, we store the definition
        else if case .named(let subIRI) = sub {
            let info = getOrCreateClass(subIRI)
            // Store the superclass expression for later reasoning
            classes[subIRI] = info
        }
    }

    private mutating func processEquivalentClasses(_ exprs: [OWLClassExpression]) {
        // Extract named classes
        var namedClasses: [String] = []
        var complexExprs: [OWLClassExpression] = []

        for expr in exprs {
            if case .named(let iri) = expr {
                namedClasses.append(iri)
            } else {
                complexExprs.append(expr)
            }
        }

        // Named classes are equivalent to each other
        for i in 0..<namedClasses.count {
            for j in 0..<namedClasses.count where i != j {
                var info = getOrCreateClass(namedClasses[i])
                info.equivalentClasses.insert(namedClasses[j])
                // Equivalent classes are mutual subclasses
                info.directSuperClasses.insert(namedClasses[j])
                info.directSubClasses.insert(namedClasses[j])
                classes[namedClasses[i]] = info
            }
        }

        // Store complex expressions as definitions
        if !complexExprs.isEmpty && !namedClasses.isEmpty {
            for named in namedClasses {
                var info = getOrCreateClass(named)
                info.definedAs = complexExprs.first
                classes[named] = info
            }
        }
    }

    private mutating func processDisjointClasses(_ exprs: [OWLClassExpression]) {
        // Extract named classes for disjoint pairs
        var namedClasses: [String] = []
        for expr in exprs {
            if case .named(let iri) = expr {
                namedClasses.append(iri)
            }
        }

        // Create pairwise disjoint relationships
        for i in 0..<namedClasses.count {
            for j in (i+1)..<namedClasses.count {
                let pair = DisjointPair(namedClasses[i], namedClasses[j])
                disjointPairs.insert(pair)

                var info1 = getOrCreateClass(namedClasses[i])
                info1.disjointClasses.insert(namedClasses[j])
                classes[namedClasses[i]] = info1

                var info2 = getOrCreateClass(namedClasses[j])
                info2.disjointClasses.insert(namedClasses[i])
                classes[namedClasses[j]] = info2
            }
        }
    }

    private mutating func processDisjointUnion(class_: String, disjuncts: [OWLClassExpression]) {
        var classInfo = getOrCreateClass(class_)

        // Extract named classes from disjuncts
        var namedDisjuncts: [String] = []
        for expr in disjuncts {
            if case .named(let iri) = expr {
                namedDisjuncts.append(iri)
                // Each disjunct is a subclass of the union class
                var disjunctInfo = getOrCreateClass(iri)
                disjunctInfo.directSuperClasses.insert(class_)
                classes[iri] = disjunctInfo

                classInfo.directSubClasses.insert(iri)
            }
        }

        classes[class_] = classInfo

        // Disjuncts are pairwise disjoint
        for i in 0..<namedDisjuncts.count {
            for j in (i+1)..<namedDisjuncts.count {
                let pair = DisjointPair(namedDisjuncts[i], namedDisjuncts[j])
                disjointPairs.insert(pair)

                var info1 = getOrCreateClass(namedDisjuncts[i])
                info1.disjointClasses.insert(namedDisjuncts[j])
                classes[namedDisjuncts[i]] = info1

                var info2 = getOrCreateClass(namedDisjuncts[j])
                info2.disjointClasses.insert(namedDisjuncts[i])
                classes[namedDisjuncts[j]] = info2
            }
        }
    }

    // MARK: - Query Methods

    /// Get all known classes
    public var allClasses: Set<String> {
        Set(classes.keys)
    }

    /// Get class information
    public func info(for class_: String) -> ClassInfo? {
        classes[class_]
    }

    /// Get direct super-classes
    public func directSuperClasses(of class_: String) -> Set<String> {
        classes[class_]?.directSuperClasses ?? []
    }

    /// Get direct sub-classes
    public func directSubClasses(of class_: String) -> Set<String> {
        classes[class_]?.directSubClasses ?? []
    }

    /// Get all super-classes (transitive closure)
    public mutating func superClasses(of class_: String) -> Set<String> {
        computeClosuresIfNeeded()
        return superClassClosure[class_] ?? []
    }

    /// Get all sub-classes (transitive closure)
    public mutating func subClasses(of class_: String) -> Set<String> {
        computeClosuresIfNeeded()
        return subClassClosure[class_] ?? []
    }

    /// Get equivalent classes
    public func equivalentClasses(of class_: String) -> Set<String> {
        classes[class_]?.equivalentClasses ?? []
    }

    /// Get disjoint classes
    public func disjointClasses(of class_: String) -> Set<String> {
        classes[class_]?.disjointClasses ?? []
    }

    /// Get class definition (if defined class)
    public func definition(of class_: String) -> OWLClassExpression? {
        classes[class_]?.definedAs
    }

    // MARK: - Subsumption

    /// Check if sup subsumes sub (sub ⊑ sup)
    public mutating func subsumes(_ sup: String, _ sub: String) -> Bool {
        if sup == sub { return true }
        if sup == "owl:Thing" { return true }
        if sub == "owl:Nothing" { return true }

        computeClosuresIfNeeded()
        return superClassClosure[sub]?.contains(sup) ?? false
    }

    /// Check if two classes are equivalent
    public mutating func areEquivalent(_ class1: String, _ class2: String) -> Bool {
        if class1 == class2 { return true }
        return classes[class1]?.equivalentClasses.contains(class2) ?? false
    }

    /// Check if two classes are disjoint
    public func areDisjoint(_ class1: String, _ class2: String) -> Bool {
        if class1 == class2 { return false }
        if class1 == "owl:Nothing" || class2 == "owl:Nothing" { return true }

        let pair = DisjointPair(class1, class2)
        return disjointPairs.contains(pair)
    }

    /// Check if two classes are disjoint (considering hierarchy)
    public mutating func areDisjointWithHierarchy(_ class1: String, _ class2: String) -> Bool {
        // Direct disjointness
        if areDisjoint(class1, class2) { return true }

        // Check if any super-class pair is disjoint
        computeClosuresIfNeeded()

        let supers1 = superClassClosure[class1] ?? []
        let supers2 = superClassClosure[class2] ?? []

        for s1 in supers1.union([class1]) {
            for s2 in supers2.union([class2]) {
                if areDisjoint(s1, s2) {
                    return true
                }
            }
        }

        return false
    }

    // MARK: - Transitive Closure Computation (Topological Sort)
    //
    // Uses Kahn's algorithm for topological ordering, computing closures
    // in a single O(V + E) pass instead of per-class DFS O(n * (V+E)).
    //
    // Reference: Kahn, A.B. (1962). "Topological sorting of large networks"

    private mutating func computeClosuresIfNeeded() {
        guard !closuresComputed else { return }

        let allClasses = Array(classes.keys)

        // --- Super-class closure (bottom-up: leaves → roots) ---
        // Compute in-degree based on directSuperClasses (edges go sub → super)
        var superInDegree: [String: Int] = [:]
        for cls in allClasses {
            superInDegree[cls] = 0
        }
        // For super-class closure, we process leaves first
        // An edge sub → super means super depends on sub being processed first? No.
        // We want: closure(cls) = directSupers ∪ union(closure(each directSuper))
        // So we need supers processed before subs → process in top-down order for supers
        // Actually, for super-class closure: closure(C) = directSupers(C) ∪ ⋃{closure(S) | S ∈ directSupers(C)}
        // We need each super S processed first. So process top → bottom (roots first).

        // Build reverse edges: for each cls, who are its directSuperClasses
        // Process order: classes with no super-classes first (roots)
        var rootQueue: [String] = []
        var superDependencyCount: [String: Int] = [:]

        for cls in allClasses {
            let supers = classes[cls]?.directSuperClasses ?? []
            let equivs = classes[cls]?.equivalentClasses ?? []
            // Dependencies = super-classes + equivalent classes that need processing first
            let depCount = supers.count + equivs.count
            superDependencyCount[cls] = depCount
            if depCount == 0 {
                rootQueue.append(cls)
            }
        }

        // Initialize closures
        for cls in allClasses {
            superClassClosure[cls] = []
            subClassClosure[cls] = []
        }

        // Process in topological order for super-class closure
        var processedForSuper = Set<String>()
        var queue = rootQueue

        while !queue.isEmpty {
            let cls = queue.removeFirst()
            if processedForSuper.contains(cls) { continue }
            processedForSuper.insert(cls)

            var closure = Set<String>()
            let info = classes[cls]

            // Add direct super-classes and their closures
            for superCls in info?.directSuperClasses ?? [] {
                closure.insert(superCls)
                closure.formUnion(superClassClosure[superCls] ?? [])
            }
            // Add equivalent classes' closures
            for equiv in info?.equivalentClasses ?? [] {
                if let equivClosure = superClassClosure[equiv] {
                    closure.formUnion(equivClosure)
                }
            }

            superClassClosure[cls] = closure

            // Enqueue dependents: classes whose directSuperClasses include cls
            for (otherCls, otherInfo) in classes {
                if processedForSuper.contains(otherCls) { continue }
                if otherInfo.directSuperClasses.contains(cls) || otherInfo.equivalentClasses.contains(cls) {
                    // Check if all dependencies of otherCls are now processed
                    let allDepsProcessed = otherInfo.directSuperClasses.allSatisfy { processedForSuper.contains($0) }
                        && otherInfo.equivalentClasses.allSatisfy { processedForSuper.contains($0) }
                    if allDepsProcessed {
                        queue.append(otherCls)
                    }
                }
            }
        }

        // Handle unprocessed classes (cycles — equivalent classes form cycles)
        for cls in allClasses where !processedForSuper.contains(cls) {
            superClassClosure[cls] = computeSuperClosureFallback(for: cls, visited: [])
        }

        // --- Sub-class closure (top-down: roots → leaves) ---
        // closure(C) = directSubs(C) ∪ ⋃{closure(S) | S ∈ directSubs(C)}
        // Process leaves first (classes with no sub-classes)
        var processedForSub = Set<String>()
        var leafQueue: [String] = []

        for cls in allClasses {
            let subs = classes[cls]?.directSubClasses ?? []
            let equivs = classes[cls]?.equivalentClasses ?? []
            if subs.isEmpty && equivs.isEmpty {
                leafQueue.append(cls)
            }
        }

        queue = leafQueue
        while !queue.isEmpty {
            let cls = queue.removeFirst()
            if processedForSub.contains(cls) { continue }
            processedForSub.insert(cls)

            var closure = Set<String>()
            let info = classes[cls]

            for subCls in info?.directSubClasses ?? [] {
                closure.insert(subCls)
                closure.formUnion(subClassClosure[subCls] ?? [])
            }
            for equiv in info?.equivalentClasses ?? [] {
                if let equivClosure = subClassClosure[equiv] {
                    closure.formUnion(equivClosure)
                }
            }

            subClassClosure[cls] = closure

            // Enqueue parents
            for (otherCls, otherInfo) in classes {
                if processedForSub.contains(otherCls) { continue }
                if otherInfo.directSubClasses.contains(cls) || otherInfo.equivalentClasses.contains(cls) {
                    let allDepsProcessed = otherInfo.directSubClasses.allSatisfy { processedForSub.contains($0) }
                        && otherInfo.equivalentClasses.allSatisfy { processedForSub.contains($0) }
                    if allDepsProcessed {
                        queue.append(otherCls)
                    }
                }
            }
        }

        // Handle unprocessed (cycles)
        for cls in allClasses where !processedForSub.contains(cls) {
            subClassClosure[cls] = computeSubClosureFallback(for: cls, visited: [])
        }

        closuresComputed = true
    }

    /// Fallback DFS for cycles (equivalent classes form mutual sub/super relationships)
    private func computeSuperClosureFallback(for class_: String, visited: Set<String>) -> Set<String> {
        var result = Set<String>()
        guard let info = classes[class_] else { return result }
        guard !visited.contains(class_) else { return result }

        var newVisited = visited
        newVisited.insert(class_)

        for superClass in info.directSuperClasses {
            result.insert(superClass)
            result.formUnion(computeSuperClosureFallback(for: superClass, visited: newVisited))
        }
        for equiv in info.equivalentClasses {
            if !visited.contains(equiv) {
                result.formUnion(computeSuperClosureFallback(for: equiv, visited: newVisited))
            }
        }

        return result
    }

    private func computeSubClosureFallback(for class_: String, visited: Set<String>) -> Set<String> {
        var result = Set<String>()
        guard let info = classes[class_] else { return result }
        guard !visited.contains(class_) else { return result }

        var newVisited = visited
        newVisited.insert(class_)

        for subClass in info.directSubClasses {
            result.insert(subClass)
            result.formUnion(computeSubClosureFallback(for: subClass, visited: newVisited))
        }
        for equiv in info.equivalentClasses {
            if !visited.contains(equiv) {
                result.formUnion(computeSubClosureFallback(for: equiv, visited: newVisited))
            }
        }

        return result
    }

    // MARK: - Classification

    /// Get the most specific super-classes (direct parents in hierarchy)
    public mutating func mostSpecificSuperClasses(of class_: String) -> Set<String> {
        let direct = directSuperClasses(of: class_)
        if direct.isEmpty { return [] }

        // Remove any class that is a super-class of another in the set
        var result = direct
        for c1 in direct {
            for c2 in direct where c1 != c2 {
                if subsumes(c1, c2) {
                    result.remove(c1)
                }
            }
        }

        return result
    }

    /// Get the most general sub-classes (direct children in hierarchy)
    public mutating func mostGeneralSubClasses(of class_: String) -> Set<String> {
        let direct = directSubClasses(of: class_)
        if direct.isEmpty { return [] }

        // Remove any class that is a sub-class of another in the set
        var result = direct
        for c1 in direct {
            for c2 in direct where c1 != c2 {
                if subsumes(c2, c1) {
                    result.remove(c1)
                }
            }
        }

        return result
    }

    /// Check if a class is satisfiable (not equivalent to owl:Nothing)
    public mutating func isSatisfiable(_ class_: String) -> Bool {
        if class_ == "owl:Nothing" { return false }
        // A class is unsatisfiable if it subsumes owl:Nothing
        // This is a basic check; full satisfiability requires reasoning
        return !areEquivalent(class_, "owl:Nothing")
    }

    // MARK: - Modification Methods

    /// Add a subsumption relationship (subClass ⊑ superClass)
    ///
    /// - Parameters:
    ///   - subClass: The subclass IRI
    ///   - superClass: The superclass IRI
    public mutating func addSubsumption(subClass: String, superClass: String) {
        // Skip if same class or trivial cases
        guard subClass != superClass else { return }
        guard superClass != "owl:Thing" else { return }
        guard subClass != "owl:Nothing" else { return }

        // Ensure both classes exist
        if classes[subClass] == nil {
            classes[subClass] = ClassInfo(iri: subClass)
        }
        if classes[superClass] == nil {
            classes[superClass] = ClassInfo(iri: superClass)
        }

        // Add relationship
        classes[subClass]?.directSuperClasses.insert(superClass)
        classes[superClass]?.directSubClasses.insert(subClass)

        // Invalidate cached closures
        closuresComputed = false
        superClassClosure = [:]
        subClassClosure = [:]
    }

    /// Add an equivalence relationship
    ///
    /// - Parameters:
    ///   - class1: First class IRI
    ///   - class2: Second class IRI
    public mutating func addEquivalence(class1: String, class2: String) {
        guard class1 != class2 else { return }

        // Ensure both classes exist
        if classes[class1] == nil {
            classes[class1] = ClassInfo(iri: class1)
        }
        if classes[class2] == nil {
            classes[class2] = ClassInfo(iri: class2)
        }

        // Add mutual equivalence
        classes[class1]?.equivalentClasses.insert(class2)
        classes[class2]?.equivalentClasses.insert(class1)

        // Equivalent classes are mutual subclasses
        classes[class1]?.directSuperClasses.insert(class2)
        classes[class1]?.directSubClasses.insert(class2)
        classes[class2]?.directSuperClasses.insert(class1)
        classes[class2]?.directSubClasses.insert(class1)

        // Invalidate cached closures
        closuresComputed = false
        superClassClosure = [:]
        subClassClosure = [:]
    }

    /// Add a disjoint relationship
    ///
    /// - Parameters:
    ///   - class1: First class IRI
    ///   - class2: Second class IRI
    public mutating func addDisjoint(class1: String, class2: String) {
        guard class1 != class2 else { return }

        // Ensure both classes exist
        if classes[class1] == nil {
            classes[class1] = ClassInfo(iri: class1)
        }
        if classes[class2] == nil {
            classes[class2] = ClassInfo(iri: class2)
        }

        // Add disjoint pair
        let pair = DisjointPair(class1, class2)
        disjointPairs.insert(pair)

        classes[class1]?.disjointClasses.insert(class2)
        classes[class2]?.disjointClasses.insert(class1)
    }
}

// MARK: - CustomStringConvertible

extension ClassHierarchy: CustomStringConvertible {
    public var description: String {
        var lines: [String] = ["ClassHierarchy:"]

        // Sort classes, putting owl:Thing first
        var sortedClasses = classes.keys.sorted()
        if let thingIdx = sortedClasses.firstIndex(of: "owl:Thing") {
            sortedClasses.remove(at: thingIdx)
            sortedClasses.insert("owl:Thing", at: 0)
        }

        for iri in sortedClasses {
            guard let info = classes[iri] else { continue }
            var line = "  \(iri)"

            if !info.directSuperClasses.isEmpty {
                line += " ⊑ \(info.directSuperClasses.sorted().joined(separator: ", "))"
            }

            if !info.equivalentClasses.isEmpty {
                line += " ≡ \(info.equivalentClasses.sorted().joined(separator: ", "))"
            }

            if !info.disjointClasses.isEmpty {
                line += " ⊥ \(info.disjointClasses.sorted().joined(separator: ", "))"
            }

            lines.append(line)
        }

        return lines.joined(separator: "\n")
    }
}

// MARK: - Convenience Extension

extension OWLOntology {
    /// Build class hierarchy from this ontology
    public func buildClassHierarchy() -> ClassHierarchy {
        ClassHierarchy(ontology: self)
    }
}
