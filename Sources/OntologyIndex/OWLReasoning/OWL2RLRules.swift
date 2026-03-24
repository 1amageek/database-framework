// OWL2RLRules.swift
// GraphIndex - OWL 2 RL Rule Definitions
//
// Defines the OWL 2 RL rule catalog for forward-chaining reasoning.
//
// Reference: W3C OWL 2 RL Profile https://www.w3.org/TR/owl2-profiles/#OWL_2_RL

import Foundation

/// OWL 2 RL Rule identifier
///
/// Based on W3C OWL 2 RL Profile specification.
/// Rule names follow the W3C naming convention.
///
/// **Reference**: https://www.w3.org/TR/owl2-profiles/#Reasoning_in_OWL_2_RL_and_RDF_Graphs_using_Rules
public enum OWL2RLRule: String, Codable, Sendable, CaseIterable {

    // MARK: - Equality Rules (eq-*)

    /// eq-ref: x owl:sameAs x (reflexivity)
    case eqRef = "eq-ref"

    /// eq-sym: x owl:sameAs y → y owl:sameAs x (symmetry)
    case eqSym = "eq-sym"

    /// eq-trans: x owl:sameAs y ∧ y owl:sameAs z → x owl:sameAs z (transitivity)
    case eqTrans = "eq-trans"

    /// eq-rep-s: s owl:sameAs s' ∧ s p o → s' p o (subject replacement)
    case eqRepS = "eq-rep-s"

    /// eq-rep-p: p owl:sameAs p' ∧ s p o → s p' o (predicate replacement)
    case eqRepP = "eq-rep-p"

    /// eq-rep-o: o owl:sameAs o' ∧ s p o → s p o' (object replacement)
    case eqRepO = "eq-rep-o"

    /// eq-diff1: x owl:sameAs y ∧ x owl:differentFrom y → ⊥ (inconsistency)
    case eqDiff1 = "eq-diff1"

    // MARK: - Property Rules (prp-*)

    /// prp-dom: p rdfs:domain c ∧ x p y → x rdf:type c
    case prpDom = "prp-dom"

    /// prp-rng: p rdfs:range c ∧ x p y → y rdf:type c
    case prpRng = "prp-rng"

    /// prp-fp: p rdf:type owl:FunctionalProperty ∧ x p y ∧ x p z → y owl:sameAs z
    case prpFp = "prp-fp"

    /// prp-ifp: p rdf:type owl:InverseFunctionalProperty ∧ x p z ∧ y p z → x owl:sameAs y
    case prpIfp = "prp-ifp"

    /// prp-irp: p rdf:type owl:IrreflexiveProperty ∧ x p x → ⊥
    case prpIrp = "prp-irp"

    /// prp-symp: p rdf:type owl:SymmetricProperty ∧ x p y → y p x
    case prpSymp = "prp-symp"

    /// prp-asyp: p rdf:type owl:AsymmetricProperty ∧ x p y ∧ y p x → ⊥
    case prpAsyp = "prp-asyp"

    /// prp-trp: p rdf:type owl:TransitiveProperty ∧ x p y ∧ y p z → x p z
    case prpTrp = "prp-trp"

    /// prp-spo1: p1 rdfs:subPropertyOf p2 ∧ x p1 y → x p2 y
    case prpSpo1 = "prp-spo1"

    /// prp-spo2: p owl:propertyChainAxiom (p1 ... pn) ∧ x1 p1 x2 ∧ ... ∧ xn-1 pn xn → x1 p xn
    case prpSpo2 = "prp-spo2"

    /// prp-eqp1: p1 owl:equivalentProperty p2 ∧ x p1 y → x p2 y
    case prpEqp1 = "prp-eqp1"

    /// prp-eqp2: p1 owl:equivalentProperty p2 ∧ x p2 y → x p1 y
    case prpEqp2 = "prp-eqp2"

    /// prp-pdw: p1 owl:propertyDisjointWith p2 ∧ x p1 y ∧ x p2 y → ⊥
    case prpPdw = "prp-pdw"

    /// prp-inv1: p1 owl:inverseOf p2 ∧ x p1 y → y p2 x
    case prpInv1 = "prp-inv1"

    /// prp-inv2: p1 owl:inverseOf p2 ∧ x p2 y → y p1 x
    case prpInv2 = "prp-inv2"

    // MARK: - Class Rules (cls-*)

    /// cls-thing: owl:Thing is the universal class
    case clsThing = "cls-thing"

    /// cls-nothing1: x rdf:type owl:Nothing → ⊥
    case clsNothing1 = "cls-nothing1"

    /// cls-nothing2: owl:Nothing is empty
    case clsNothing2 = "cls-nothing2"

    /// cls-int1: c owl:intersectionOf (c1 ... cn) ∧ x rdf:type c1 ∧ ... ∧ x rdf:type cn → x rdf:type c
    case clsInt1 = "cls-int1"

    /// cls-int2: c owl:intersectionOf (c1 ... cn) ∧ x rdf:type c → x rdf:type ci
    case clsInt2 = "cls-int2"

    /// cls-uni: c owl:unionOf (c1 ... cn) ∧ x rdf:type ci → x rdf:type c
    case clsUni = "cls-uni"

    /// cls-com: c1 owl:complementOf c2 ∧ x rdf:type c1 ∧ x rdf:type c2 → ⊥
    case clsCom = "cls-com"

    /// cls-svf1: c owl:someValuesFrom y ∧ c owl:onProperty p ∧ u p v ∧ v rdf:type y → u rdf:type c
    case clsSvf1 = "cls-svf1"

    /// cls-svf2: c owl:someValuesFrom owl:Thing ∧ c owl:onProperty p ∧ u p v → u rdf:type c
    case clsSvf2 = "cls-svf2"

    /// cls-avf: c owl:allValuesFrom y ∧ c owl:onProperty p ∧ u rdf:type c ∧ u p v → v rdf:type y
    case clsAvf = "cls-avf"

    /// cls-hv1: c owl:hasValue y ∧ c owl:onProperty p ∧ u rdf:type c → u p y
    case clsHv1 = "cls-hv1"

    /// cls-hv2: c owl:hasValue y ∧ c owl:onProperty p ∧ u p y → u rdf:type c
    case clsHv2 = "cls-hv2"

    /// cls-maxc1: c owl:maxCardinality 0 ∧ c owl:onProperty p ∧ u rdf:type c ∧ u p y → ⊥
    case clsMaxc1 = "cls-maxc1"

    /// cls-maxc2: c owl:maxCardinality 1 ∧ c owl:onProperty p ∧ u rdf:type c ∧ u p y1 ∧ u p y2 → y1 owl:sameAs y2
    case clsMaxc2 = "cls-maxc2"

    /// cls-maxqc1: c owl:maxQualifiedCardinality 0 ∧ c owl:onProperty p ∧ c owl:onClass c1 ∧ u rdf:type c ∧ u p y ∧ y rdf:type c1 → ⊥
    case clsMaxqc1 = "cls-maxqc1"

    /// cls-maxqc2: (qualified max cardinality 1)
    case clsMaxqc2 = "cls-maxqc2"

    /// cls-oo: c owl:oneOf (x1 ... xn) → xi rdf:type c
    case clsOo = "cls-oo"

    // MARK: - Class Axiom Rules (cax-*)

    /// cax-sco: c1 rdfs:subClassOf c2 ∧ x rdf:type c1 → x rdf:type c2
    case caxSco = "cax-sco"

    /// cax-eqc1: c1 owl:equivalentClass c2 ∧ x rdf:type c1 → x rdf:type c2
    case caxEqc1 = "cax-eqc1"

    /// cax-eqc2: c1 owl:equivalentClass c2 ∧ x rdf:type c2 → x rdf:type c1
    case caxEqc2 = "cax-eqc2"

    /// cax-dw: c1 owl:disjointWith c2 ∧ x rdf:type c1 ∧ x rdf:type c2 → ⊥
    case caxDw = "cax-dw"

    // MARK: - Schema Vocabulary Rules (scm-*)

    /// scm-cls: c rdf:type owl:Class → c rdfs:subClassOf c, c owl:equivalentClass c, c rdfs:subClassOf owl:Thing, owl:Nothing rdfs:subClassOf c
    case scmCls = "scm-cls"

    /// scm-sco: c1 rdfs:subClassOf c2 ∧ c2 rdfs:subClassOf c3 → c1 rdfs:subClassOf c3
    case scmSco = "scm-sco"

    /// scm-eqc1: c1 owl:equivalentClass c2 → c1 rdfs:subClassOf c2
    case scmEqc1 = "scm-eqc1"

    /// scm-eqc2: c1 owl:equivalentClass c2 → c2 rdfs:subClassOf c1
    case scmEqc2 = "scm-eqc2"

    /// scm-op: p rdf:type owl:ObjectProperty → p rdfs:subPropertyOf p, p owl:equivalentProperty p
    case scmOp = "scm-op"

    /// scm-dp: p rdf:type owl:DatatypeProperty → p rdfs:subPropertyOf p, p owl:equivalentProperty p
    case scmDp = "scm-dp"

    /// scm-spo: p1 rdfs:subPropertyOf p2 ∧ p2 rdfs:subPropertyOf p3 → p1 rdfs:subPropertyOf p3
    case scmSpo = "scm-spo"

    /// scm-eqp1: p1 owl:equivalentProperty p2 → p1 rdfs:subPropertyOf p2
    case scmEqp1 = "scm-eqp1"

    /// scm-eqp2: p1 owl:equivalentProperty p2 → p2 rdfs:subPropertyOf p1
    case scmEqp2 = "scm-eqp2"

    /// scm-dom1: p rdfs:domain c1 ∧ c1 rdfs:subClassOf c2 → p rdfs:domain c2
    case scmDom1 = "scm-dom1"

    /// scm-dom2: p2 rdfs:domain c ∧ p1 rdfs:subPropertyOf p2 → p1 rdfs:domain c
    case scmDom2 = "scm-dom2"

    /// scm-rng1: p rdfs:range c1 ∧ c1 rdfs:subClassOf c2 → p rdfs:range c2
    case scmRng1 = "scm-rng1"

    /// scm-rng2: p2 rdfs:range c ∧ p1 rdfs:subPropertyOf p2 → p1 rdfs:range c
    case scmRng2 = "scm-rng2"

    /// scm-hv: c1 owl:hasValue y ∧ c1 owl:onProperty p1 ∧ c2 owl:hasValue y ∧ c2 owl:onProperty p2 ∧ p1 rdfs:subPropertyOf p2 → c1 rdfs:subClassOf c2
    case scmHv = "scm-hv"

    /// scm-svf1: c1 owl:someValuesFrom y1 ∧ c1 owl:onProperty p ∧ c2 owl:someValuesFrom y2 ∧ c2 owl:onProperty p ∧ y1 rdfs:subClassOf y2 → c1 rdfs:subClassOf c2
    case scmSvf1 = "scm-svf1"

    /// scm-svf2: (similar for subPropertyOf)
    case scmSvf2 = "scm-svf2"

    /// scm-avf1: c1 owl:allValuesFrom y1 ∧ c1 owl:onProperty p ∧ c2 owl:allValuesFrom y2 ∧ c2 owl:onProperty p ∧ y1 rdfs:subClassOf y2 → c1 rdfs:subClassOf c2
    case scmAvf1 = "scm-avf1"

    /// scm-avf2: (similar for subPropertyOf)
    case scmAvf2 = "scm-avf2"

    /// scm-int: c owl:intersectionOf (c1 ... cn) → c rdfs:subClassOf ci
    case scmInt = "scm-int"

    /// scm-uni: c owl:unionOf (c1 ... cn) → ci rdfs:subClassOf c
    case scmUni = "scm-uni"
}

// MARK: - Rule Metadata

extension OWL2RLRule {
    /// Rule category
    public enum Category: String, Sendable {
        case equality = "eq"
        case property = "prp"
        case classExpression = "cls"
        case classAxiom = "cax"
        case schemaVocabulary = "scm"
    }

    /// Get the category of this rule
    public var category: Category {
        let prefix = rawValue.prefix(while: { $0.isLetter })
        switch prefix {
        case "eq": return .equality
        case "prp": return .property
        case "cls": return .classExpression
        case "cax": return .classAxiom
        case "scm": return .schemaVocabulary
        default: return .property
        }
    }

    /// Whether this rule can produce inconsistency (⊥)
    public var canProduceInconsistency: Bool {
        switch self {
        case .eqDiff1, .prpIrp, .prpAsyp, .prpPdw,
             .clsNothing1, .clsCom, .clsMaxc1, .clsMaxqc1,
             .caxDw:
            return true
        default:
            return false
        }
    }

    /// Whether this rule involves owl:sameAs
    public var involvesSameAs: Bool {
        switch self {
        case .eqRef, .eqSym, .eqTrans, .eqRepS, .eqRepP, .eqRepO, .eqDiff1,
             .prpFp, .prpIfp, .clsMaxc2, .clsMaxqc2:
            return true
        default:
            return false
        }
    }

    /// Whether this rule involves class hierarchy
    public var involvesClassHierarchy: Bool {
        switch self {
        case .caxSco, .caxEqc1, .caxEqc2,
             .scmCls, .scmSco, .scmEqc1, .scmEqc2,
             .scmDom1, .scmRng1, .scmInt, .scmUni:
            return true
        default:
            return false
        }
    }

    /// Whether this rule involves property hierarchy
    public var involvesPropertyHierarchy: Bool {
        switch self {
        case .prpSpo1, .prpEqp1, .prpEqp2,
             .scmOp, .scmDp, .scmSpo, .scmEqp1, .scmEqp2,
             .scmDom2, .scmRng2:
            return true
        default:
            return false
        }
    }

    /// Reasoning strategy for this rule
    public enum Strategy: Sendable {
        /// Materialize at write time
        case materialize

        /// Rewrite query at read time
        case queryRewrite

        /// Use Union-Find (for owl:sameAs)
        case unionFind

        /// Check at consistency validation time
        case consistencyCheck
    }

    /// Recommended reasoning strategy
    public var recommendedStrategy: Strategy {
        switch self {
        // Materialize: hierarchy, inverse, symmetric
        case .caxSco, .caxEqc1, .caxEqc2,
             .prpSpo1, .prpEqp1, .prpEqp2,
             .prpInv1, .prpInv2, .prpSymp,
             .scmSco, .scmSpo, .scmEqc1, .scmEqc2, .scmEqp1, .scmEqp2:
            return .materialize

        // Union-Find: owl:sameAs
        case .eqRef, .eqSym, .eqTrans, .eqRepS, .eqRepP, .eqRepO:
            return .unionFind

        // Query rewrite: transitive, property chains
        case .prpTrp, .prpSpo2:
            return .queryRewrite

        // Consistency check: disjointness, cardinality
        case .eqDiff1, .prpIrp, .prpAsyp, .prpPdw, .prpFp, .prpIfp,
             .clsNothing1, .clsCom, .clsMaxc1, .clsMaxc2, .clsMaxqc1, .clsMaxqc2,
             .caxDw:
            return .consistencyCheck

        // Default to query rewrite for complex rules
        default:
            return .queryRewrite
        }
    }
}

// MARK: - Rule Groups

extension OWL2RLRule {
    /// Rules that should be materialized at write time
    public static var materializationRules: [OWL2RLRule] {
        allCases.filter { $0.recommendedStrategy == .materialize }
    }

    /// Rules that should be applied via query rewriting
    public static var queryRewriteRules: [OWL2RLRule] {
        allCases.filter { $0.recommendedStrategy == .queryRewrite }
    }

    /// Rules that use Union-Find for owl:sameAs
    public static var unionFindRules: [OWL2RLRule] {
        allCases.filter { $0.recommendedStrategy == .unionFind }
    }

    /// Rules for consistency checking
    public static var consistencyRules: [OWL2RLRule] {
        allCases.filter { $0.recommendedStrategy == .consistencyCheck }
    }
}
