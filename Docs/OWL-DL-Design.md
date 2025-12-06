# OWL DL 推論エンジン設計

## 1. OWL DL (SHOIN(D)) 概要

### 1.1 記述論理 SHOIN(D)

OWL DL は記述論理 SHOIN(D) に基づく。決定可能で完全な推論を保証する。

| 記号 | 意味 | OWL 対応 |
|------|------|----------|
| **S** | ALC + 推移的ロール | owl:TransitiveProperty |
| **H** | ロール階層 | rdfs:subPropertyOf |
| **O** | Nominals (列挙) | owl:oneOf, owl:hasValue |
| **I** | 逆ロール | owl:inverseOf |
| **N** | 数量制約 | owl:cardinality, min/maxCardinality |
| **(D)** | データ型 | owl:DatatypeProperty, xsd:* |

**Reference**: Horrocks, I., Patel-Schneider, P. F., & Van Harmelen, F. (2003).
"From SHIQ and RDF to OWL: The making of a web ontology language."
Journal of web semantics, 1(1), 7-26.

### 1.2 OWL DL 構文制限（正規性要件）

OWL DL は決定可能性のために以下の制限を持つ：

1. **推移的ロール制限**: 推移的ロール R は cardinality 制約に使用不可
   - `≤n R.C` / `≥n R.C` で R が transitive → 不正
2. **Simple role 要件**: cardinality 制約には simple role のみ使用可
   - Simple role = 推移的でなく、推移的ロールを含まない
3. **ロール階層の正規性**: role inclusion axiom `R ⊑ S` で R が transitive なら S も transitive
4. **Property chain 制限**: `R₁ ∘ R₂ ∘ ... ∘ Rₙ ⊑ S` は正規表現文法に従う

```swift
/// OWL DL 正規性チェッカー
public struct OWLDLRegularityChecker: Sendable {
    /// 正規性違反
    public enum Violation: Sendable, Equatable {
        case transitiveInCardinality(role: String, axiom: String)
        case nonSimpleRoleInCardinality(role: String, axiom: String)
        case irregularRoleHierarchy(sub: String, sup: String)
        case irregularPropertyChain(chain: [String], target: String)
    }

    /// オントロジーの正規性をチェック
    public func check(_ ontology: OWLOntology) -> [Violation]

    /// ロールが simple か判定
    public func isSimpleRole(_ role: String, in ontology: OWLOntology) -> Bool
}
```

---

## 2. スキーマ定義 (database-kit)

### 2.1 モジュール構成

```
database-kit/Sources/Graph/
├── GraphIndexKind.swift          # 既存
├── GraphIndexStrategy.swift      # 既存
└── Schema/                       # NEW
    ├── OWLOntology.swift         # オントロジー全体
    ├── OWLClass.swift            # クラス・クラス式
    ├── OWLProperty.swift         # オブジェクト/データプロパティ
    ├── OWLAxiom.swift            # 公理（TBox/RBox/ABox）
    ├── OWLDataRange.swift        # データ範囲・リテラル
    └── OWLIndividual.swift       # 個体（Named Individual）
```

### 2.2 OWLOntology

```swift
/// OWL DL オントロジー
///
/// TBox（概念定義）、RBox（ロール定義）、ABox（事実）を含む。
public struct OWLOntology: Sendable, Codable, Hashable {
    /// オントロジー IRI
    public let iri: String

    /// バージョン IRI
    public let versionIRI: String?

    /// インポートするオントロジー
    public var imports: [String] = []

    /// 名前空間プレフィックス
    public var prefixes: [String: String] = [
        "owl": "http://www.w3.org/2002/07/owl#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    ]

    // --- TBox (Terminological Box) ---

    /// 名前付きクラス
    public var classes: [OWLClass] = []

    // --- RBox (Role Box) ---

    /// オブジェクトプロパティ
    public var objectProperties: [OWLObjectProperty] = []

    /// データプロパティ
    public var dataProperties: [OWLDataProperty] = []

    // --- ABox (Assertional Box) ---

    /// 名前付き個体
    public var individuals: [OWLNamedIndividual] = []

    // --- 公理 ---

    /// 全ての公理
    public var axioms: [OWLAxiom] = []

    // --- 便利メソッド ---

    /// TBox 公理のみ取得
    public var tboxAxioms: [OWLAxiom] {
        axioms.filter { $0.isTBoxAxiom }
    }

    /// RBox 公理のみ取得
    public var rboxAxioms: [OWLAxiom] {
        axioms.filter { $0.isRBoxAxiom }
    }

    /// ABox 公理のみ取得
    public var aboxAxioms: [OWLAxiom] {
        axioms.filter { $0.isABoxAxiom }
    }
}
```

### 2.3 OWLClass / OWLClassExpression

```swift
/// OWL 名前付きクラス
public struct OWLClass: Sendable, Codable, Hashable {
    public let iri: String
    public var label: String?
    public var comment: String?
    public var annotations: [String: String] = [:]

    public init(iri: String, label: String? = nil) {
        self.iri = iri
        self.label = label
    }
}

/// クラス式（SHOIN(D) 完全対応）
///
/// **DL 記法対応**:
/// - `⊤` = Thing, `⊥` = Nothing
/// - `C ⊓ D` = intersection, `C ⊔ D` = union, `¬C` = complement
/// - `{a, b}` = oneOf
/// - `∃R.C` = someValuesFrom, `∀R.C` = allValuesFrom
/// - `∃R.{a}` = hasValue
/// - `∃R.Self` = hasSelf
/// - `≥n R.C` = minCardinality, `≤n R.C` = maxCardinality, `=n R.C` = exactCardinality
public indirect enum OWLClassExpression: Sendable, Codable, Hashable {
    // --- 基本 ---
    case named(String)                    // 名前付きクラス
    case thing                            // owl:Thing (⊤)
    case nothing                          // owl:Nothing (⊥)

    // --- Boolean 構成子 ---
    case intersection([OWLClassExpression])   // C ⊓ D
    case union([OWLClassExpression])          // C ⊔ D
    case complement(OWLClassExpression)       // ¬C

    // --- Nominal (O) ---
    case oneOf([String])                  // {a, b, c}

    // --- オブジェクトプロパティ制約 ---
    case someValuesFrom(property: String, filler: OWLClassExpression)  // ∃R.C
    case allValuesFrom(property: String, filler: OWLClassExpression)   // ∀R.C
    case hasValue(property: String, individual: String)                // ∃R.{a}
    case hasSelf(property: String)                                     // ∃R.Self

    // --- 数量制約 (N) ---
    case minCardinality(property: String, n: Int, filler: OWLClassExpression?)  // ≥n R.C
    case maxCardinality(property: String, n: Int, filler: OWLClassExpression?)  // ≤n R.C
    case exactCardinality(property: String, n: Int, filler: OWLClassExpression?) // =n R.C

    // --- データプロパティ制約 (D) ---
    case dataSomeValuesFrom(property: String, range: OWLDataRange)
    case dataAllValuesFrom(property: String, range: OWLDataRange)
    case dataHasValue(property: String, literal: OWLLiteral)
    case dataMinCardinality(property: String, n: Int, range: OWLDataRange?)
    case dataMaxCardinality(property: String, n: Int, range: OWLDataRange?)
    case dataExactCardinality(property: String, n: Int, range: OWLDataRange?)
}

// MARK: - Convenience

extension OWLClassExpression {
    /// NNF (否定標準形) に変換
    public func toNNF() -> OWLClassExpression { ... }

    /// 使用されているロールを取得
    public var usedRoles: Set<String> { ... }

    /// 使用されているクラスを取得
    public var usedClasses: Set<String> { ... }
}
```

### 2.4 OWLProperty

```swift
/// オブジェクトプロパティ（ロール）
public struct OWLObjectProperty: Sendable, Codable, Hashable {
    public let iri: String
    public var label: String?

    // --- ロール特性 (S, I) ---
    public var characteristics: Set<PropertyCharacteristic> = []

    // --- 逆ロール (I) ---
    public var inverseOf: String?

    // --- ドメイン/レンジ ---
    public var domains: [OWLClassExpression] = []
    public var ranges: [OWLClassExpression] = []

    // --- ロール階層 (H) ---
    public var superProperties: [String] = []

    public init(iri: String, label: String? = nil) {
        self.iri = iri
        self.label = label
    }
}

/// プロパティ特性
public enum PropertyCharacteristic: String, Sendable, Codable, CaseIterable {
    // --- 基本特性 ---
    case functional           // owl:FunctionalProperty: R(x,y) ∧ R(x,z) → y=z
    case inverseFunctional    // owl:InverseFunctionalProperty: R(x,z) ∧ R(y,z) → x=y

    // --- 対称性 ---
    case symmetric            // owl:SymmetricProperty: R(x,y) → R(y,x)
    case asymmetric           // owl:AsymmetricProperty: R(x,y) → ¬R(y,x)

    // --- 推移性 (S) ---
    case transitive           // owl:TransitiveProperty: R(x,y) ∧ R(y,z) → R(x,z)

    // --- 反射性 ---
    case reflexive            // owl:ReflexiveProperty: ∀x. R(x,x)
    case irreflexive          // owl:IrreflexiveProperty: ∀x. ¬R(x,x)
}

/// データプロパティ
public struct OWLDataProperty: Sendable, Codable, Hashable {
    public let iri: String
    public var label: String?
    public var domains: [OWLClassExpression] = []
    public var ranges: [OWLDataRange] = []
    public var isFunctional: Bool = false
    public var superProperties: [String] = []
}
```

### 2.5 OWLAxiom

```swift
/// OWL 公理
///
/// TBox（概念関係）、RBox（ロール関係）、ABox（事実）を表現。
public enum OWLAxiom: Sendable, Codable, Hashable {

    // ========================================
    // TBox 公理（概念関係）
    // ========================================

    /// C ⊑ D (サブクラス)
    case subClassOf(sub: OWLClassExpression, sup: OWLClassExpression)

    /// C ≡ D (等価クラス)
    case equivalentClasses([OWLClassExpression])

    /// disjoint(C, D) (排他的クラス)
    case disjointClasses([OWLClassExpression])

    /// C ≡ D₁ ⊔ D₂ ⊔ ... かつ disjoint(D₁, D₂, ...)
    case disjointUnion(class_: String, disjuncts: [OWLClassExpression])

    // ========================================
    // RBox 公理（ロール関係）
    // ========================================

    /// R ⊑ S (サブプロパティ)
    case subObjectPropertyOf(sub: String, sup: String)

    /// R₁ ∘ R₂ ∘ ... ∘ Rₙ ⊑ S (プロパティチェーン) [OWL 2]
    case subPropertyChainOf(chain: [String], sup: String)

    /// R ≡ S (等価プロパティ)
    case equivalentObjectProperties([String])

    /// disjoint(R, S) (排他的プロパティ)
    case disjointObjectProperties([String])

    /// R⁻ ≡ S (逆プロパティ)
    case inverseObjectProperties(first: String, second: String)

    /// domain(R) = C
    case objectPropertyDomain(property: String, domain: OWLClassExpression)

    /// range(R) = C
    case objectPropertyRange(property: String, range: OWLClassExpression)

    // --- ロール特性宣言 ---
    case functionalObjectProperty(String)
    case inverseFunctionalObjectProperty(String)
    case transitiveObjectProperty(String)
    case symmetricObjectProperty(String)
    case asymmetricObjectProperty(String)
    case reflexiveObjectProperty(String)
    case irreflexiveObjectProperty(String)

    // --- データプロパティ公理 ---
    case subDataPropertyOf(sub: String, sup: String)
    case equivalentDataProperties([String])
    case disjointDataProperties([String])
    case dataPropertyDomain(property: String, domain: OWLClassExpression)
    case dataPropertyRange(property: String, range: OWLDataRange)
    case functionalDataProperty(String)

    // ========================================
    // ABox 公理（個体に関する事実）
    // ========================================

    /// a : C (クラスアサーション)
    case classAssertion(individual: String, class_: OWLClassExpression)

    /// R(a, b) (オブジェクトプロパティアサーション)
    case objectPropertyAssertion(subject: String, property: String, object: String)

    /// ¬R(a, b) (否定オブジェクトプロパティアサーション)
    case negativeObjectPropertyAssertion(subject: String, property: String, object: String)

    /// T(a, v) (データプロパティアサーション)
    case dataPropertyAssertion(subject: String, property: String, value: OWLLiteral)

    /// ¬T(a, v) (否定データプロパティアサーション)
    case negativeDataPropertyAssertion(subject: String, property: String, value: OWLLiteral)

    /// a = b (同一個体)
    case sameIndividual([String])

    /// a ≠ b (異なる個体)
    case differentIndividuals([String])

    // ========================================
    // 分類ヘルパー
    // ========================================

    public var isTBoxAxiom: Bool {
        switch self {
        case .subClassOf, .equivalentClasses, .disjointClasses, .disjointUnion:
            return true
        default:
            return false
        }
    }

    public var isRBoxAxiom: Bool {
        switch self {
        case .subObjectPropertyOf, .subPropertyChainOf, .equivalentObjectProperties,
             .disjointObjectProperties, .inverseObjectProperties,
             .objectPropertyDomain, .objectPropertyRange,
             .functionalObjectProperty, .inverseFunctionalObjectProperty,
             .transitiveObjectProperty, .symmetricObjectProperty,
             .asymmetricObjectProperty, .reflexiveObjectProperty, .irreflexiveObjectProperty,
             .subDataPropertyOf, .equivalentDataProperties, .disjointDataProperties,
             .dataPropertyDomain, .dataPropertyRange, .functionalDataProperty:
            return true
        default:
            return false
        }
    }

    public var isABoxAxiom: Bool {
        switch self {
        case .classAssertion, .objectPropertyAssertion, .negativeObjectPropertyAssertion,
             .dataPropertyAssertion, .negativeDataPropertyAssertion,
             .sameIndividual, .differentIndividuals:
            return true
        default:
            return false
        }
    }
}
```

### 2.6 OWLDataRange / OWLLiteral

```swift
/// データ範囲
public indirect enum OWLDataRange: Sendable, Codable, Hashable {
    /// 基本データ型 (xsd:string, xsd:integer, xsd:boolean, etc.)
    case datatype(String)

    /// データ範囲の交差
    case dataIntersectionOf([OWLDataRange])

    /// データ範囲の和集合
    case dataUnionOf([OWLDataRange])

    /// データ範囲の補集合
    case dataComplementOf(OWLDataRange)

    /// リテラル列挙
    case dataOneOf([OWLLiteral])

    /// ファセット制約付きデータ型
    case datatypeRestriction(datatype: String, facets: [FacetRestriction])
}

/// XSD ファセット制約
public struct FacetRestriction: Sendable, Codable, Hashable {
    public let facet: XSDFacet
    public let value: OWLLiteral
}

/// XSD ファセット種別
public enum XSDFacet: String, Sendable, Codable, CaseIterable {
    case minInclusive = "xsd:minInclusive"
    case maxInclusive = "xsd:maxInclusive"
    case minExclusive = "xsd:minExclusive"
    case maxExclusive = "xsd:maxExclusive"
    case length = "xsd:length"
    case minLength = "xsd:minLength"
    case maxLength = "xsd:maxLength"
    case pattern = "xsd:pattern"
    case totalDigits = "xsd:totalDigits"
    case fractionDigits = "xsd:fractionDigits"
}

/// リテラル値
public struct OWLLiteral: Sendable, Codable, Hashable {
    /// 字句表現
    public let lexicalForm: String

    /// データ型 IRI (xsd:string, xsd:integer, etc.)
    public let datatype: String

    /// 言語タグ (rdf:langString の場合)
    public let language: String?

    // --- 便利イニシャライザ ---

    public static func string(_ value: String) -> OWLLiteral {
        OWLLiteral(lexicalForm: value, datatype: "xsd:string", language: nil)
    }

    public static func integer(_ value: Int) -> OWLLiteral {
        OWLLiteral(lexicalForm: String(value), datatype: "xsd:integer", language: nil)
    }

    public static func decimal(_ value: Double) -> OWLLiteral {
        OWLLiteral(lexicalForm: String(value), datatype: "xsd:decimal", language: nil)
    }

    public static func boolean(_ value: Bool) -> OWLLiteral {
        OWLLiteral(lexicalForm: value ? "true" : "false", datatype: "xsd:boolean", language: nil)
    }

    public static func langString(_ value: String, language: String) -> OWLLiteral {
        OWLLiteral(lexicalForm: value, datatype: "rdf:langString", language: language)
    }
}

/// 名前付き個体
public struct OWLNamedIndividual: Sendable, Codable, Hashable {
    public let iri: String
    public var label: String?
    public var annotations: [String: String] = [:]
}
```

### 2.7 データ型検証器

```swift
/// XSD データ型検証器
///
/// リテラルの型適合性とファセット制約を検証。
///
/// **Reference**: W3C XML Schema Part 2: Datatypes
/// https://www.w3.org/TR/xmlschema-2/
public struct OWLDatatypeValidator: Sendable {

    /// 検証エラー
    public enum ValidationError: Error, Sendable, Equatable {
        case invalidLexicalForm(literal: OWLLiteral, expected: String)
        case facetViolation(literal: OWLLiteral, facet: XSDFacet, constraint: String)
        case unknownDatatype(String)
        case incompatibleTypes(OWLLiteral, OWLDataRange)
    }

    /// サポートするデータ型
    public static let supportedDatatypes: Set<String> = [
        "xsd:string", "xsd:boolean", "xsd:decimal", "xsd:integer",
        "xsd:float", "xsd:double", "xsd:date", "xsd:dateTime",
        "xsd:time", "xsd:duration", "xsd:anyURI", "xsd:base64Binary",
        "xsd:hexBinary", "xsd:normalizedString", "xsd:token",
        "xsd:language", "xsd:NMTOKEN", "xsd:Name", "xsd:NCName",
        "xsd:nonPositiveInteger", "xsd:negativeInteger",
        "xsd:nonNegativeInteger", "xsd:positiveInteger",
        "xsd:long", "xsd:int", "xsd:short", "xsd:byte",
        "xsd:unsignedLong", "xsd:unsignedInt", "xsd:unsignedShort", "xsd:unsignedByte",
        "rdf:langString", "rdf:PlainLiteral"
    ]

    /// リテラルの字句形式を検証
    public func validateLexicalForm(_ literal: OWLLiteral) -> ValidationError?

    /// リテラルがデータ範囲に適合するか検証
    public func validate(_ literal: OWLLiteral, against range: OWLDataRange) -> ValidationError?

    /// ファセット制約を検証
    public func validateFacets(_ literal: OWLLiteral, facets: [FacetRestriction]) -> ValidationError?

    /// 2つのリテラルの比較（順序付きデータ型用）
    public func compare(_ lhs: OWLLiteral, _ rhs: OWLLiteral) -> ComparisonResult?
}
```

---

## 3. 推論エンジン (database-framework)

### 3.1 モジュール構成

```
database-framework/Sources/GraphIndex/
├── GraphIndexMaintainer.swift    # 既存
├── GraphQueryBuilder.swift       # 既存
└── Reasoning/                    # NEW
    ├── OWLReasoner.swift         # メイン推論エンジン
    ├── TableauxReasoner.swift    # Tableaux アルゴリズム (SHOIN(D))
    ├── RoleHierarchy.swift       # ロール階層管理
    ├── ClassHierarchy.swift      # クラス階層キャッシュ
    ├── RuleEngine.swift          # RETE ベースルール推論
    ├── Materializer.swift        # 推論結果永続化
    ├── ConsistencyChecker.swift  # 一貫性検証
    ├── DatatypeValidator.swift   # データ型検証
    └── RegularityChecker.swift   # OWL DL 正規性検証
```

### 3.2 OWLReasoner

```swift
/// OWL DL 推論エンジン
///
/// SHOIN(D) 完全対応のハイブリッド推論エンジン。
/// Tableaux による整合性判定と RETE によるマテリアライズを併用。
///
/// **Reference**: Motik, B., Shearer, R., & Horrocks, I. (2009).
/// "Hypertableau reasoning for description logics."
/// Journal of Artificial Intelligence Research, 36, 165-228.
public final class OWLReasoner: Sendable {

    // MARK: - Properties

    nonisolated(unsafe) private let database: any DatabaseProtocol
    private let ontology: OWLOntology
    private let subspace: Subspace

    // 階層キャッシュ
    private let classHierarchy: ClassHierarchy
    private let roleHierarchy: RoleHierarchy

    // サブコンポーネント
    private let tableaux: TableauxReasoner
    private let ruleEngine: RuleEngine
    private let datatypeValidator: OWLDatatypeValidator

    // MARK: - Initialization

    public init(
        database: any DatabaseProtocol,
        ontology: OWLOntology,
        subspace: Subspace
    ) async throws {
        // 正規性チェック
        let regularityChecker = OWLDLRegularityChecker()
        let violations = regularityChecker.check(ontology)
        if !violations.isEmpty {
            throw OWLReasonerError.regularityViolation(violations)
        }

        self.database = database
        self.ontology = ontology
        self.subspace = subspace

        // 階層構築
        self.classHierarchy = try await ClassHierarchy.build(from: ontology)
        self.roleHierarchy = try await RoleHierarchy.build(from: ontology)

        // サブコンポーネント初期化
        self.tableaux = TableauxReasoner(ontology: ontology, roleHierarchy: roleHierarchy)
        self.ruleEngine = RuleEngine(ontology: ontology)
        self.datatypeValidator = OWLDatatypeValidator()
    }

    // MARK: - Standard Reasoning Tasks

    /// オントロジーの一貫性チェック
    public func isConsistent() async throws -> Bool

    /// クラスの充足可能性チェック
    public func isSatisfiable(_ class_: OWLClassExpression) async throws -> Bool

    /// サブクラス判定: C ⊑ D
    public func isSubClassOf(_ sub: OWLClassExpression, _ sup: OWLClassExpression) async throws -> Bool

    /// インスタンス判定: a : C
    public func isInstanceOf(_ individual: String, class_: OWLClassExpression) async throws -> Bool

    /// 個体の全タイプ取得
    public func getTypes(_ individual: String, direct: Bool) async throws -> Set<OWLClassExpression>

    /// クラスの全インスタンス取得
    public func getInstances(_ class_: OWLClassExpression, direct: Bool) async throws -> Set<String>

    /// プロパティ値取得（推論込み）
    public func getObjectPropertyValues(
        subject: String,
        property: String
    ) async throws -> Set<String>

    /// 同一個体判定
    public func isSameIndividual(_ a: String, _ b: String) async throws -> Bool

    // MARK: - Materialization

    /// 全マテリアライズ
    public func materialize() async throws -> MaterializationResult

    /// 増分マテリアライズ
    public func materializeIncremental(
        added: [OWLAxiom],
        removed: [OWLAxiom]
    ) async throws -> MaterializationResult
}
```

### 3.3 TableauxReasoner (SHOIN(D) 完全対応)

```swift
/// SHOIN(D) Tableaux 推論器
///
/// **アルゴリズム概要**:
/// 1. 入力: クラス式 C
/// 2. NNF 変換: C → C_nnf
/// 3. Tableau 初期化: 新しい個体 x₀ に C_nnf をラベル付け
/// 4. 飽和: 適用可能なルールがなくなるまで展開
/// 5. 結果: clash がなければ充足可能
///
/// **Reference**: Baader, F., et al. (2003).
/// "The Description Logic Handbook", Chapter 2.
public struct TableauxReasoner: Sendable {

    // MARK: - Types

    /// Tableau ノード
    struct Node: Sendable {
        let id: String
        var labels: Set<OWLClassExpression>
        var blockingCandidate: String?
        var isBlocked: Bool = false
        var isRootNode: Bool = false
    }

    /// Tableau エッジ
    struct Edge: Sendable {
        let from: String
        let to: String
        let role: String
    }

    /// Tableau 状態
    struct Tableau: Sendable {
        var nodes: [String: Node] = [:]
        var edges: [Edge] = []
        var inequalities: Set<Pair<String>> = []  // x ≠ y
        var clashInfo: ClashInfo?
        private var freshCounter: Int = 0

        mutating func freshIndividual() -> String {
            freshCounter += 1
            return "_:b\(freshCounter)"
        }
    }

    /// 矛盾情報
    struct ClashInfo: Sendable {
        let type: ClashType
        let node: String
        let detail: String
    }

    enum ClashType: String, Sendable {
        case bottomClass           // ⊥ ∈ L(x)
        case directContradiction   // C ∈ L(x) ∧ ¬C ∈ L(x)
        case disjointViolation     // C ∈ L(x) ∧ D ∈ L(x) ∧ disjoint(C,D)
        case cardinalityClash      // ≤n R と実際の successor 数の矛盾
        case inequalityClash       // x ≠ x
        case datatypeClash         // データ型制約違反
    }

    /// 展開ルール
    enum ExpansionRule: Sendable {
        case intersection(node: String, expr: OWLClassExpression)  // ⊓-rule
        case union(node: String, expr: OWLClassExpression)         // ⊔-rule (非決定的)
        case existential(node: String, expr: OWLClassExpression)   // ∃-rule
        case universal(node: String, successor: String, expr: OWLClassExpression)  // ∀-rule
        case atLeast(node: String, expr: OWLClassExpression)       // ≥-rule
        case atMost(node: String, expr: OWLClassExpression)        // ≤-rule (マージ)
        case choose(node: String, expr: OWLClassExpression)        // choose-rule (qualified)
        case nominal(node: String, individual: String)             // o-rule
        case selfRestriction(node: String, role: String)           // Self-rule
    }

    // MARK: - Properties

    let ontology: OWLOntology
    let roleHierarchy: RoleHierarchy

    // MARK: - Main Algorithm

    /// 充足可能性チェック
    func isSatisfiable(_ expression: OWLClassExpression) -> TableauxResult {
        // 1. NNF 変換
        let nnf = expression.toNNF()

        // 2. Tableau 初期化
        var tableau = Tableau()
        let root = tableau.freshIndividual()
        tableau.nodes[root] = Node(id: root, labels: [nnf], isRootNode: true)

        // 3. 飽和ループ
        return saturate(&tableau)
    }

    /// サブクラス判定: C ⊑ D ⟺ C ⊓ ¬D は充足不能
    func isSubClassOf(_ sub: OWLClassExpression, _ sup: OWLClassExpression) -> Bool {
        let test = OWLClassExpression.intersection([sub, .complement(sup)])
        return !isSatisfiable(test).isSatisfiable
    }

    /// 飽和アルゴリズム
    private func saturate(_ tableau: inout Tableau) -> TableauxResult {
        var statistics = Statistics()

        while let rule = selectApplicableRule(tableau) {
            statistics.rulesApplied += 1

            switch rule {
            case .intersection(let node, let expr):
                applyIntersectionRule(&tableau, node: node, expr: expr)

            case .union(let node, let expr):
                // 非決定的: バックトラッキング
                if let result = applyUnionRuleWithBacktrack(&tableau, node: node, expr: expr, stats: &statistics) {
                    return result
                }

            case .existential(let node, let expr):
                statistics.nodesCreated += 1
                applyExistentialRule(&tableau, node: node, expr: expr)

            case .universal(let node, let successor, let expr):
                applyUniversalRule(&tableau, node: node, successor: successor, expr: expr)

            case .atLeast(let node, let expr):
                applyAtLeastRule(&tableau, node: node, expr: expr, stats: &statistics)

            case .atMost(let node, let expr):
                // 非決定的: マージ候補の選択
                if let result = applyAtMostRuleWithBacktrack(&tableau, node: node, expr: expr, stats: &statistics) {
                    return result
                }

            case .choose(let node, let expr):
                applyChooseRule(&tableau, node: node, expr: expr)

            case .nominal(let node, let individual):
                applyNominalRule(&tableau, node: node, individual: individual)

            case .selfRestriction(let node, let role):
                applySelfRule(&tableau, node: node, role: role)
            }

            // 矛盾チェック
            if let clash = detectClash(tableau) {
                tableau.clashInfo = clash
                return TableauxResult(
                    isSatisfiable: false,
                    clashInfo: clash,
                    statistics: statistics
                )
            }

            // ブロッキング更新
            updateBlocking(&tableau)
        }

        return TableauxResult(
            isSatisfiable: true,
            clashInfo: nil,
            statistics: statistics
        )
    }

    // MARK: - Expansion Rules Implementation

    /// ⊓-rule: C ⊓ D ∈ L(x), {C,D} ⊄ L(x) ⟹ L(x) := L(x) ∪ {C, D}
    private func applyIntersectionRule(_ tableau: inout Tableau, node: String, expr: OWLClassExpression) {
        guard case .intersection(let operands) = expr else { return }
        tableau.nodes[node]?.labels.formUnion(operands)
    }

    /// ⊔-rule: C ⊔ D ∈ L(x), {C,D} ∩ L(x) = ∅ ⟹ L(x) := L(x) ∪ {C} or L(x) := L(x) ∪ {D}
    private func applyUnionRuleWithBacktrack(
        _ tableau: inout Tableau,
        node: String,
        expr: OWLClassExpression,
        stats: inout Statistics
    ) -> TableauxResult? {
        guard case .union(let operands) = expr else { return nil }

        // 各選択肢を試行
        for operand in operands {
            var branch = tableau
            branch.nodes[node]?.labels.insert(operand)

            let result = saturate(&branch)
            if result.isSatisfiable {
                tableau = branch
                return nil  // 継続
            }
            stats.backtracks += 1
        }

        // 全ての選択肢で矛盾
        return TableauxResult(
            isSatisfiable: false,
            clashInfo: ClashInfo(type: .directContradiction, node: node, detail: "All union branches clash"),
            statistics: stats
        )
    }

    /// ∃-rule: ∃R.C ∈ L(x), x has no R-successor y with C ∈ L(y)
    ///         ⟹ create new y, add edge xRy, L(y) := {C}
    private func applyExistentialRule(_ tableau: inout Tableau, node: String, expr: OWLClassExpression) {
        guard case .someValuesFrom(let role, let filler) = expr else { return }

        // 既存の適切な successor があるかチェック
        let successors = getSuccessors(node, role: role, in: tableau)
        for succ in successors {
            if tableau.nodes[succ]?.labels.contains(filler) == true {
                return  // 既に存在
            }
        }

        // 新しい successor を作成
        let newNode = tableau.freshIndividual()
        tableau.nodes[newNode] = Node(id: newNode, labels: [filler])
        tableau.edges.append(Edge(from: node, to: newNode, role: role))

        // 逆ロールも追加（ロール階層考慮）
        if let inverse = roleHierarchy.inverseOf(role) {
            tableau.edges.append(Edge(from: newNode, to: node, role: inverse))
        }
    }

    /// ∀-rule: ∀R.C ∈ L(x), xRy, C ∉ L(y) ⟹ L(y) := L(y) ∪ {C}
    private func applyUniversalRule(_ tableau: inout Tableau, node: String, successor: String, expr: OWLClassExpression) {
        guard case .allValuesFrom(_, let filler) = expr else { return }
        tableau.nodes[successor]?.labels.insert(filler)
    }

    /// ≥-rule: ≥n R.C ∈ L(x), x has fewer than n R-successors with C
    ///         ⟹ create n new successors with C, all pairwise different
    private func applyAtLeastRule(_ tableau: inout Tableau, node: String, expr: OWLClassExpression, stats: inout Statistics) {
        guard case .minCardinality(let role, let n, let filler) = expr else { return }

        let qualifiedSuccessors = getQualifiedSuccessors(node, role: role, filler: filler, in: tableau)
        let deficit = n - qualifiedSuccessors.count

        if deficit <= 0 { return }

        var newNodes: [String] = []
        for _ in 0..<deficit {
            let newNode = tableau.freshIndividual()
            var labels: Set<OWLClassExpression> = []
            if let f = filler { labels.insert(f) }
            tableau.nodes[newNode] = Node(id: newNode, labels: labels)
            tableau.edges.append(Edge(from: node, to: newNode, role: role))
            newNodes.append(newNode)
            stats.nodesCreated += 1
        }

        // 全ての新ノードは互いに異なる
        for i in 0..<newNodes.count {
            for j in (i+1)..<newNodes.count {
                tableau.inequalities.insert(Pair(newNodes[i], newNodes[j]))
            }
        }
    }

    /// ≤-rule: ≤n R.C ∈ L(x), x has more than n R-successors with C
    ///         ⟹ merge two successors (non-deterministic)
    private func applyAtMostRuleWithBacktrack(
        _ tableau: inout Tableau,
        node: String,
        expr: OWLClassExpression,
        stats: inout Statistics
    ) -> TableauxResult? {
        guard case .maxCardinality(let role, let n, let filler) = expr else { return nil }

        let qualifiedSuccessors = getQualifiedSuccessors(node, role: role, filler: filler, in: tableau)
        if qualifiedSuccessors.count <= n { return nil }

        // マージ候補ペアを試行
        for i in 0..<qualifiedSuccessors.count {
            for j in (i+1)..<qualifiedSuccessors.count {
                let s1 = qualifiedSuccessors[i]
                let s2 = qualifiedSuccessors[j]

                // 不等式制約に違反しないかチェック
                if tableau.inequalities.contains(Pair(s1, s2)) {
                    continue  // マージ不可
                }

                var branch = tableau
                merge(&branch, s1, into: s2)

                let result = saturate(&branch)
                if result.isSatisfiable {
                    tableau = branch
                    return nil
                }
                stats.backtracks += 1
            }
        }

        // 全てのマージで矛盾
        return TableauxResult(
            isSatisfiable: false,
            clashInfo: ClashInfo(type: .cardinalityClash, node: node, detail: "Cannot satisfy ≤\(n) \(role)"),
            statistics: stats
        )
    }

    // MARK: - Blocking (Pairwise Blocking for SHOIN)

    /// ブロッキング更新
    ///
    /// Pairwise blocking: ノード y が祖先 x によりブロックされる条件:
    /// 1. L(y) ⊆ L(x)
    /// 2. 全ての y の親 y' と x の親 x' について:
    ///    y'Ry かつ x'Rx (同じロール) ならば L(y') ⊆ L(x')
    private func updateBlocking(_ tableau: inout Tableau) {
        for (nodeId, node) in tableau.nodes where !node.isRootNode {
            tableau.nodes[nodeId]?.isBlocked = false

            for ancestor in getAncestors(nodeId, in: tableau) {
                if pairwiseBlocks(blocker: ancestor, blocked: nodeId, in: tableau) {
                    tableau.nodes[nodeId]?.isBlocked = true
                    tableau.nodes[nodeId]?.blockingCandidate = ancestor
                    break
                }
            }
        }
    }

    private func pairwiseBlocks(blocker: String, blocked: String, in tableau: Tableau) -> Bool {
        guard let blockerNode = tableau.nodes[blocker],
              let blockedNode = tableau.nodes[blocked] else { return false }

        // L(blocked) ⊆ L(blocker)
        guard blockedNode.labels.isSubset(of: blockerNode.labels) else { return false }

        // 親ノードのラベルもチェック
        let blockedParents = getParents(blocked, in: tableau)
        let blockerParents = getParents(blocker, in: tableau)

        for (blockedParent, role) in blockedParents {
            var found = false
            for (blockerParent, role2) in blockerParents where role == role2 {
                if let bp1 = tableau.nodes[blockedParent],
                   let bp2 = tableau.nodes[blockerParent],
                   bp1.labels.isSubset(of: bp2.labels) {
                    found = true
                    break
                }
            }
            if !found { return false }
        }

        return true
    }

    // MARK: - Clash Detection

    private func detectClash(_ tableau: Tableau) -> ClashInfo? {
        for (nodeId, node) in tableau.nodes {
            // 1. ⊥ チェック
            if node.labels.contains(.nothing) {
                return ClashInfo(type: .bottomClass, node: nodeId, detail: "⊥ in label")
            }

            // 2. C と ¬C
            for label in node.labels {
                if node.labels.contains(.complement(label)) {
                    return ClashInfo(type: .directContradiction, node: nodeId, detail: "\(label) and ¬\(label)")
                }
            }

            // 3. Disjoint クラス違反
            for axiom in ontology.axioms {
                if case .disjointClasses(let classes) = axiom {
                    let presentClasses = classes.filter { node.labels.contains($0) }
                    if presentClasses.count >= 2 {
                        return ClashInfo(type: .disjointViolation, node: nodeId, detail: "disjoint classes: \(presentClasses)")
                    }
                }
            }
        }

        // 4. 不等式違反
        for pair in tableau.inequalities {
            if pair.first == pair.second {
                return ClashInfo(type: .inequalityClash, node: pair.first, detail: "\(pair.first) ≠ \(pair.first)")
            }
        }

        return nil
    }

    // MARK: - Helper Types

    struct Pair<T: Hashable>: Hashable {
        let first: T
        let second: T

        init(_ a: T, _ b: T) {
            // 正規化して順序を固定
            if "\(a)" < "\(b)" {
                self.first = a
                self.second = b
            } else {
                self.first = b
                self.second = a
            }
        }
    }

    struct Statistics: Sendable {
        var nodesCreated: Int = 0
        var edgesCreated: Int = 0
        var rulesApplied: Int = 0
        var backtracks: Int = 0
    }

    struct TableauxResult: Sendable {
        let isSatisfiable: Bool
        let clashInfo: ClashInfo?
        let statistics: Statistics
    }
}
```

### 3.4 RoleHierarchy

```swift
/// ロール階層管理
///
/// ロールの上位関係、逆関係、プロパティチェーンを管理。
public struct RoleHierarchy: Sendable {

    /// R ⊑ S の関係（推移閉包）
    private var superRoles: [String: Set<String>]

    /// R⁻ の関係
    private var inverses: [String: String]

    /// プロパティチェーン: R₁ ∘ R₂ ∘ ... ⊑ S
    private var propertyChains: [(chain: [String], target: String)]

    /// ロール特性
    private var characteristics: [String: Set<PropertyCharacteristic>]

    /// オントロジーから構築
    public static func build(from ontology: OWLOntology) async throws -> RoleHierarchy

    /// R の全上位ロール（R 自身を含む）
    public func superRolesOf(_ role: String) -> Set<String>

    /// R の逆ロール
    public func inverseOf(_ role: String) -> String?

    /// R が simple role か（cardinality 制約に使用可能か）
    public func isSimple(_ role: String) -> Bool

    /// R が推移的か
    public func isTransitive(_ role: String) -> Bool

    /// R が対称的か
    public func isSymmetric(_ role: String) -> Bool

    /// R が機能的か
    public func isFunctional(_ role: String) -> Bool

    /// xRy から導出される全関係（ロール階層、逆、推移性考慮）
    public func derivedRelations(from: String, to: String, role: String) -> [(from: String, to: String, role: String)]
}
```

### 3.5 RuleEngine (RETE)

```swift
/// RETE ベースルールエンジン
///
/// Forward chaining による効率的なマテリアライズ。
///
/// **Reference**: Forgy, C. L. (1982).
/// "Rete: A fast algorithm for the many pattern/many object pattern match problem."
public struct RuleEngine: Sendable {

    /// 推論ルール
    public struct Rule: Sendable {
        let id: String
        let name: String
        let conditions: [Condition]
        let consequences: [Consequence]
        let priority: Int
    }

    /// 条件パターン
    public enum Condition: Sendable {
        case triple(subject: Pattern, predicate: Pattern, object: Pattern)
        case classAssertion(individual: Pattern, class_: OWLClassExpression)
        case propertyCharacteristic(property: Pattern, characteristic: PropertyCharacteristic)
        case axiom(OWLAxiom)
        case negation(Condition)
    }

    /// 結果アクション
    public enum Consequence: Sendable {
        case assertTriple(subject: Pattern, predicate: Pattern, object: Pattern)
        case assertClassMembership(individual: Pattern, class_: OWLClassExpression)
        case assertSameAs(Pattern, Pattern)
    }

    /// パターン変数
    public enum Pattern: Sendable, Hashable {
        case variable(String)
        case constant(String)
    }

    /// 標準 RDFS/OWL ルール
    public static let standardRules: [Rule] = [
        // rdfs:subClassOf
        Rule(
            id: "rdfs9",
            name: "rdfs-subClassOf",
            conditions: [
                .classAssertion(individual: .variable("?x"), class_: .named("?C")),
                .axiom(.subClassOf(sub: .named("?C"), sup: .named("?D")))
            ],
            consequences: [
                .assertClassMembership(individual: .variable("?x"), class_: .named("?D"))
            ],
            priority: 100
        ),

        // rdfs:domain
        Rule(
            id: "rdfs2",
            name: "rdfs-domain",
            conditions: [
                .triple(subject: .variable("?x"), predicate: .variable("?P"), object: .variable("?y")),
                .axiom(.objectPropertyDomain(property: "?P", domain: .named("?C")))
            ],
            consequences: [
                .assertClassMembership(individual: .variable("?x"), class_: .named("?C"))
            ],
            priority: 90
        ),

        // rdfs:range
        Rule(
            id: "rdfs3",
            name: "rdfs-range",
            conditions: [
                .triple(subject: .variable("?x"), predicate: .variable("?P"), object: .variable("?y")),
                .axiom(.objectPropertyRange(property: "?P", range: .named("?C")))
            ],
            consequences: [
                .assertClassMembership(individual: .variable("?y"), class_: .named("?C"))
            ],
            priority: 90
        ),

        // owl:TransitiveProperty
        Rule(
            id: "prp-trp",
            name: "owl-transitive",
            conditions: [
                .triple(subject: .variable("?x"), predicate: .variable("?P"), object: .variable("?y")),
                .triple(subject: .variable("?y"), predicate: .variable("?P"), object: .variable("?z")),
                .propertyCharacteristic(property: .variable("?P"), characteristic: .transitive)
            ],
            consequences: [
                .assertTriple(subject: .variable("?x"), predicate: .variable("?P"), object: .variable("?z"))
            ],
            priority: 80
        ),

        // owl:SymmetricProperty
        Rule(
            id: "prp-symp",
            name: "owl-symmetric",
            conditions: [
                .triple(subject: .variable("?x"), predicate: .variable("?P"), object: .variable("?y")),
                .propertyCharacteristic(property: .variable("?P"), characteristic: .symmetric)
            ],
            consequences: [
                .assertTriple(subject: .variable("?y"), predicate: .variable("?P"), object: .variable("?x"))
            ],
            priority: 80
        ),

        // owl:inverseOf
        Rule(
            id: "prp-inv",
            name: "owl-inverse",
            conditions: [
                .triple(subject: .variable("?x"), predicate: .variable("?P"), object: .variable("?y")),
                .axiom(.inverseObjectProperties(first: "?P", second: "?Q"))
            ],
            consequences: [
                .assertTriple(subject: .variable("?y"), predicate: .variable("?Q"), object: .variable("?x"))
            ],
            priority: 80
        ),

        // owl:sameAs transitivity
        Rule(
            id: "eq-trans",
            name: "owl-sameAs-trans",
            conditions: [
                .axiom(.sameIndividual(["?x", "?y"])),
                .axiom(.sameIndividual(["?y", "?z"]))
            ],
            consequences: [
                .assertSameAs(.variable("?x"), .variable("?z"))
            ],
            priority: 70
        ),

        // owl:FunctionalProperty → sameAs
        Rule(
            id: "prp-fp",
            name: "owl-functional-sameAs",
            conditions: [
                .triple(subject: .variable("?x"), predicate: .variable("?P"), object: .variable("?y1")),
                .triple(subject: .variable("?x"), predicate: .variable("?P"), object: .variable("?y2")),
                .propertyCharacteristic(property: .variable("?P"), characteristic: .functional)
            ],
            consequences: [
                .assertSameAs(.variable("?y1"), .variable("?y2"))
            ],
            priority: 60
        ),

        // owl:InverseFunctionalProperty → sameAs
        Rule(
            id: "prp-ifp",
            name: "owl-inverseFunctional-sameAs",
            conditions: [
                .triple(subject: .variable("?x1"), predicate: .variable("?P"), object: .variable("?y")),
                .triple(subject: .variable("?x2"), predicate: .variable("?P"), object: .variable("?y")),
                .propertyCharacteristic(property: .variable("?P"), characteristic: .inverseFunctional)
            ],
            consequences: [
                .assertSameAs(.variable("?x1"), .variable("?x2"))
            ],
            priority: 60
        ),

        // rdfs:subPropertyOf
        Rule(
            id: "rdfs7",
            name: "rdfs-subPropertyOf",
            conditions: [
                .triple(subject: .variable("?x"), predicate: .variable("?P"), object: .variable("?y")),
                .axiom(.subObjectPropertyOf(sub: "?P", sup: "?Q"))
            ],
            consequences: [
                .assertTriple(subject: .variable("?x"), predicate: .variable("?Q"), object: .variable("?y"))
            ],
            priority: 85
        )
    ]

    /// RETE ネットワーク
    private var network: RETENetwork

    /// ルールエンジン初期化
    public init(ontology: OWLOntology, customRules: [Rule] = [])

    /// ファクト追加による増分推論
    public mutating func addFacts(_ facts: [Fact]) -> [Fact]

    /// ファクト削除による増分推論（DRed）
    public mutating func removeFacts(_ facts: [Fact]) -> (removed: [Fact], rederived: [Fact])

    /// 飽和（全ルール適用）
    public mutating func saturate() -> [Fact]
}
```

### 3.6 Materializer

```swift
/// 推論結果の永続化
///
/// 明示的トリプル (asserted) と推論トリプル (inferred) を区別して保存。
///
/// **DRed アルゴリズム**による増分マテリアライズをサポート。
///
/// **Reference**: Gupta, A., et al. (1993).
/// "Maintaining views incrementally." ACM SIGMOD Record.
public final class Materializer: Sendable {

    /// トリプルの種類
    public enum TripleKind: Int64, Sendable, Codable {
        case asserted = 0   // 明示的に追加
        case inferred = 1   // 推論により導出
    }

    /// マテリアライズ結果
    public struct MaterializationResult: Sendable {
        public let assertedCount: Int
        public let inferredCount: Int
        public let duration: Duration
        public let errors: [MaterializationError]
    }

    /// マテリアライズエラー
    public enum MaterializationError: Error, Sendable {
        case inconsistencyDetected(ConsistencyChecker.Inconsistency)
        case transactionFailed(String)
        case timeout
    }

    // MARK: - Properties

    nonisolated(unsafe) private let database: any DatabaseProtocol
    private let subspace: Subspace
    private let ruleEngine: RuleEngine

    // MARK: - Materialization

    /// フルマテリアライズ
    ///
    /// 全ての推論トリプルを削除し、再計算。
    public func materializeFull(ontology: OWLOntology) async throws -> MaterializationResult

    /// 増分マテリアライズ (DRed)
    ///
    /// 追加・削除されたトリプルに基づいて推論結果を更新。
    ///
    /// **DRed アルゴリズム**:
    /// 1. 削除フェーズ: 削除されたトリプルから導出された推論を削除
    /// 2. 再導出フェーズ: 残りのトリプルから再導出可能なものを復元
    /// 3. 追加フェーズ: 新しいトリプルから推論を追加
    public func materializeIncremental(
        added: [Triple],
        removed: [Triple],
        ontology: OWLOntology
    ) async throws -> MaterializationResult

    // MARK: - Storage Layout

    /// ストレージレイアウト:
    /// ```
    /// [subspace]/spo/[s]/[p]/[o]     → '' (トリプル)
    /// [subspace]/pos/[p]/[o]/[s]     → '' (インデックス)
    /// [subspace]/osp/[o]/[s]/[p]     → '' (インデックス)
    /// [subspace]/meta/[s]/[p]/[o]    → Tuple(kind: Int64, ruleId: String?, timestamp: Int64)
    /// ```
}
```

### 3.7 ConsistencyChecker

```swift
/// 一貫性検証器
///
/// ABox の一貫性とデータ型制約を検証。
public struct ConsistencyChecker: Sendable {

    /// 非一貫性の種類
    public enum Inconsistency: Sendable, Equatable {
        // クラス関連
        case disjointClassViolation(individual: String, class1: String, class2: String)
        case bottomClassMembership(individual: String)

        // プロパティ関連
        case functionalPropertyViolation(individual: String, property: String, values: [String])
        case inverseFunctionalPropertyViolation(property: String, value: String, subjects: [String])
        case asymmetricPropertyViolation(property: String, a: String, b: String)
        case irreflexivePropertyViolation(property: String, individual: String)

        // Cardinality
        case minCardinalityViolation(individual: String, property: String, required: Int, actual: Int)
        case maxCardinalityViolation(individual: String, property: String, allowed: Int, actual: Int)

        // 個体関連
        case sameAsDifferentConflict(a: String, b: String)

        // データ型関連
        case datatypeViolation(individual: String, property: String, value: OWLLiteral, expectedType: String)
    }

    /// 検証結果
    public struct CheckResult: Sendable {
        public let isConsistent: Bool
        public let inconsistencies: [Inconsistency]
        public let duration: Duration
    }

    /// フル一貫性チェック
    public func checkConsistency(
        ontology: OWLOntology,
        reasoner: OWLReasoner
    ) async throws -> CheckResult

    /// 増分一貫性チェック
    public func checkIncremental(
        newTriples: [Triple],
        ontology: OWLOntology,
        reasoner: OWLReasoner
    ) async throws -> CheckResult
}
```

---

## 4. Query Extension パターン

### 4.1 設計原則

CLAUDE.md の extension パターンに従い、推論機能は GraphQueryBuilder と FDBContext への extension として提供する。
コアを変更せず、ユーザーが `import GraphIndex` 後に推論機能を使用可能にする。

```
┌─────────────────────────────────────────────────────────────────────┐
│  User Code                                                          │
│  import GraphIndex  // 推論機能が自動的に利用可能                      │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GraphIndex Module                                                   │
│  ├── GraphQueryBuilder (既存)                                        │
│  ├── GraphQueryBuilder+Reasoning (extension)                         │
│  ├── FDBContext+Reasoning (extension)                                │
│  └── Reasoning/                                                      │
│      ├── OWLReasoner                                                │
│      └── ...                                                        │
└─────────────────────────────────────────────────────────────────────┘
```

### 4.2 GraphQueryBuilder Extension

```swift
// GraphIndex/Reasoning/GraphQueryBuilder+Reasoning.swift

import DatabaseEngine

/// 推論付きクエリ機能を GraphQueryBuilder に追加
extension GraphQueryBuilder {

    /// 推論コンテキストを設定
    ///
    /// - Parameter reasoner: OWL 推論エンジン
    /// - Returns: 推論付きクエリビルダー
    ///
    /// **Usage**:
    /// ```swift
    /// let results = try await queryBuilder
    ///     .withReasoning(reasoner)
    ///     .instancesOf(.named("Person"))
    /// ```
    public func withReasoning(_ reasoner: OWLReasoner) -> ReasoningGraphQueryBuilder<Item> {
        ReasoningGraphQueryBuilder(
            base: self,
            reasoner: reasoner
        )
    }
}

/// 推論付きグラフクエリビルダー
///
/// GraphQueryBuilder を拡張し、OWL 推論を適用したクエリを実行。
public struct ReasoningGraphQueryBuilder<Item: Persistable>: Sendable {
    private let base: GraphQueryBuilder<Item>
    private let reasoner: OWLReasoner

    // MARK: - 基本クエリ（推論なし）の継承

    public func from(_ value: any TupleElement) -> Self {
        Self(base: base.from(value), reasoner: reasoner)
    }

    public func edge(_ value: any TupleElement) -> Self {
        Self(base: base.edge(value), reasoner: reasoner)
    }

    public func to(_ value: any TupleElement) -> Self {
        Self(base: base.to(value), reasoner: reasoner)
    }

    public func limit(_ count: Int) -> Self {
        Self(base: base.limit(count), reasoner: reasoner)
    }

    /// 基本クエリを実行（推論なし）
    public func execute() async throws -> [GraphQueryBuilder<Item>.GraphEdge] {
        try await base.execute()
    }

    // MARK: - 推論付きクエリ

    /// クラスのインスタンスを検索（推論込み）
    ///
    /// サブクラス関係を考慮してインスタンスを返す。
    ///
    /// **例**:
    /// ```swift
    /// // Person のインスタンスを検索
    /// // Student subClassOf Person の場合、Student インスタンスも返す
    /// let people = try await queryBuilder
    ///     .withReasoning(reasoner)
    ///     .instancesOf(.named("Person"))
    /// ```
    public func instancesOf(_ class_: OWLClassExpression, direct: Bool = false) async throws -> [String] {
        try await reasoner.getInstances(class_, direct: direct)
    }

    /// プロパティ値を検索（推論込み）
    ///
    /// サブプロパティ関係、逆プロパティ、推移性を考慮。
    ///
    /// **例**:
    /// ```swift
    /// // knows の値を検索
    /// // friendOf subPropertyOf knows の場合、friendOf の値も返す
    /// let knownPeople = try await queryBuilder
    ///     .withReasoning(reasoner)
    ///     .valuesOf(subject: "Alice", property: "knows")
    /// ```
    public func valuesOf(subject: String, property: String) async throws -> Set<String> {
        try await reasoner.getObjectPropertyValues(subject: subject, property: property)
    }

    /// 個体のタイプを取得（推論込み）
    ///
    /// **例**:
    /// ```swift
    /// let types = try await queryBuilder
    ///     .withReasoning(reasoner)
    ///     .typesOf("Alice")
    /// // Returns: [Person, Agent, Thing, ...]
    /// ```
    public func typesOf(_ individual: String, direct: Bool = false) async throws -> Set<OWLClassExpression> {
        try await reasoner.getTypes(individual, direct: direct)
    }

    /// インスタンス判定（推論込み）
    public func isInstanceOf(_ individual: String, class_: OWLClassExpression) async throws -> Bool {
        try await reasoner.isInstanceOf(individual, class_: class_)
    }

    /// 同一個体判定（推論込み）
    public func isSameAs(_ a: String, _ b: String) async throws -> Bool {
        try await reasoner.isSameIndividual(a, b)
    }

    // MARK: - SPARQL-like パターンマッチング

    /// トリプルパターンマッチング（推論込み）
    ///
    /// **例**:
    /// ```swift
    /// // ?x knows ?y, ?y knows ?z のパターンを検索
    /// let results = try await queryBuilder
    ///     .withReasoning(reasoner)
    ///     .match(
    ///         .triple("?x", "knows", "?y"),
    ///         .triple("?y", "knows", "?z")
    ///     )
    /// // Returns: [["?x": "Alice", "?y": "Bob", "?z": "Charlie"], ...]
    /// ```
    public func match(_ patterns: TriplePattern...) async throws -> [[String: String]] {
        try await matchPatterns(patterns)
    }

    private func matchPatterns(_ patterns: [TriplePattern]) async throws -> [[String: String]] {
        // パターンマッチング実装
        // 1. 各パターンを評価
        // 2. 推論を適用（サブクラス、サブプロパティ展開）
        // 3. 変数バインディングを結合
        fatalError("Implementation required")
    }
}

/// トリプルパターン
public struct TriplePattern: Sendable {
    public let subject: PatternElement
    public let predicate: PatternElement
    public let object: PatternElement

    public static func triple(_ s: String, _ p: String, _ o: String) -> TriplePattern {
        TriplePattern(
            subject: s.hasPrefix("?") ? .variable(String(s.dropFirst())) : .constant(s),
            predicate: p.hasPrefix("?") ? .variable(String(p.dropFirst())) : .constant(p),
            object: o.hasPrefix("?") ? .variable(String(o.dropFirst())) : .constant(o)
        )
    }

    public enum PatternElement: Sendable, Hashable {
        case variable(String)
        case constant(String)
    }
}
```

### 4.3 FDBContext Extension

```swift
// GraphIndex/Reasoning/FDBContext+Reasoning.swift

import DatabaseEngine

/// FDBContext に推論付きクエリ機能を追加
extension FDBContext {

    /// 推論付きグラフクエリを開始
    ///
    /// **Usage**:
    /// ```swift
    /// let context = container.newContext()
    ///
    /// // 推論エンジンを初期化（通常は一度だけ）
    /// let reasoner = try await OWLReasoner(
    ///     database: database,
    ///     ontology: ontology,
    ///     subspace: subspace
    /// )
    ///
    /// // 推論付きクエリ
    /// let instances = try await context
    ///     .graphQuery(Statement.self, reasoner: reasoner)
    ///     .instancesOf(.named("Person"))
    /// ```
    public func graphQuery<T: Persistable>(
        _ type: T.Type,
        reasoner: OWLReasoner,
        strategy: GraphIndexStrategy = .tripleStore
    ) -> ReasoningGraphQueryBuilder<T> {
        let subspace = getGraphIndexSubspace(for: type)
        let baseBuilder = GraphQueryBuilder<T>(
            database: database,
            subspace: subspace,
            strategy: strategy
        )
        return baseBuilder.withReasoning(reasoner)
    }

    /// グラフクエリを開始（推論なし）
    ///
    /// 既存の基本クエリ機能。
    public func graphQuery<T: Persistable>(
        _ type: T.Type,
        strategy: GraphIndexStrategy = .tripleStore
    ) -> GraphQueryBuilder<T> {
        let subspace = getGraphIndexSubspace(for: type)
        return GraphQueryBuilder<T>(
            database: database,
            subspace: subspace,
            strategy: strategy
        )
    }

    private func getGraphIndexSubspace<T: Persistable>(for type: T.Type) -> Subspace {
        // インデックスサブスペースを取得
        // 実装は FDBContext の内部構造に依存
        fatalError("Implementation required")
    }
}
```

### 4.4 OWLReasoner の注入パターン

```swift
/// 推論エンジンの生成と管理
///
/// **シングルトンパターン（オントロジーごと）**:
/// ```swift
/// // アプリケーション起動時に一度だけ初期化
/// let reasoner = try await OWLReasoner(
///     database: container.database,
///     ontology: ontology,
///     subspace: ontologySubspace
/// )
///
/// // 複数のクエリで共有
/// let query1 = context.graphQuery(Statement.self, reasoner: reasoner)
/// let query2 = context.graphQuery(Relationship.self, reasoner: reasoner)
/// ```
///
/// **コンテナ統合パターン**:
/// ```swift
/// // FDBContainer に推論エンジンを登録
/// extension FDBContainer {
///     public func registerReasoner(
///         _ reasoner: OWLReasoner,
///         for ontologyIRI: String
///     ) { ... }
///
///     public func reasoner(for ontologyIRI: String) -> OWLReasoner? { ... }
/// }
///
/// // 使用例
/// container.registerReasoner(reasoner, for: "http://example.org/ontology")
///
/// let context = container.newContext()
/// if let reasoner = container.reasoner(for: "http://example.org/ontology") {
///     let results = try await context
///         .graphQuery(Statement.self, reasoner: reasoner)
///         .instancesOf(.named("Person"))
/// }
/// ```
```

### 4.5 使用例

```swift
import DatabaseEngine
import GraphIndex

// 1. オントロジーを定義
var ontology = OWLOntology(iri: "http://example.org/people")
ontology.classes = [
    OWLClass(iri: "Person"),
    OWLClass(iri: "Student"),
    OWLClass(iri: "Employee")
]
ontology.axioms = [
    .subClassOf(sub: .named("Student"), sup: .named("Person")),
    .subClassOf(sub: .named("Employee"), sup: .named("Person")),
    .transitiveObjectProperty("knows"),
    .symmetricObjectProperty("friendOf")
]

// 2. 推論エンジンを初期化
let reasoner = try await OWLReasoner(
    database: container.database,
    ontology: ontology,
    subspace: graphSubspace
)

// 3. マテリアライズ（オプション：推論結果を永続化）
try await reasoner.materialize()

// 4. 推論付きクエリ
let context = container.newContext()

// Person の全インスタンス（Student, Employee 含む）
let people = try await context
    .graphQuery(Statement.self, reasoner: reasoner)
    .instancesOf(.named("Person"))

// Alice が知っている人（推移的 knows を考慮）
let aliceKnows = try await context
    .graphQuery(Statement.self, reasoner: reasoner)
    .valuesOf(subject: "Alice", property: "knows")

// パターンマッチング
let friendsOfFriends = try await context
    .graphQuery(Statement.self, reasoner: reasoner)
    .match(
        .triple("Alice", "friendOf", "?x"),
        .triple("?x", "friendOf", "?y")
    )

// 5. 推論なしの基本クエリも引き続き利用可能
let directEdges = try await context
    .graphQuery(Statement.self)
    .from("Alice")
    .edge("knows")
    .execute()
```

---

## 5. トランザクションと並行性

### 5.1 FDB トランザクション戦略

```swift
/// 推論タスクのトランザクション設定
public struct ReasoningTransactionConfig: Sendable {
    /// 読み取りスナップショット使用（一貫した読み取り）
    public var useSnapshot: Bool = true

    /// バッチサイズ（1トランザクションあたりの書き込み数）
    public var batchSize: Int = 1000

    /// トランザクションタイムアウト（ミリ秒）
    public var timeoutMs: Int = 5000

    /// リトライ設定
    public var maxRetries: Int = 5
    public var retryDelayMs: Int = 100
}

/// 推論ワークキュー
///
/// 大規模マテリアライズを複数トランザクションに分割。
public final class ReasoningWorkQueue: Sendable {

    /// ワークアイテム
    public enum WorkItem: Sendable {
        case materializeClass(OWLClassExpression)
        case materializeProperty(String)
        case materializeIndividual(String)
        case checkConsistency(String)
    }

    /// キューにアイテムを追加
    public func enqueue(_ items: [WorkItem]) async

    /// ワーカーを開始
    public func startWorkers(count: Int) async

    /// 進捗を取得
    public func getProgress() -> Progress

    struct Progress: Sendable {
        let total: Int
        let completed: Int
        let failed: Int
        let currentItem: WorkItem?
    }
}
```

### 5.2 一貫性チェックのトリガー

```swift
/// 一貫性チェックポリシー
public enum ConsistencyCheckPolicy: Sendable {
    /// 毎回チェック（最も安全、最も遅い）
    case always

    /// バッチ終了時のみ
    case onBatchComplete

    /// 手動トリガーのみ
    case manual

    /// サンプリング（N回に1回）
    case sampling(rate: Int)
}

/// 更新時の一貫性チェック
extension OWLReasoner {
    public func addAxiom(
        _ axiom: OWLAxiom,
        checkPolicy: ConsistencyCheckPolicy = .onBatchComplete
    ) async throws -> AddAxiomResult

    public struct AddAxiomResult: Sendable {
        let success: Bool
        let inferredTriples: Int
        let consistencyCheck: ConsistencyChecker.CheckResult?
    }
}
```

---

## 6. 可観測性

### 6.1 推論トレース

```swift
/// 推論トレーサー
public struct ReasoningTracer: Sendable {

    /// トレースイベント
    public enum TraceEvent: Sendable {
        case ruleApplied(rule: String, bindings: [String: String], inferred: [Triple])
        case clashDetected(type: TableauxReasoner.ClashType, node: String, detail: String)
        case backtrack(from: String, reason: String)
        case nodeCreated(id: String, labels: [String])
        case blocking(blocked: String, by: String)
        case materialized(count: Int, duration: Duration)
    }

    /// トレースレベル
    public enum Level: Int, Sendable {
        case off = 0
        case error = 1
        case info = 2
        case debug = 3
        case trace = 4
    }

    /// イベントハンドラ
    public var onEvent: (@Sendable (TraceEvent) -> Void)?

    /// サンプリングレート（1.0 = 全て、0.1 = 10%）
    public var samplingRate: Double = 1.0
}

/// 推論統計
public struct ReasoningStatistics: Sendable {
    public let tableauxInvocations: Int
    public let nodesCreated: Int
    public let rulesApplied: Int
    public let backtracks: Int
    public let clashes: Int
    public let inferredTriples: Int
    public let duration: Duration
}
```

---

## 7. ベンチマーク計画

### 7.1 テストオントロジー

| オントロジー | クラス数 | プロパティ数 | 個体数 | 特徴 |
|-------------|---------|-------------|--------|------|
| Wine | ~100 | ~20 | ~200 | 基本的なクラス階層 |
| Pizza | ~100 | ~10 | ~50 | Defined classes |
| LUBM-1 | ~50 | ~30 | ~100K | 大規模 ABox |
| LUBM-10 | ~50 | ~30 | ~1M | スケーラビリティ |
| GO (Gene Ontology) | ~40K | ~10 | ~1K | 深いクラス階層 |

### 7.2 ベンチマーク項目

```swift
/// ベンチマークスイート
public struct ReasoningBenchmark {

    /// 分類ベンチマーク（TBox 推論）
    func benchmarkClassification(ontology: OWLOntology) -> BenchmarkResult

    /// 実現ベンチマーク（ABox 推論）
    func benchmarkRealization(ontology: OWLOntology) -> BenchmarkResult

    /// マテリアライズベンチマーク
    func benchmarkMaterialization(ontology: OWLOntology) -> BenchmarkResult

    /// 増分更新ベンチマーク
    func benchmarkIncremental(
        ontology: OWLOntology,
        additions: [OWLAxiom],
        deletions: [OWLAxiom]
    ) -> BenchmarkResult

    /// クエリベンチマーク
    func benchmarkQuery(
        ontology: OWLOntology,
        queries: [OWLClassExpression]
    ) -> BenchmarkResult

    struct BenchmarkResult: Sendable {
        let duration: Duration
        let memoryPeakMB: Int
        let transactionCount: Int
        let resultCount: Int
    }
}
```

---

## 8. 実装計画

### Phase 1: スキーマ定義（database-kit）
- [ ] OWLOntology, OWLClass, OWLClassExpression
- [ ] OWLObjectProperty, OWLDataProperty, PropertyCharacteristic
- [ ] OWLAxiom (TBox/RBox/ABox 全公理)
- [ ] OWLDataRange, OWLLiteral, FacetRestriction
- [ ] OWLNamedIndividual
- [ ] OWLDLRegularityChecker

### Phase 2: 階層とデータ型検証（database-framework）
- [ ] RoleHierarchy（推移閉包、逆ロール、simple role 判定）
- [ ] ClassHierarchy（サブクラス推移閉包）
- [ ] OWLDatatypeValidator（XSD 型検証）

### Phase 3: Tableaux 推論
- [ ] TableauxReasoner（SHOIN(D) 完全対応）
- [ ] 展開ルール実装（⊓, ⊔, ∃, ∀, ≥, ≤, nominal, Self）
- [ ] Pairwise blocking
- [ ] Clash detection

### Phase 4: ルールエンジンとマテリアライズ
- [ ] RuleEngine（RETE）
- [ ] 標準 RDFS/OWL ルール
- [ ] Materializer（フル/増分）
- [ ] DRed アルゴリズム

### Phase 5: 一貫性と最適化
- [ ] ConsistencyChecker
- [ ] ReasoningWorkQueue
- [ ] トレースとメトリクス
- [ ] ベンチマーク

---

## 9. 参考文献

1. Baader, F., et al. (2003). "The Description Logic Handbook." Cambridge University Press.
2. Horrocks, I., et al. (2003). "From SHIQ and RDF to OWL." Journal of Web Semantics.
3. Motik, B., et al. (2009). "Hypertableau reasoning for description logics." JAIR.
4. Forgy, C. L. (1982). "Rete: A fast algorithm." Artificial Intelligence.
5. Gupta, A., et al. (1993). "Maintaining views incrementally." ACM SIGMOD.
6. Weiss, C., et al. (2008). "Hexastore." VLDB.
7. W3C. (2012). "OWL 2 Web Ontology Language." https://www.w3.org/TR/owl2-overview/
