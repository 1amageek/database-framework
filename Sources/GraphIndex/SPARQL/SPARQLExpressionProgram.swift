import DatabaseTypes
import DatabaseKit

/// Flat, immutable execution representation of one QueryIR expression.
///
/// An opcode never retains an expression subtree. Child relationships are
/// stored only as indices in one contiguous table, so scalar evaluation does
/// not walk the process call stack. Compiled EXISTS handles retain their
/// separately bounded graph-algebra plans.
struct SPARQLExpressionProgram: Sendable {
    enum Opcode: Sendable, Hashable {
        case literal(Literal)
        case column(String)
        case variable(String)
        case parameter

        case add
        case subtract
        case multiply
        case divide
        case modulo
        case negate

        case equal
        case notEqual
        case lessThan
        case lessThanOrEqual
        case greaterThan
        case greaterThanOrEqual

        case conjunction
        case disjunction
        case negation

        case isNull
        case isNotNull
        case bound(String)

        case like(pattern: String)
        case regularExpression(pattern: String, flags: String?)
        case between
        case membership(negated: Bool)

        case function(
            name: String,
            identifier: SPARQLFunctionIdentifier,
            distinct: Bool
        )
        case conditional
        case coalesce
        case caseSelection(pairCount: Int, hasElseResult: Bool)
        case nullIf
        case cast(DataType)

        case triple
        case isTriple
        case subject
        case predicate
        case object

        case exists(handle: Int)
        case unsupported(String)
    }

    struct Node: Sendable, Hashable {
        let opcode: Opcode
        let children: Range<Int>
    }

    struct CompiledExistsPattern: Sendable {
        let executionPattern: ExecutionPattern
    }

    private struct PendingNode {
        let expression: Expression
        let depth: UInt64
        let parentChildSlot: Int?
    }

    let nodes: [Node]
    let childIndices: [Int]
    let compiledExistsPatterns: [CompiledExistsPattern]

    init(
        _ expression: consuming Expression,
        limits: SPARQLExpressionCompilationLimits,
        compileExistsPatterns: Bool
    ) throws(SPARQLExpressionCompilationError) {
        guard limits.maximumNodes >= 1 else {
            throw .structural(
                .resourceLimitExceeded(
                    resource: .totalNodes,
                    actual: 1,
                    maximum: limits.maximumNodes
                )
            )
        }

        var nodes: [Node] = []
        nodes.reserveCapacity(Int(min(limits.maximumNodes, 256)))
        var childIndices: [Int] = []
        var existsPatterns: [CompiledExistsPattern] = []
        var pending = [
            PendingNode(
                expression: consume expression,
                depth: 0,
                parentChildSlot: nil
            ),
        ]

        while let current = pending.popLast() {
            guard current.depth <= limits.maximumDepth else {
                throw .structural(
                    .resourceLimitExceeded(
                        resource: .nestingDepth,
                        actual: current.depth,
                        maximum: limits.maximumDepth
                    )
                )
            }
            let requiredNodes = UInt64(nodes.count) + 1
            guard requiredNodes <= limits.maximumNodes else {
                throw .structural(
                    .resourceLimitExceeded(
                        resource: .totalNodes,
                        actual: requiredNodes,
                        maximum: limits.maximumNodes
                    )
                )
            }

            let nodeIndex = nodes.count
            if let slot = current.parentChildSlot {
                childIndices[slot] = nodeIndex
            }

            let decomposition = try Self.decompose(
                current.expression,
                limits: limits,
                compileExistsPatterns: compileExistsPatterns,
                existsPatterns: &existsPatterns
            )
            let childStart = childIndices.count
            let (childEnd, childCountOverflow) = childStart
                .addingReportingOverflow(decomposition.children.count)
            guard !childCountOverflow else {
                throw .structural(
                    .resourceLimitExceeded(
                        resource: .collectionElements,
                        actual: .max,
                        maximum: limits.maximumCollectionElements
                    )
                )
            }
            guard UInt64(decomposition.children.count)
                    <= limits.maximumCollectionElements else {
                throw .structural(
                    .resourceLimitExceeded(
                        resource: .collectionElements,
                        actual: UInt64(decomposition.children.count),
                        maximum: limits.maximumCollectionElements
                    )
                )
            }
            childIndices.append(
                contentsOf: repeatElement(
                    -1,
                    count: decomposition.children.count
                )
            )
            nodes.append(
                Node(
                    opcode: decomposition.opcode,
                    children: childStart..<childEnd
                )
            )

            let (childDepth, depthOverflow) = current.depth
                .addingReportingOverflow(1)
            guard decomposition.children.isEmpty || !depthOverflow else {
                throw .structural(
                    .resourceLimitExceeded(
                        resource: .nestingDepth,
                        actual: .max,
                        maximum: limits.maximumDepth
                    )
                )
            }
            for offset in decomposition.children.indices.reversed() {
                pending.append(
                    PendingNode(
                        expression: decomposition.children[offset],
                        depth: childDepth,
                        parentChildSlot: childStart + offset
                    )
                )
            }
        }

        guard !childIndices.contains(-1) else {
            throw .unsupportedExpression(
                "expression program contains an unresolved child"
            )
        }
        self.nodes = consume nodes
        self.childIndices = consume childIndices
        self.compiledExistsPatterns = consume existsPatterns
    }

    func child(
        of nodeIndex: Int,
        at offset: Int
    ) -> Int {
        let range = nodes[nodeIndex].children
        return childIndices[range.lowerBound + offset]
    }

    func childCount(of nodeIndex: Int) -> Int {
        nodes[nodeIndex].children.count
    }

    func compiledExistsPattern(at handle: Int) -> ExecutionPattern? {
        guard compiledExistsPatterns.indices.contains(handle) else {
            return nil
        }
        return compiledExistsPatterns[handle].executionPattern
    }

    private struct Decomposition {
        let opcode: Opcode
        let children: [Expression]
    }

    private static func decompose(
        _ expression: consuming Expression,
        limits: SPARQLExpressionCompilationLimits,
        compileExistsPatterns: Bool,
        existsPatterns: inout [CompiledExistsPattern]
    ) throws(SPARQLExpressionCompilationError) -> Decomposition {
        switch consume expression {
        case .literal(let value):
            return Decomposition(opcode: .literal(value), children: [])
        case .column(let value):
            return Decomposition(opcode: .column(value.column), children: [])
        case .variable(let value):
            return Decomposition(opcode: .variable(value.name), children: [])
        case .parameter:
            return Decomposition(opcode: .parameter, children: [])

        case .add(let lhs, let rhs):
            return binary(.add, consume lhs, consume rhs)
        case .subtract(let lhs, let rhs):
            return binary(.subtract, consume lhs, consume rhs)
        case .multiply(let lhs, let rhs):
            return binary(.multiply, consume lhs, consume rhs)
        case .divide(let lhs, let rhs):
            return binary(.divide, consume lhs, consume rhs)
        case .modulo(let lhs, let rhs):
            return binary(.modulo, consume lhs, consume rhs)
        case .negate(let value):
            return unary(.negate, consume value)

        case .equal(let lhs, let rhs):
            return binary(.equal, consume lhs, consume rhs)
        case .notEqual(let lhs, let rhs):
            return binary(.notEqual, consume lhs, consume rhs)
        case .lessThan(let lhs, let rhs):
            return binary(.lessThan, consume lhs, consume rhs)
        case .lessThanOrEqual(let lhs, let rhs):
            return binary(.lessThanOrEqual, consume lhs, consume rhs)
        case .greaterThan(let lhs, let rhs):
            return binary(.greaterThan, consume lhs, consume rhs)
        case .greaterThanOrEqual(let lhs, let rhs):
            return binary(.greaterThanOrEqual, consume lhs, consume rhs)

        case .and(let lhs, let rhs):
            return binary(.conjunction, consume lhs, consume rhs)
        case .or(let lhs, let rhs):
            return binary(.disjunction, consume lhs, consume rhs)
        case .not(let value):
            return unary(.negation, consume value)

        case .isNull(let value):
            return unary(.isNull, consume value)
        case .isNotNull(let value):
            return unary(.isNotNull, consume value)
        case .bound(let variable):
            return Decomposition(
                opcode: .bound(variable.name),
                children: []
            )

        case .like(let value, let pattern):
            return unary(.like(pattern: pattern), consume value)
        case .regex(let value, let pattern, let flags):
            return unary(
                .regularExpression(pattern: pattern, flags: flags),
                consume value
            )
        case .between(let value, let low, let high):
            return Decomposition(
                opcode: .between,
                children: [
                    consume value,
                    consume low,
                    consume high,
                ]
            )
        case .inList(let value, let candidates):
            return membership(
                consume value,
                candidates: consume candidates,
                negated: false
            )
        case .notInList(let value, let candidates):
            return membership(
                consume value,
                candidates: consume candidates,
                negated: true
            )
        case .inSubquery:
            return Decomposition(
                opcode: .unsupported("IN-subquery expression"),
                children: []
            )

        case .aggregate:
            return Decomposition(
                opcode: .unsupported("aggregate"),
                children: []
            )

        case .function(let call):
            let identifier = try SPARQLFunctionIdentifier.resolve(call.name)
            guard !call.distinct else {
                return Decomposition(
                    opcode: .function(
                        name: call.name,
                        identifier: identifier,
                        distinct: true
                    ),
                    children: call.arguments
                )
            }
            switch identifier {
            case .builtIn(.bound):
                guard call.arguments.count == 1,
                      case .variable(let variable) = call.arguments[0] else {
                    return Decomposition(
                        opcode: .function(
                            name: call.name,
                            identifier: identifier,
                            distinct: call.distinct
                        ),
                        children: call.arguments
                    )
                }
                return Decomposition(
                    opcode: .bound(variable.name),
                    children: []
                )
            case .builtIn(.conditional):
                guard call.arguments.count == 3 else {
                    return Decomposition(
                        opcode: .function(
                            name: call.name,
                            identifier: identifier,
                            distinct: call.distinct
                        ),
                        children: call.arguments
                    )
                }
                return Decomposition(
                    opcode: .conditional,
                    children: call.arguments
                )
            case .builtIn(.coalesce):
                guard !call.arguments.isEmpty else {
                    return Decomposition(
                        opcode: .function(
                            name: call.name,
                            identifier: identifier,
                            distinct: call.distinct
                        ),
                        children: []
                    )
                }
                return Decomposition(
                    opcode: .coalesce,
                    children: call.arguments
                )
            default:
                return Decomposition(
                        opcode: .function(
                            name: call.name,
                            identifier: identifier,
                            distinct: call.distinct
                        ),
                    children: call.arguments
                )
            }

        case .caseWhen(let pairs, let elseResult):
            let (pairChildren, pairCountOverflow) = pairs.count
                .multipliedReportingOverflow(by: 2)
            let additionalChild = elseResult == nil ? 0 : 1
            let (requiredChildren, childCountOverflow) = pairChildren
                .addingReportingOverflow(additionalChild)
            guard !pairCountOverflow, !childCountOverflow else {
                throw .structural(
                    .resourceLimitExceeded(
                        resource: .collectionElements,
                        actual: .max,
                        maximum: limits.maximumCollectionElements
                    )
                )
            }
            var children: [Expression] = []
            children.reserveCapacity(requiredChildren)
            for pair in pairs {
                children.append(pair.condition)
                children.append(pair.result)
            }
            let hasElseResult = elseResult != nil
            if let elseResult {
                children.append(consume elseResult)
            }
            return Decomposition(
                opcode: .caseSelection(
                    pairCount: pairs.count,
                    hasElseResult: hasElseResult
                ),
                children: consume children
            )
        case .coalesce(let values):
            return Decomposition(
                opcode: .coalesce,
                children: consume values
            )
        case .nullIf(let lhs, let rhs):
            return binary(.nullIf, consume lhs, consume rhs)
        case .cast(let value, let targetType):
            return unary(.cast(targetType), consume value)

        case .triple(let subject, let predicate, let object):
            return Decomposition(
                opcode: .triple,
                children: [
                    consume subject,
                    consume predicate,
                    consume object,
                ]
            )
        case .isTriple(let value):
            return unary(.isTriple, consume value)
        case .subject(let value):
            return unary(.subject, consume value)
        case .predicate(let value):
            return unary(.predicate, consume value)
        case .object(let value):
            return unary(.object, consume value)

        case .subquery:
            return Decomposition(
                opcode: .unsupported("scalar subquery"),
                children: []
            )
        case .exists(let query):
            guard compileExistsPatterns else {
                return Decomposition(
                    opcode: .unsupported(
                        "EXISTS requires a runtime resolver"
                    ),
                    children: []
                )
            }
            let pattern: ExecutionPattern
            do {
                pattern = try SPARQLExistsPatternCompiler.compile(
                    query,
                    limits: limits
                )
            } catch let error as SPARQLExpressionCompilationError {
                throw error
            } catch {
                throw .unsupportedExpression(
                    "EXISTS graph pattern conversion failed"
                )
            }
            let handle = existsPatterns.count
            existsPatterns.append(
                CompiledExistsPattern(
                    executionPattern: pattern
                )
            )
            return Decomposition(
                opcode: .exists(handle: handle),
                children: []
            )
        }
    }

    private static func unary(
        _ opcode: Opcode,
        _ value: consuming Expression
    ) -> Decomposition {
        Decomposition(opcode: opcode, children: [consume value])
    }

    private static func binary(
        _ opcode: Opcode,
        _ lhs: consuming Expression,
        _ rhs: consuming Expression
    ) -> Decomposition {
        Decomposition(
            opcode: opcode,
            children: [consume lhs, consume rhs]
        )
    }

    private static func membership(
        _ value: consuming Expression,
        candidates: consuming [Expression],
        negated: Bool
    ) -> Decomposition {
        var children: [Expression] = []
        children.reserveCapacity(candidates.count + 1)
        children.append(consume value)
        children.append(contentsOf: consume candidates)
        return Decomposition(
            opcode: .membership(negated: negated),
            children: consume children
        )
    }

    var rootDescription: String {
        guard let root = nodes.first else {
            return "<empty expression>"
        }
        switch root.opcode {
        case .literal(let literal): return String(describing: literal)
        case .column(let column): return column
        case .variable(let variable): return "?\(variable)"
        case .parameter: return "parameter"
        case .add: return "addition"
        case .subtract: return "subtraction"
        case .multiply: return "multiplication"
        case .divide: return "division"
        case .modulo: return "modulo"
        case .negate: return "numeric negation"
        case .equal: return "equality"
        case .notEqual: return "inequality"
        case .lessThan: return "less-than comparison"
        case .lessThanOrEqual: return "less-than-or-equal comparison"
        case .greaterThan: return "greater-than comparison"
        case .greaterThanOrEqual:
            return "greater-than-or-equal comparison"
        case .conjunction: return "logical conjunction"
        case .disjunction: return "logical disjunction"
        case .negation: return "logical negation"
        case .isNull: return "null test"
        case .isNotNull: return "non-null test"
        case .bound(let variable): return "BOUND(?\(variable))"
        case .like: return "LIKE expression"
        case .regularExpression: return "regular expression"
        case .between: return "BETWEEN expression"
        case .membership(let negated):
            return negated ? "NOT-IN-list expression" : "IN-list expression"
        case .function(let name, _, _): return "\(name)(...)"
        case .conditional: return "IF(...)"
        case .coalesce: return "COALESCE(...)"
        case .caseSelection: return "CASE expression"
        case .nullIf: return "NULLIF(...)"
        case .cast: return "cast"
        case .triple: return "RDF triple constructor"
        case .isTriple: return "RDF triple test"
        case .subject: return "RDF subject accessor"
        case .predicate: return "RDF predicate accessor"
        case .object: return "RDF object accessor"
        case .exists: return "EXISTS"
        case .unsupported(let name): return name
        }
    }
}
