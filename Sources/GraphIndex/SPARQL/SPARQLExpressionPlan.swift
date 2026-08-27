import DatabaseKit
import DatabaseEngine
import DatabaseTypes

enum SPARQLExpressionResultOwnership {
    case borrowed
    case produced(maximumFootprint: DatabaseIntermediateFootprint)
}

public struct SPARQLExpressionPlan: Sendable {
    public enum Volatility: Sendable, Hashable {
        case immutable
        case queryStable
        case volatile
    }

    public let referencedVariables: Set<String>
    public let requiresDataset: Bool
    public let usesExtensionFunction: Bool
    public let volatility: Volatility
    let program: SPARQLExpressionProgram
    private let maximumStringUTF8Count: UInt64

    public init(
        _ expression: consuming Expression,
        limits: SPARQLExpressionCompilationLimits = .default
    ) throws {
        try SPARQLExpressionValidator.validate(expression, limits: limits)
        let program = try SPARQLExpressionProgram(
            consume expression,
            limits: limits,
            compileExistsPatterns: true
        )
        let analysis = try Self.analyze(program)
        self.referencedVariables = analysis.variables
        self.requiresDataset = analysis.requiresDataset
        self.usesExtensionFunction = analysis.usesExtensionFunction
        self.volatility = analysis.volatility
        self.program = program
        self.maximumStringUTF8Count = limits.maximumStringUTF8Count
    }

    public var isFilterPushdownSafe: Bool {
        !requiresDataset
            && !usesExtensionFunction
            && volatility != .volatile
    }

    var directVariable: String? {
        guard program.nodes.count == 1,
              case .variable(let variable) = program.nodes[0].opcode else {
            return nil
        }
        return "?\(variable)"
    }

    func compiledExistsPattern(at handle: Int) -> ExecutionPattern? {
        program.compiledExistsPattern(at: handle)
    }

    /// Computes a conservative retained-result bound without producing the
    /// result. Direct variables and root literals stay borrowed from their
    /// existing owner; every derived result receives a query-scoped owner.
    func resultOwnership(
        binding: borrowing VariableBinding,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        maximumExtensionResultByteCount: (String) throws -> UInt64
    ) throws -> SPARQLExpressionResultOwnership {
        guard let root = program.nodes.first else {
            return .borrowed
        }
        switch root.opcode {
        case .variable, .column, .literal:
            return .borrowed
        default:
            break
        }

        let maximumSingleTerm = try CanonicalRelationalFootprintMeter
            .maximumRDFTermValueFootprint(
                maximumUTF8ByteCount: maximumStringUTF8Count
            )
        let boundsFootprint = try DatabaseIntermediateCollectionMeter
            .arrayFootprint(
                count: program.nodes.count,
                element: DatabaseIntermediateFootprint.self
            )
        let boundsReservation = try workMeter.reserveIntermediate(
            bytes: boundsFootprint.bytes,
            at: stage
        )
        defer { boundsReservation.release() }
        var bounds = Array(
            repeating: DatabaseIntermediateFootprint(),
            count: program.nodes.count
        )
        for nodeIndex in program.nodes.indices.reversed() {
            let node = program.nodes[nodeIndex]
            let bound: DatabaseIntermediateFootprint
            switch node.opcode {
            case .variable(let variable), .column(let variable):
                if let value = binding["?\(variable)"] {
                    bound = try CanonicalRelationalFootprintMeter
                        .valueFootprint(
                            of: value,
                            workMeter: workMeter,
                            stage: stage
                        )
                } else {
                    bound = DatabaseIntermediateFootprint(
                        bytes: UInt64(MemoryLayout<FieldValue>.stride) + 32
                    )
                }

            case .literal:
                bound = maximumSingleTerm

            case .function(
                let name,
                .extensionFunction,
                _
            ):
                bound = DatabaseIntermediateFootprint(
                    bytes: try maximumExtensionResultByteCount(name)
                )

            case .triple:
                var total = try CanonicalRelationalFootprintMeter
                    .maximumRDFTermValueFootprint(
                        maximumUTF8ByteCount: 0
                    )
                for childSlot in node.children {
                    total = try total.adding(
                        bounds[program.childIndices[childSlot]]
                    )
                }
                bound = total

            case .subject, .predicate, .object:
                if let childSlot = node.children.first {
                    bound = bounds[program.childIndices[childSlot]]
                } else {
                    bound = maximumSingleTerm
                }

            case .conditional, .coalesce, .caseSelection, .nullIf:
                var maximumChild: DatabaseIntermediateFootprint?
                for childSlot in node.children {
                    let candidate = bounds[program.childIndices[childSlot]]
                    if maximumChild.map({ $0.bytes < candidate.bytes })
                        ?? true {
                        maximumChild = candidate
                    }
                }
                bound = maximumChild ?? maximumSingleTerm

            default:
                bound = maximumSingleTerm
            }
            bounds[nodeIndex] = bound
        }
        return .produced(maximumFootprint: bounds[0])
    }

    private struct Analysis {
        var variables: Set<String> = []
        var requiresDataset = false
        var usesExtensionFunction = false
        var volatility: Volatility = .immutable

        mutating func merge(volatility candidate: Volatility) {
            if candidate == .volatile {
                volatility = .volatile
            } else if candidate == .queryStable, volatility == .immutable {
                volatility = .queryStable
            }
        }
    }

    private static func analyze(
        _ program: SPARQLExpressionProgram
    ) throws -> Analysis {
        var analysis = Analysis()
        for node in program.nodes {
            switch node.opcode {
            case .variable(let variable), .bound(let variable):
                analysis.variables.insert(prefixed(variable))
            case .function(_, let identifier, _):
                switch identifier {
                case .extensionFunction:
                    analysis.usesExtensionFunction = true
                case .datatypeConstructor:
                    break
                case .builtIn(let builtIn):
                    analysis.merge(volatility: builtIn.volatility)
                }
            case .exists:
                analysis.requiresDataset = true
            default:
                break
            }
        }
        return analysis
    }

    private static func prefixed(_ variable: String) -> String {
        return "?\(variable)"
    }
}

extension SPARQLExpressionPlan: CustomStringConvertible {
    public var description: String {
        program.rootDescription
    }
}
