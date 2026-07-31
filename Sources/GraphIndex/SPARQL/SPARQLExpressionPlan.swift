import DatabaseKit

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
