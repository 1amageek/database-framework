import DatabaseKit

public struct SPARQLExpressionPlan: Sendable, Hashable {
    public enum Volatility: Sendable, Hashable {
        case immutable
        case queryStable
        case volatile
    }

    public let expression: Expression
    public let referencedVariables: Set<String>
    public let requiresDataset: Bool
    public let usesExtensionFunction: Bool
    public let volatility: Volatility
    private let compiledExistsPatterns: [SelectQuery: ExecutionPattern]

    public init(
        _ expression: Expression,
        limits: SPARQLExpressionCompilationLimits = .default
    ) throws {
        try SPARQLExpressionValidator.validate(expression, limits: limits)
        var analysis = Analysis()
        try Self.analyze(expression, into: &analysis)
        self.expression = expression
        self.referencedVariables = analysis.variables
        self.requiresDataset = analysis.requiresDataset
        self.usesExtensionFunction = analysis.usesExtensionFunction
        self.volatility = analysis.volatility
        self.compiledExistsPatterns = analysis.compiledExistsPatterns
    }

    public var isFilterPushdownSafe: Bool {
        !requiresDataset
            && !usesExtensionFunction
            && volatility != .volatile
    }

    func compiledExistsPattern(
        for query: SelectQuery
    ) -> ExecutionPattern? {
        compiledExistsPatterns[query]
    }

    public static func == (
        lhs: SPARQLExpressionPlan,
        rhs: SPARQLExpressionPlan
    ) -> Bool {
        lhs.expression == rhs.expression
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(expression)
    }

    private struct Analysis {
        var variables: Set<String> = []
        var requiresDataset = false
        var usesExtensionFunction = false
        var volatility: Volatility = .immutable
        var compiledExistsPatterns: [SelectQuery: ExecutionPattern] = [:]

        mutating func merge(volatility candidate: Volatility) {
            if candidate == .volatile {
                volatility = .volatile
            } else if candidate == .queryStable, volatility == .immutable {
                volatility = .queryStable
            }
        }
    }

    private static func analyze(
        _ expression: Expression,
        into analysis: inout Analysis
    ) throws {
        switch expression {
        case .variable(let variable):
            analysis.variables.insert(prefixed(variable.name))
        case .bound(let variable):
            analysis.variables.insert(prefixed(variable.name))

        case .add(let lhs, let rhs), .subtract(let lhs, let rhs),
             .multiply(let lhs, let rhs), .divide(let lhs, let rhs),
             .modulo(let lhs, let rhs), .equal(let lhs, let rhs),
             .notEqual(let lhs, let rhs), .lessThan(let lhs, let rhs),
             .lessThanOrEqual(let lhs, let rhs), .greaterThan(let lhs, let rhs),
             .greaterThanOrEqual(let lhs, let rhs), .and(let lhs, let rhs),
             .or(let lhs, let rhs), .nullIf(let lhs, let rhs):
            try analyze(lhs, into: &analysis)
            try analyze(rhs, into: &analysis)

        case .negate(let value), .not(let value), .isNull(let value),
             .isNotNull(let value), .isTriple(let value), .subject(let value),
             .predicate(let value), .object(let value), .cast(let value, _):
            try analyze(value, into: &analysis)

        case .like(let value, _), .regex(let value, _, _):
            try analyze(value, into: &analysis)

        case .between(let value, let low, let high):
            try analyze(value, into: &analysis)
            try analyze(low, into: &analysis)
            try analyze(high, into: &analysis)

        case .inList(let value, let values), .notInList(let value, let values):
            try analyze(value, into: &analysis)
            for candidate in values {
                try analyze(candidate, into: &analysis)
            }

        case .inSubquery(let value, _):
            try analyze(value, into: &analysis)
            analysis.requiresDataset = true

        case .aggregate(let aggregate):
            try analyze(aggregate, into: &analysis)

        case .function(let call):
            for argument in call.arguments {
                try analyze(argument, into: &analysis)
            }
            let identifier = try SPARQLFunctionIdentifier.resolve(call.name)
            switch identifier {
            case .extensionFunction:
                analysis.usesExtensionFunction = true
            case .datatypeConstructor:
                break
            case .builtIn(let builtIn):
                analysis.merge(volatility: builtIn.volatility)
            }

        case .caseWhen(let pairs, let elseResult):
            for pair in pairs {
                try analyze(pair.condition, into: &analysis)
                try analyze(pair.result, into: &analysis)
            }
            if let elseResult {
                try analyze(elseResult, into: &analysis)
            }

        case .coalesce(let expressions):
            for expression in expressions {
                try analyze(expression, into: &analysis)
            }

        case .triple(let subject, let predicate, let object):
            try analyze(subject, into: &analysis)
            try analyze(predicate, into: &analysis)
            try analyze(object, into: &analysis)

        case .exists(let query):
            analysis.requiresDataset = true
            analysis.compiledExistsPatterns[query] = try
                SPARQLExistsPatternCompiler.compile(query)

        case .subquery:
            analysis.requiresDataset = true

        case .literal, .column, .parameter:
            break
        }
    }

    private static func analyze(
        _ aggregate: AggregateFunction,
        into analysis: inout Analysis
    ) throws {
        switch aggregate {
        case .count(let expression, _):
            if let expression {
                try analyze(expression, into: &analysis)
            }
        case .sum(let expression, _), .avg(let expression, _),
             .min(let expression), .max(let expression),
             .groupConcat(let expression, _, _), .sample(let expression):
            try analyze(expression, into: &analysis)
        case .arrayAgg(let expression, let orderBy, _):
            try analyze(expression, into: &analysis)
            if let orderBy {
                for key in orderBy {
                    try analyze(key.expression, into: &analysis)
                }
            }
        }
    }

    private static func prefixed(_ variable: String) -> String {
        return "?\(variable)"
    }
}
