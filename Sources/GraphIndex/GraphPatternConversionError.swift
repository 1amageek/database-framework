public enum GraphPatternConversionError: Error, Sendable, Equatable {
    case undefinedPrefix(String)
    case reifiedTripleRequiresPatternContext
    case graphSelectorMustBeIRIOrVariable
    case invalidGraphIRI(String)
    case unsupportedGraphPattern(String)
    case variableAlreadyInScope(String)
    case duplicateProjectionAlias(String)
    case projectionExpressionRequiresAlias
    case projectionAliasDependency(String)
    case projectionVariableNotInScope(String)
    case duplicateValuesVariable(String)
    case valuesRowWidth(row: Int, expected: Int, actual: Int)
    case valuesCellCountOverflow(rows: Int, columns: Int)
    case unsupportedGroupExpression
    case unsupportedAggregateExpression(String)
}
