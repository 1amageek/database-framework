import DatabaseKit

package struct SPARQLAlgebraCompilationContext {
    package let structuralLimits: QueryStructuralLimits
    package let expressionLimits: SPARQLExpressionCompilationLimits
    private var nextBlankNodeScope: UInt64 = 0
    private var nextSubqueryOccurrence: UInt64 = 0

    package init(structuralLimits: QueryStructuralLimits) {
        self.structuralLimits = structuralLimits
        self.expressionLimits = SPARQLExpressionCompilationLimits(
            structuralLimits: structuralLimits
        )
    }

    package init(expressionLimits: SPARQLExpressionCompilationLimits) {
        self.structuralLimits = expressionLimits.structuralLimits
        self.expressionLimits = expressionLimits
    }

    package mutating func takeBlankNodeScope() -> UInt64 {
        let identifier = nextBlankNodeScope
        nextBlankNodeScope += 1
        return identifier
    }

    package mutating func takeSubqueryOccurrence() -> UInt64 {
        let identifier = nextSubqueryOccurrence
        nextSubqueryOccurrence += 1
        return identifier
    }
}
