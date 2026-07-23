package struct SPARQLAlgebraCompilationContext {
    private var nextBlankNodeScope: UInt64 = 0
    private var nextSubqueryOccurrence: UInt64 = 0

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
