import DatabaseKit

/// A checked expression signature. Slot indices address the binding schema,
/// independently of public column numbers and labels.
struct CanonicalBoundExpression: Sendable {
    let expression: DatabaseKit.Expression
    let type: QueryColumnType
    let isNullable: Bool
    let columnSlot: Int?
    let children: [CanonicalBoundExpression]

    init(
        expression: DatabaseKit.Expression,
        type: QueryColumnType,
        isNullable: Bool,
        columnSlot: Int? = nil,
        children: [CanonicalBoundExpression] = []
    ) {
        self.expression = expression
        self.type = type
        self.isNullable = isNullable
        self.columnSlot = columnSlot
        self.children = children
    }

    /// Normalize value-dependent primitive evaluator representations to the
    /// checked signature before publishing a positional cell.
    func normalize(_ value: QueryValue) throws -> QueryValue {
        guard isNullable || !value.isNull else {
            throw DatabaseExpressionEvaluationError.typeMismatch(
                operation: "nonnullable bound expression"
            )
        }
        let permitsFloatingRounding: Bool
        if case .cast = expression { permitsFloatingRounding = true }
        else { permitsFloatingRounding = false }
        return try CanonicalQueryTypeBinder.normalize(
            value, to: type, permitsFloatingRounding: permitsFloatingRounding
        )
    }
}
