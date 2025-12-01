import FoundationDB

/// Visitor pattern for traversing and evaluating KeyExpressions
///
/// KeyExpressionVisitor provides a unified way to traverse and process
/// KeyExpression trees without depending on the concrete types.
public protocol KeyExpressionVisitor {
    associatedtype Result

    /// Visit a field expression
    func visitField(_ fieldName: String) throws -> Result

    /// Visit a concatenation of multiple expressions
    func visitConcatenate(_ expressions: [KeyExpression]) throws -> Result

    /// Visit a literal expression
    func visitLiteral(_ value: any TupleElement) throws -> Result

    /// Visit an empty expression
    func visitEmpty() throws -> Result

    /// Visit a nest expression
    func visitNest(_ parentField: String, _ child: KeyExpression) throws -> Result

    /// Visit a range boundary expression
    func visitRangeBoundary(_ fieldName: String, _ component: RangeComponent) throws -> Result
}

// MARK: - Default Implementation

extension KeyExpressionVisitor {
    /// Default implementation of visitRangeBoundary that throws an error
    public func visitRangeBoundary(_ fieldName: String, _ component: RangeComponent) throws -> Result {
        throw DataAccessError.rangeFieldsNotSupported(
            itemType: "Unknown",
            suggestion: "Override visitRangeBoundary() to support Range indexes."
        )
    }
}

// MARK: - KeyExpression Visitor Support

extension KeyExpression {
    /// Accept a visitor to traverse this expression
    public func accept<V: KeyExpressionVisitor>(visitor: V) throws -> V.Result {
        switch self {
        case let field as FieldKeyExpression:
            return try visitor.visitField(field.fieldName)

        case let concat as ConcatenateKeyExpression:
            return try visitor.visitConcatenate(concat.children)

        case is EmptyKeyExpression:
            return try visitor.visitEmpty()

        case let nest as NestExpression:
            return try visitor.visitNest(nest.parentField, nest.child)

        case let rangeExpr as RangeKeyExpression:
            return try visitor.visitRangeBoundary(rangeExpr.fieldName, rangeExpr.component)

        default:
            // Handle all LiteralKeyExpression types generically
            if let literalBase = self as? any LiteralKeyExpressionBase {
                return try visitor.visitLiteral(literalBase.anyValue)
            }

            throw DataAccessError.fieldNotFound(
                itemType: "Unknown",
                keyPath: "Unsupported KeyExpression type: \(type(of: self))"
            )
        }
    }
}

// MARK: - LiteralKeyExpression Base Protocol

/// Internal protocol to enable type-erased access to LiteralKeyExpression values
fileprivate protocol LiteralKeyExpressionBase {
    var anyValue: any TupleElement { get }
}

extension LiteralKeyExpression: LiteralKeyExpressionBase {
    fileprivate var anyValue: any TupleElement {
        return value
    }
}
