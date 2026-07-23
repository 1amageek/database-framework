/// Result of applying a SPARQL value comparison operator.
///
/// `unordered` represents a defined comparison whose operands have no order,
/// such as a comparison involving NaN or an indeterminate XSD dateTime.
/// `typeError` represents operands for which SPARQL defines no comparison.
enum SPARQLValueComparison: Sendable, Equatable {
    case less
    case equal
    case greater
    case unordered
    case typeError
}
