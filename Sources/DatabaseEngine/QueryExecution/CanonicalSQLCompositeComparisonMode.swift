/// Constructor comparisons propagate UNKNOWN; record comparisons order NULL fields.
enum CanonicalSQLCompositeComparisonMode: Sendable {
    case constructor
    case record
}
