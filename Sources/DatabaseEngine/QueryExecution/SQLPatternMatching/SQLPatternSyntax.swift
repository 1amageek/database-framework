enum SQLPatternSyntax: Sendable {
    case like(caseInsensitive: Bool, escape: Unicode.Scalar?)
    case similar(escape: Unicode.Scalar?)
    case ere(caseInsensitive: Bool)
}

enum SQLPatternOperation: Sendable {
    case predicate, match, matches, replace, split
}
