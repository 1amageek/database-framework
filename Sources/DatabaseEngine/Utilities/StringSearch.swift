internal enum DatabaseStringSearch {
    static func contains(_ pattern: String, in source: String) -> Bool {
        let sourceBytes = source.utf8
        let patternBytes = pattern.utf8
        guard !patternBytes.isEmpty else { return true }
        guard patternBytes.count <= sourceBytes.count else { return false }

        var candidateStart = sourceBytes.startIndex
        while candidateStart != sourceBytes.endIndex {
            var sourceIndex = candidateStart
            var patternIndex = patternBytes.startIndex
            while sourceIndex != sourceBytes.endIndex,
                  patternIndex != patternBytes.endIndex,
                  sourceBytes[sourceIndex] == patternBytes[patternIndex] {
                sourceBytes.formIndex(after: &sourceIndex)
                patternBytes.formIndex(after: &patternIndex)
            }
            if patternIndex == patternBytes.endIndex {
                return true
            }
            sourceBytes.formIndex(after: &candidateStart)
        }
        return false
    }
}
