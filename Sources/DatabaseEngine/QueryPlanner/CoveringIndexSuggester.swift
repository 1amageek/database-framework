import Core

/// Suggests the smallest canonical covering-index extension for a query.
public struct CoveringIndexSuggester<T: Persistable> {
    public init() {}

    public func suggest(
        analysis: QueryAnalysis<T>,
        existingIndexes: [IndexDescriptor]
    ) -> CoveringIndexSuggestion? {
        let modelFields = Set(T.fieldSchemas.map(\.name))

        for index in existingIndexes {
            if CoveringIndexMetadata.build(for: index, type: T.self).isFullyCovering {
                return nil
            }
        }

        var bestCandidate: (index: IndexDescriptor, missingFields: Set<String>)?
        for index in existingIndexes {
            let metadata = CoveringIndexMetadata.build(for: index, type: T.self)
            let missing = modelFields.subtracting(metadata.allFields)
            let conditionFields = Set(analysis.fieldConditions.map(\.fieldName))
            guard !conditionFields.isDisjoint(with: Set(metadata.keyFields)) else {
                continue
            }
            if bestCandidate == nil || missing.count < bestCandidate!.missingFields.count {
                bestCandidate = (index, missing)
            }
        }

        guard let candidate = bestCandidate else {
            return CoveringIndexSuggestion(
                type: .newIndex,
                indexName: nil,
                keyFields: Array(Set(analysis.fieldConditions.map(\.fieldName))).sorted(),
                storedFields: modelFields.sorted(),
                reason: "Create a canonical covering index"
            )
        }
        guard !candidate.missingFields.isEmpty else { return nil }
        return CoveringIndexSuggestion(
            type: .extendExisting,
            indexName: candidate.index.name,
            keyFields: CoveringIndexMetadata.build(
                for: candidate.index,
                type: T.self
            ).keyFields,
            storedFields: candidate.missingFields.sorted(),
            reason: "Store every missing compiled entity field"
        )
    }
}
