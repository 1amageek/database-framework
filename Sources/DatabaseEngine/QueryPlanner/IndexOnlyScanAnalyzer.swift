import Core

/// Finds indexes whose canonical projection contains every compiled record field.
public struct IndexOnlyScanAnalyzer<T: Persistable> {
    public init() {}

    public func analyze(
        analysis: QueryAnalysis<T>,
        index: IndexDescriptor
    ) -> IndexOnlyScanResult {
        let metadata = CoveringIndexMetadata.build(for: index, type: T.self)
        let modelFields = Set(T.fieldSchemas.map(\.name))
        let indexFields = metadata.allFields
        let coveredFields = modelFields.intersection(indexFields)
        let uncoveredFields = modelFields.subtracting(indexFields)

        return IndexOnlyScanResult(
            canUseIndexOnlyScan: metadata.isFullyCovering,
            index: index,
            metadata: metadata,
            coveredFields: coveredFields,
            uncoveredFields: uncoveredFields,
            estimatedSavings: metadata.isFullyCovering ? 0.90 : 0
        )
    }
}
