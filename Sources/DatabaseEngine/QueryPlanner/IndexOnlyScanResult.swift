import Core

/// Result of canonical covering-index analysis.
public struct IndexOnlyScanResult: Sendable {
    public let canUseIndexOnlyScan: Bool
    public let index: IndexDescriptor
    public let metadata: CoveringIndexMetadata
    public let coveredFields: Set<String>
    public let uncoveredFields: Set<String>
    public let estimatedSavings: Double
}
