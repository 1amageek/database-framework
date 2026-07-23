/// A stored spatial index entry does not satisfy the scanner's physical contract.
public enum SpatialCellScannerError: Error, Sendable, Equatable {
    case missingPrimaryKey
}
