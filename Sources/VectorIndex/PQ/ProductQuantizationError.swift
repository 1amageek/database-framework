/// Failures produced while training or evaluating product quantization.
public enum ProductQuantizationError: Error, Sendable, Equatable, CustomStringConvertible {
    case invalidDimensions(Int)
    case incompatibleSubspaceCount(dimensions: Int, subquantizers: Int)
    case emptyTrainingSet
    case vectorDimensionMismatch(expected: Int, actual: Int)
    case untrained
    case emptyCodebooks
    case inconsistentCentroidCount(subspace: Int, expected: Int, actual: Int)
    case invalidCentroidCount(Int)
    case centroidDimensionMismatch(subspace: Int, centroid: Int, expected: Int, actual: Int)
    case codeCountMismatch(expected: Int, actual: Int)
    case centroidCodeOutOfRange(subspace: Int, code: Int, centroidCount: Int)
    case subspaceOutOfRange(Int)
    case centroidOutOfRange(subspace: Int, centroid: Int)
    case incompatibleDistanceTable

    public var description: String {
        switch self {
        case .invalidDimensions(let dimensions):
            return "Product quantization dimensions must be positive, got \(dimensions)"
        case .incompatibleSubspaceCount(let dimensions, let subquantizers):
            return "Vector dimensions \(dimensions) are not divisible by \(subquantizers) subquantizers"
        case .emptyTrainingSet:
            return "Product quantization requires at least one training vector"
        case .vectorDimensionMismatch(let expected, let actual):
            return "Product quantization vector dimension mismatch: expected \(expected), got \(actual)"
        case .untrained:
            return "Product quantizer has no trained codebooks"
        case .emptyCodebooks:
            return "Product quantization codebooks must not be empty"
        case .inconsistentCentroidCount(let subspace, let expected, let actual):
            return "Product quantization subspace \(subspace) has \(actual) centroids; expected \(expected)"
        case .invalidCentroidCount(let count):
            return "Product quantization requires between 1 and 256 centroids per subspace, got \(count)"
        case .centroidDimensionMismatch(let subspace, let centroid, let expected, let actual):
            return "Product quantization centroid \(centroid) in subspace \(subspace) has dimension \(actual); expected \(expected)"
        case .codeCountMismatch(let expected, let actual):
            return "Product quantization code count mismatch: expected \(expected), got \(actual)"
        case .centroidCodeOutOfRange(let subspace, let code, let centroidCount):
            return "Product quantization code \(code) in subspace \(subspace) exceeds \(centroidCount) centroids"
        case .subspaceOutOfRange(let subspace):
            return "Product quantization subspace \(subspace) is out of range"
        case .centroidOutOfRange(let subspace, let centroid):
            return "Product quantization centroid \(centroid) is out of range for subspace \(subspace)"
        case .incompatibleDistanceTable:
            return "Product quantization distance table does not match the quantizer layout"
        }
    }
}
