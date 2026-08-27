import DatabaseEngine
import DatabaseKit
import DatabaseMath
import DatabaseTypes

/// Executes product-quantized search directly over retained persisted
/// codebooks. Only the query-specific distance table is materialized.
struct PersistedProductQuantizer: Sendable {
    let dimensions: Int
    let subquantizerCount: Int
    let centroidCount: Int
    let dimensionsPerSubquantizer: Int
    private let distanceTableEntryCount: Int

    private let codebooks: DatabaseSharedRetainedArray<PersistedVectorView>

    init(
        dimensions: Int,
        subquantizerCount: Int,
        centroidCount: Int,
        codebooks: DatabaseSharedRetainedArray<PersistedVectorView>
    ) throws(VectorIndexError) {
        guard dimensions > 0,
              subquantizerCount > 0,
              centroidCount > 0,
              centroidCount <= Int(UInt8.max) + 1,
              dimensions % subquantizerCount == 0 else {
            throw .invalidArgument(
                "Persisted PQ dimensions and codebook shape must be positive and compatible"
            )
        }
        guard codebooks.count == subquantizerCount else {
            throw .invalidStructure("Invalid PQ codebook count")
        }
        let dimensionsPerSubquantizer = dimensions / subquantizerCount
        let (expectedValueCount, overflow) = centroidCount
            .multipliedReportingOverflow(by: dimensionsPerSubquantizer)
        guard !overflow else {
            throw .invalidArgument(
                "Persisted PQ codebook shape exceeds the current platform limit"
            )
        }
        guard codebooks.allSatisfy({ $0.count == expectedValueCount }) else {
            throw .invalidStructure("Invalid persisted PQ codebook shape")
        }
        let (distanceTableEntryCount, tableOverflow) = subquantizerCount
            .multipliedReportingOverflow(by: centroidCount)
        guard !tableOverflow else {
            throw .invalidArgument(
                "Persisted PQ distance table exceeds the current platform limit"
            )
        }
        self.dimensions = dimensions
        self.subquantizerCount = subquantizerCount
        self.centroidCount = centroidCount
        self.dimensionsPerSubquantizer = dimensionsPerSubquantizer
        self.distanceTableEntryCount = distanceTableEntryCount
        self.codebooks = codebooks
    }

    func distanceTable(
        for query: Vector,
        metric: VectorMetric
    ) throws -> ProductQuantizedDistanceTable {
        try validate(query)
        var outcome: Result<ProductQuantizedDistanceTable, VectorIndexError>?
        guard query.withFloat32Elements({ queryElements in
            outcome = Result {
                () throws(VectorIndexError)
                    -> ProductQuantizedDistanceTable in
                try makeDistanceTable(
                    queryElements: queryElements,
                    metric: metric
                )
            }
        }) != nil else {
            throw VectorIndexError.invalidStructure(
                "PQ query storage does not match its Float32 element type"
            )
        }
        guard let outcome else {
            preconditionFailure("PQ query borrow produced no result")
        }
        return try outcome.get()
    }

    func encode(_ vector: Vector) throws -> [UInt8] {
        try validate(vector)
        var outcome: Result<[UInt8], VectorIndexError>?
        guard vector.withFloat32Elements({ elements in
            outcome = Result {
                () throws(VectorIndexError) -> [UInt8] in
                try encode(elements)
            }
        }) != nil else {
            throw VectorIndexError.invalidStructure(
                "PQ source storage does not match its Float32 element type"
            )
        }
        guard let outcome else {
            preconditionFailure("PQ source borrow produced no result")
        }
        return try outcome.get()
    }

    /// Verifies a persisted code against its canonical vector without
    /// materializing either payload.
    func matchesPersistedCode(
        _ code: ByteString,
        vector: PersistedVectorView
    ) throws -> Bool {
        var outcome: Result<Bool, VectorIndexError>?
        code.withUnsafeBytes { codeBytes in
            outcome = Result {
                () throws(VectorIndexError) -> Bool in
                try matchesPersistedCode(
                    codeBytes,
                    vector: vector
                )
            }
        }
        guard let outcome else {
            preconditionFailure("PQ code verification produced no result")
        }
        return try outcome.get()
    }

    /// Validates and scores one retained code through a single code borrow.
    func validatedDistance(
        for code: ByteString,
        vector: PersistedVectorView,
        using table: ProductQuantizedDistanceTable
    ) throws -> Double {
        var outcome: Result<Double, any Error>?
        code.withUnsafeBytes { codeBytes in
            outcome = Result {
                guard try matchesPersistedCode(
                    codeBytes,
                    vector: vector
                ) else {
                    throw VectorIndexError.invalidStructure(
                        "PQ code disagrees with its persisted canonical vector"
                    )
                }
                return try distance(
                    for: codeBytes.bindMemory(to: UInt8.self),
                    using: table
                )
            }
        }
        guard let outcome else {
            preconditionFailure("PQ validated distance produced no result")
        }
        return try outcome.get()
    }

    private func matchesPersistedCode(
        _ codeBytes: UnsafeRawBufferPointer,
        vector: PersistedVectorView
    ) throws(VectorIndexError) -> Bool {
        guard codeBytes.count == subquantizerCount else {
            throw VectorIndexError.invalidStructure(
                "Invalid PQ code length"
            )
        }
        guard vector.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(
                expected: dimensions,
                actual: vector.count
            )
        }

        return try vector.withElements {
            (vectorElements) throws(VectorIndexError) -> Bool in
            for (subquantizerIndex, codebook) in codebooks.enumerated() {
                let vectorOffset = subquantizerIndex
                    * dimensionsPerSubquantizer
                let nearest = try codebook.withElements {
                    (codebookElements) throws(VectorIndexError) -> Int in
                    var bestIndex = 0
                    var bestDistance = Double.infinity
                    for centroidIndex in 0..<centroidCount {
                        let centroidOffset = centroidIndex
                            * dimensionsPerSubquantizer
                        var distance = 0.0
                        for componentIndex in 0..<dimensionsPerSubquantizer {
                            let source = Double(
                                try vectorElements.element(
                                    at: vectorOffset + componentIndex
                                )
                            )
                            let centroid = Double(
                                try codebookElements.element(
                                    at: centroidOffset + componentIndex
                                )
                            )
                            let difference = source - centroid
                            distance += difference * difference
                        }
                        if distance < bestDistance {
                            bestDistance = distance
                            bestIndex = centroidIndex
                        }
                    }
                    return bestIndex
                }
                guard codeBytes[subquantizerIndex]
                        == UInt8(nearest) else {
                    return false
                }
            }
            return true
        }
    }

    func distance(
        for codes: ByteString,
        using table: ProductQuantizedDistanceTable
    ) throws -> Double {
        var outcome: Result<Double, ProductQuantizationError>?
        codes.withUnsafeBytes { bytes in
            outcome = Result {
                () throws(ProductQuantizationError) -> Double in
                try distance(
                    for: bytes.bindMemory(to: UInt8.self),
                    using: table
                )
            }
        }
        guard let outcome else {
            preconditionFailure("PQ code borrow produced no result")
        }
        return try outcome.get()
    }

    func distance<Codes: RandomAccessCollection>(
        for codes: Codes,
        using table: ProductQuantizedDistanceTable
    ) throws(ProductQuantizationError) -> Double
    where Codes.Element == UInt8, Codes.Index == Int {
        guard codes.count == subquantizerCount else {
            throw ProductQuantizationError.codeCountMismatch(
                expected: subquantizerCount,
                actual: codes.count
            )
        }
        guard table.subquantizerCount == subquantizerCount,
              table.centroidCount == centroidCount,
              table.contributions.count == distanceTableEntryCount,
              table.metric != .cosine || (
                  table.centroidNormsSquared.count
                      == distanceTableEntryCount
              ) else {
            throw ProductQuantizationError.incompatibleDistanceTable
        }

        var contribution = 0.0
        var reconstructedNormSquared = 0.0
        for (subquantizerIndex, code) in codes.enumerated() {
            let centroidIndex = Int(code)
            guard centroidIndex < centroidCount else {
                throw ProductQuantizationError.centroidCodeOutOfRange(
                    subspace: subquantizerIndex,
                    code: centroidIndex,
                    centroidCount: centroidCount
                )
            }
            let tableIndex = subquantizerIndex * centroidCount
                + centroidIndex
            contribution += table.contributions[tableIndex]
            if table.metric == .cosine {
                reconstructedNormSquared += table.centroidNormsSquared[
                    tableIndex
                ]
            }
        }

        switch table.metric {
        case .euclidean:
            return DatabaseMath.squareRoot(contribution)
        case .dotProduct:
            return -contribution
        case .cosine:
            return productQuantizedCosineDistance(
                dotProduct: contribution,
                queryNormSquared: table.queryNormSquared,
                reconstructedNormSquared: reconstructedNormSquared
            )
        }
    }

    private func validate(_ vector: Vector) throws {
        guard vector.elementType == .float32 else {
            throw VectorIndexError.invalidArgument(
                "PQ execution requires Float32 elements"
            )
        }
        guard vector.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(
                expected: dimensions,
                actual: vector.count
            )
        }
    }

    private func makeDistanceTable(
        queryElements: UnsafeBufferPointer<Float>,
        metric: VectorMetric
    ) throws(VectorIndexError) -> ProductQuantizedDistanceTable {
        var contributions: [Double] = []
        contributions.reserveCapacity(distanceTableEntryCount)
        var centroidNormsSquared: [Double] = []
        if metric == .cosine {
            centroidNormsSquared.reserveCapacity(distanceTableEntryCount)
        }

        var queryNormSquared = 0.0
        if metric == .cosine {
            for component in queryElements {
                let widened = Double(component)
                queryNormSquared += widened * widened
            }
        }

        for (subquantizerIndex, codebook) in codebooks.enumerated() {
            let queryOffset = subquantizerIndex * dimensionsPerSubquantizer
            try codebook.withElements {
                (codebookElements) throws(VectorIndexError) -> Void in
                for centroidIndex in 0..<centroidCount {
                    let centroidOffset = centroidIndex
                        * dimensionsPerSubquantizer
                    var contribution = 0.0
                    var centroidNormSquared = 0.0
                    for componentIndex in 0..<dimensionsPerSubquantizer {
                        let queryComponent = Double(
                            queryElements[queryOffset + componentIndex]
                        )
                        let centroidComponent = Double(
                            try codebookElements.element(
                                at: centroidOffset + componentIndex
                            )
                        )
                        switch metric {
                        case .euclidean:
                            let difference = queryComponent - centroidComponent
                            contribution += difference * difference
                        case .cosine, .dotProduct:
                            contribution += queryComponent * centroidComponent
                        }
                        if metric == .cosine {
                            centroidNormSquared += centroidComponent
                                * centroidComponent
                        }
                    }
                    contributions.append(contribution)
                    if metric == .cosine {
                        centroidNormsSquared.append(centroidNormSquared)
                    }
                }
            }
        }

        return ProductQuantizedDistanceTable(
            metric: metric,
            contributions: contributions,
            queryNormSquared: queryNormSquared,
            centroidNormsSquared: centroidNormsSquared,
            subquantizerCount: subquantizerCount,
            centroidCount: centroidCount
        )
    }

    private func encode(
        _ elements: UnsafeBufferPointer<Float>
    ) throws(VectorIndexError) -> [UInt8] {
        var result: [UInt8] = []
        result.reserveCapacity(subquantizerCount)
        for (subquantizerIndex, codebook) in codebooks.enumerated() {
            let vectorOffset = subquantizerIndex * dimensionsPerSubquantizer
            let nearest = try codebook.withElements {
                (codebookElements) throws(VectorIndexError) -> Int in
                var bestIndex = 0
                var bestDistance = Double.infinity
                for centroidIndex in 0..<centroidCount {
                    let centroidOffset = centroidIndex
                        * dimensionsPerSubquantizer
                    var distance = 0.0
                    for componentIndex in 0..<dimensionsPerSubquantizer {
                        let difference = Double(
                            elements[vectorOffset + componentIndex]
                        ) - Double(
                            try codebookElements.element(
                                at: centroidOffset + componentIndex
                            )
                        )
                        distance += difference * difference
                    }
                    if distance < bestDistance {
                        bestDistance = distance
                        bestIndex = centroidIndex
                    }
                }
                return bestIndex
            }
            result.append(UInt8(nearest))
        }
        return result
    }
}
