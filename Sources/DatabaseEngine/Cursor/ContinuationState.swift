import StorageKit

internal struct ContinuationState: Sendable {
    let version: UInt8
    let scanType: ScanType
    let lastKey: Bytes
    let reverse: Bool
    let remainingLimit: Int?
    let originalLimit: Int?
    let planFingerprint: Bytes
    let operatorState: OperatorContinuationState?

    enum ScanType: UInt8, Sendable {
        case tableScan = 0
        case indexScan = 1
        case indexSeek = 2
        case indexOnlyScan = 3
        case fullTextScan = 4
        case vectorSearch = 5
        case spatialScan = 6
        case union = 7
        case intersection = 8
        case rankScan = 9

        var name: String {
            switch self {
            case .tableScan: return "tableScan"
            case .indexScan: return "indexScan"
            case .indexSeek: return "indexSeek"
            case .indexOnlyScan: return "indexOnlyScan"
            case .fullTextScan: return "fullTextScan"
            case .vectorSearch: return "vectorSearch"
            case .spatialScan: return "spatialScan"
            case .union: return "union"
            case .intersection: return "intersection"
            case .rankScan: return "rankScan"
            }
        }
    }

    init(
        version: UInt8 = ContinuationToken.currentVersion,
        scanType: ScanType,
        lastKey: Bytes,
        reverse: Bool = false,
        remainingLimit: Int? = nil,
        originalLimit: Int? = nil,
        planFingerprint: Bytes,
        operatorState: OperatorContinuationState? = nil
    ) {
        self.version = version
        self.scanType = scanType
        self.lastKey = lastKey
        self.reverse = reverse
        self.remainingLimit = remainingLimit
        self.originalLimit = originalLimit
        self.planFingerprint = planFingerprint
        self.operatorState = operatorState
    }

    func toToken() throws -> ContinuationToken {
        ContinuationToken(data: try ContinuationStateCodec.encode(self))
    }

    static func fromToken(_ token: ContinuationToken) throws -> ContinuationState {
        guard !token.isEndOfResults else {
            throw ContinuationError.invalidTokenFormat
        }
        do {
            return try ContinuationStateCodec.decode(token.data)
        } catch let error as ContinuationError {
            throw error
        } catch {
            throw ContinuationError.corruptedToken
        }
    }

    var progress: Double? {
        guard let original = originalLimit,
              let remaining = remainingLimit,
              original > 0 else {
            return nil
        }
        return Double(original - remaining) / Double(original)
    }
}

internal struct OperatorContinuationState: Sendable {
    let unionChildIndex: Int?
    let childContinuation: Bytes?
    let exhaustedChildren: [Int]?
    let intersectionIds: [Bytes]?

    init(
        unionChildIndex: Int? = nil,
        childContinuation: Bytes? = nil,
        exhaustedChildren: [Int]? = nil,
        intersectionIds: [Bytes]? = nil
    ) {
        self.unionChildIndex = unionChildIndex
        self.childContinuation = childContinuation
        self.exhaustedChildren = exhaustedChildren
        self.intersectionIds = intersectionIds
    }
}

internal struct PlanFingerprint {
    static func compute(
        operatorDescription: String,
        indexNames: [String],
        sortFields: [String]
    ) -> Bytes {
        var hasher = DeterministicHasher()
        hasher.combine(operatorDescription)
        hasher.combine(indexNames.sorted())
        hasher.combine(sortFields)
        return hasher.finalizeToBytes()
    }
}
