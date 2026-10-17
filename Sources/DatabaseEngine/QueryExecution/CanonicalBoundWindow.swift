import DatabaseKit

/// Immutable selected analytic occurrence; object identity is authoritative.
final class CanonicalBoundWindow: Sendable {
    let call: WindowCall
    let arguments: [CanonicalBoundExpression]
    let aggregate: CanonicalBoundExpression?
    let partitionBy: [CanonicalBoundExpression]
    let orderBy: [CanonicalBoundKey]
    let frame: CanonicalBoundWindowFrame?
    let identity: Int

    static var storageByteCount: UInt64 {
        UInt64(MemoryLayout<WindowCall>.stride + MemoryLayout<[CanonicalBoundExpression]>.stride * 2
            + MemoryLayout<CanonicalBoundExpression?>.stride + MemoryLayout<[CanonicalBoundKey]>.stride
            + MemoryLayout<CanonicalBoundWindowFrame?>.stride + MemoryLayout<Int>.stride * 3)
    }

    init(call: WindowCall, arguments: [CanonicalBoundExpression],
         aggregate: CanonicalBoundExpression?, partitionBy: [CanonicalBoundExpression],
         orderBy: [CanonicalBoundKey], frame: CanonicalBoundWindowFrame?, identity: Int) {
        self.call = call
        self.arguments = arguments
        self.aggregate = aggregate
        self.partitionBy = partitionBy
        self.orderBy = orderBy
        self.frame = frame
        self.identity = identity
    }
}

struct CanonicalBoundWindowFrame: Sendable {
    let mode: WindowFrameMode
    let start: WindowFrameBound
    let end: WindowFrameBound
    let startOffset: CanonicalBoundExpression?
    let endOffset: CanonicalBoundExpression?
    let exclusion: WindowFrameExclusion
}

struct CanonicalBoundWindowDefinition: Sendable {
    let name: String
    let partitionBy: [CanonicalBoundExpression]
    let orderBy: [CanonicalBoundKey]
    let frame: CanonicalBoundWindowFrame?
    let identity: Int
}
