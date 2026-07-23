import DatabaseEngine

/// Bounded iterative validation for a recursive property-path expression.
final class SPARQLPropertyPathStructureValidator {
    private struct WorkItem {
        let path: ExecutionPropertyPath
        let depth: Int
    }

    private static let containerByteCount: UInt64 = 64
    private static let slotByteCount: UInt64 = 128
    private static let initialCapacity = 4

    private let workMeter: DatabaseWorkMeter
    private var reservation: DatabaseIntermediateReservation?
    private var worklist: [WorkItem]
    private var accountedCapacity: Int

    private init(
        workMeter: DatabaseWorkMeter,
        reservation: DatabaseIntermediateReservation
    ) {
        self.workMeter = workMeter
        self.reservation = reservation
        self.worklist = []
        self.accountedCapacity = 0
    }

    static func make(
        workMeter: DatabaseWorkMeter
    ) throws -> SPARQLPropertyPathStructureValidator {
        let reservation = try workMeter.reserveIntermediate(
            bytes: containerByteCount,
            at: .pathExpansion
        )
        return SPARQLPropertyPathStructureValidator(
            workMeter: workMeter,
            reservation: reservation
        )
    }

    func validate(
        _ path: ExecutionPropertyPath,
        maximumDepth: Int
    ) throws {
        precondition(worklist.isEmpty)
        defer { worklist.removeAll(keepingCapacity: true) }
        try append(WorkItem(path: path, depth: 0))
        while let item = worklist.popLast() {
            try workMeter.consume(at: .pathExpansion)
            guard item.depth <= maximumDepth else {
                throw SPARQLQueryError
                    .propertyPathExpressionDepthLimitExceeded(
                        maximum: maximumDepth
                    )
            }
            let (childDepth, overflow) = item.depth
                .addingReportingOverflow(1)
            guard !overflow else {
                throw SPARQLQueryError
                    .propertyPathExpressionDepthLimitExceeded(
                        maximum: maximumDepth
                    )
            }
            switch item.path {
            case .empty, .iri, .negatedPropertySet:
                break
            case .inverse(let child),
                 .zeroOrMore(let child),
                 .oneOrMore(let child),
                 .zeroOrOne(let child),
                 .range(let child, _):
                try append(WorkItem(path: child, depth: childDepth))
            case .sequence(let left, let right),
                 .alternative(let left, let right):
                try append(WorkItem(path: right, depth: childDepth))
                try append(WorkItem(path: left, depth: childDepth))
            }
        }
    }

    func shutdown() {
        worklist.removeAll(keepingCapacity: false)
        accountedCapacity = 0
        reservation?.release()
        reservation = nil
    }

    deinit { shutdown() }

    private func append(_ item: consuming WorkItem) throws {
        if worklist.count == accountedCapacity {
            let requiredCapacity: Int
            if accountedCapacity == 0 {
                requiredCapacity = Self.initialCapacity
            } else {
                let (next, overflow) = accountedCapacity
                    .multipliedReportingOverflow(by: 2)
                guard !overflow else {
                    throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                        currentCapacity: accountedCapacity
                    )
                }
                requiredCapacity = next
            }
            let additionalSlots = UInt64(
                requiredCapacity - accountedCapacity
            )
            let bytes = try DatabaseIntermediateFootprint(
                bytes: Self.slotByteCount
            ).multiplied(by: additionalSlots).bytes
            guard let reservation else {
                preconditionFailure(
                    "Property-path structure validator used after shutdown"
                )
            }
            try reservation.reserveAdditional(
                bytes: bytes,
                at: .pathExpansion
            )
            worklist.reserveCapacity(requiredCapacity)
            accountedCapacity = requiredCapacity
        }
        worklist.append(item)
    }
}
