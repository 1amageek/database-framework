import DatabaseEngine
import DatabaseKit
import DatabaseTypes

/// Request-accounted SHACL results retained across recursive validation.
/// Segments share their immutable payload reservation when parent validators
/// aggregate child results, avoiding both payload copies and double charging.
struct SHACLRetainedValidationResults: Sendable {
    private let segments:
        DatabaseSharedRetainedArray<
            DatabaseSharedRetainedArray<SHACLValidationResult>
        >
    let count: Int

    init(
        segments: DatabaseSharedRetainedArray<
            DatabaseSharedRetainedArray<SHACLValidationResult>
        >,
        count: Int
    ) {
        self.segments = segments
        self.count = count
    }

    var isEmpty: Bool { count == 0 }

    static func empty(
        workMeter: DatabaseWorkMeter
    ) throws -> SHACLRetainedValidationResults {
        let builder = try SHACLValidationResultCollectionBuilder(
            workMeter: workMeter
        )
        return try builder.finish()
    }

    static func retaining(
        _ result: SHACLValidationResult,
        workMeter: DatabaseWorkMeter
    ) throws -> SHACLRetainedValidationResults {
        var builder = try SHACLValidationResultLeafBuilder(
            workMeter: workMeter
        )
        try builder.append(result)
        return try builder.finish()
    }

    func appendSegments(
        to builder: inout DatabaseRetainedArrayBuilder<
            DatabaseSharedRetainedArray<SHACLValidationResult>
        >
    ) throws {
        for segment in segments {
            try builder.append(
                footprint: DatabaseIntermediateFootprint(),
                at: .resultMaterialization,
                make: { segment }
            )
        }
    }

    consuming func promoteToOutput() -> [SHACLValidationResult] {
        var output: [SHACLValidationResult] = []
        output.reserveCapacity(count)
        for segment in segments {
            for result in segment {
                output.append(result)
            }
        }
        return output
    }
}

struct SHACLValidationResultCollectionBuilder: ~Copyable {
    private var segments:
        DatabaseRetainedArrayBuilder<
            DatabaseSharedRetainedArray<SHACLValidationResult>
        >
    private var resultCount: Int
    private let stage: DatabaseWorkStage

    init(
        workMeter: DatabaseWorkMeter,
        expectedSegmentCount: Int = 0
    ) throws {
        self.segments = try DatabaseRetainedArrayBuilder(
            workMeter: workMeter,
            stage: .resultMaterialization,
            layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
                for: DatabaseSharedRetainedArray<SHACLValidationResult>.self
            ),
            expectedCount: expectedSegmentCount
        )
        self.resultCount = 0
        self.stage = .resultMaterialization
    }

    mutating func append(
        contentsOf results: SHACLRetainedValidationResults
    ) throws {
        let (newCount, overflow) = resultCount.addingReportingOverflow(
            results.count
        )
        guard !overflow else {
            throw SHACLRetainedValidationResultStorageError.capacityOverflow
        }
        try results.appendSegments(to: &segments)
        resultCount = newCount
    }

    consuming func finish() throws -> SHACLRetainedValidationResults {
        let shared = try segments.finish().moveToSharedOwnership(at: stage)
        return SHACLRetainedValidationResults(
            segments: shared,
            count: resultCount
        )
    }
}

enum SHACLRetainedValidationResultStorageError: Error, Sendable, Equatable {
    case capacityOverflow
}

struct SHACLValidationResultLeafBuilder: ~Copyable {
    private var results: DatabaseRetainedArrayBuilder<SHACLValidationResult>
    private let footprintMeter: SHACLValidationResultFootprintMeter
    private let workMeter: DatabaseWorkMeter

    init(workMeter: DatabaseWorkMeter) throws {
        let results = try DatabaseRetainedArrayBuilder<SHACLValidationResult>(
            workMeter: workMeter,
            stage: .resultMaterialization,
            layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
                for: SHACLValidationResult.self
            )
        )
        let footprintMeter = try SHACLValidationResultFootprintMeter.make(
            workMeter: workMeter
        )
        self.results = consume results
        self.footprintMeter = footprintMeter
        self.workMeter = workMeter
    }

    mutating func append(
        _ result: SHACLValidationResult
    ) throws {
        let footprint = try footprintMeter.footprint(of: result)
        try results.append(
            footprint: footprint,
            at: .resultMaterialization,
            make: { result }
        )
    }

    consuming func finish() throws -> SHACLRetainedValidationResults {
        let segment = try results.finish().moveToSharedOwnership(
            at: .resultMaterialization
        )
        footprintMeter.shutdown()
        guard !segment.isEmpty else {
            let collection = try SHACLValidationResultCollectionBuilder(
                workMeter: workMeter
            )
            return try collection.finish()
        }
        let leafSegments = try Self.singleSegmentOwner(
            segment,
            workMeter: workMeter
        )
        return SHACLRetainedValidationResults(
            segments: leafSegments,
            count: segment.count
        )
    }

    private static func singleSegmentOwner(
        _ segment: DatabaseSharedRetainedArray<SHACLValidationResult>,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseSharedRetainedArray<
        DatabaseSharedRetainedArray<SHACLValidationResult>
    > {
        var segments = try DatabaseRetainedArrayBuilder<
            DatabaseSharedRetainedArray<SHACLValidationResult>
        >(
            workMeter: workMeter,
            stage: .resultMaterialization,
            layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
                for: DatabaseSharedRetainedArray<SHACLValidationResult>.self
            ),
            expectedCount: 1
        )
        try segments.append(
            footprint: DatabaseIntermediateFootprint(),
            make: { segment }
        )
        return try segments.finish().moveToSharedOwnership(
            at: .resultMaterialization
        )
    }
}

final class SHACLValidationResultFootprintMeter {
    private static let resultOwnerByteCount: UInt64 = 192
    private static let optionalPathOwnerByteCount: UInt64 = 32
    private static let pathNodeByteCount: UInt64 = 64
    private static let pathListContainerByteCount: UInt64 = 64
    private static let pathListElementByteCount: UInt64 = 32
    private static let stringOwnerByteCount: UInt64 = 16
    private static let messageArrayByteCount: UInt64 = 64
    private static let messageSlotByteCount: UInt64 = 16

    private let valueMeter: SPARQLBindingFootprintMeter

    private init(valueMeter: SPARQLBindingFootprintMeter) {
        self.valueMeter = valueMeter
    }

    static func make(
        workMeter: DatabaseWorkMeter
    ) throws -> SHACLValidationResultFootprintMeter {
        SHACLValidationResultFootprintMeter(
            valueMeter: try SPARQLBindingFootprintMeter.make(
                workMeter: workMeter,
                stage: .resultMaterialization
            )
        )
    }

    func footprint(
        of result: borrowing SHACLValidationResult
    ) throws -> DatabaseIntermediateFootprint {
        var footprint = DatabaseIntermediateFootprint(
            rows: 1,
            bytes: Self.resultOwnerByteCount
        )
        footprint = try footprint.adding(
            valueFootprint(of: result.focusNode)
        )
        if let path = result.resultPath {
            footprint = try footprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: Self.optionalPathOwnerByteCount
                )
            )
            footprint = try footprint.adding(pathFootprint(of: path))
        }
        if let value = result.value {
            footprint = try footprint.adding(valueFootprint(of: value))
        }
        if let sourceShape = result.sourceShape {
            footprint = try footprint.adding(valueFootprint(of: sourceShape))
        }
        footprint = try footprint.adding(
            stringFootprint(result.sourceConstraintComponent)
        )
        footprint = try footprint.adding(
            DatabaseIntermediateFootprint(
                bytes: Self.messageArrayByteCount
            )
        )
        for message in result.resultMessage {
            footprint = try footprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: Self.messageSlotByteCount
                )
            )
            footprint = try footprint.adding(stringFootprint(message))
        }
        return footprint
    }

    func shutdown() {
        valueMeter.shutdown()
    }

    private func valueFootprint(
        of term: borrowing RDFTerm
    ) throws -> DatabaseIntermediateFootprint {
        try valueMeter.footprint(of: FieldValue.rdfTerm(copy term))
    }

    private func pathFootprint(
        of path: borrowing SHACLPath
    ) throws -> DatabaseIntermediateFootprint {
        var footprint = DatabaseIntermediateFootprint(
            bytes: Self.pathNodeByteCount
        )
        switch path {
        case .predicate(let iri):
            return try footprint.adding(stringFootprint(iri.rawValue))
        case .inverse(let inner),
             .zeroOrMore(let inner),
             .oneOrMore(let inner),
             .zeroOrOne(let inner):
            return try footprint.adding(pathFootprint(of: inner))
        case .sequence(let paths), .alternative(let paths):
            footprint = try footprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: Self.pathListContainerByteCount
                )
            )
            for child in paths.elements {
                footprint = try footprint.adding(
                    DatabaseIntermediateFootprint(
                        bytes: Self.pathListElementByteCount
                    )
                )
                footprint = try footprint.adding(pathFootprint(of: child))
            }
            return footprint
        }
    }

    private func stringFootprint(
        _ value: String
    ) throws -> DatabaseIntermediateFootprint {
        try DatabaseIntermediateFootprint(
            bytes: Self.stringOwnerByteCount
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: UInt64(value.utf8.count)
            )
        )
    }
}
