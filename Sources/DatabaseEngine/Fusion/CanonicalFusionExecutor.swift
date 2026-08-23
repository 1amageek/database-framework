import DatabaseKit
import DatabaseTypes

package struct CanonicalFusionExecutor {
    private let workMeter: DatabaseWorkMeter

    package init(workMeter: DatabaseWorkMeter) {
        self.workMeter = workMeter
    }

    package func execute(
        source: FusionSource,
        input: (IndexScanSource) async throws -> IndexReadResult
    ) async throws -> IndexReadResult {
        try validate(source)

        let inputReservation = try workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: source.inputs.count,
                element: IndexReadResult.self
            ).bytes,
            at: .indexScan
        )
        defer { inputReservation.release() }
        var results: [IndexReadResult] = []
        results.reserveCapacity(source.inputs.count)
        for indexSource in source.inputs {
            let result = try await input(indexSource)
            do {
                try result.validateWorkMeter(
                    workMeter,
                    sourceName: indexSource.indexName
                )
            } catch CanonicalReadError.executorWorkMeterMismatch {
                throw CanonicalFusionExecutionError.inputWorkMeterMismatch(
                    indexName: indexSource.indexName
                )
            }
            try validateRows(
                result.rows,
                indexName: indexSource.indexName,
                identityField: source.identityField
            )
            results.append(result)
        }

        let fused: DatabaseSharedRetainedArray<
            CanonicalFusionAlgebraResult<FieldValue, IndexReadRow>
        >
        do {
            fused = try CanonicalFusionAlgebra.fuse(
                sources: results.lazy.map(\.rows),
                orderedSources: results.lazy.map { result in
                    if case .orderedByIndex = result.ordering { return true }
                    return false
                },
                strategy: source.strategy,
                isEligible: { _ in true },
                workMeter: workMeter,
                identity: { row in
                    guard let identity = row.fields[source.identityField] else {
                        throw CanonicalFusionExecutionError.missingIdentity(
                            indexName: "fusion input",
                            field: source.identityField
                        )
                    }
                    return identity
                },
                signal: { row in
                    try canonicalSignal(in: row)
                },
                payloadsAreEquivalent: { lhs, rhs in
                    lhs.fields == rhs.fields && lhs.version == rhs.version
                }
            )
        } catch let error as CanonicalFusionAlgebraError<FieldValue> {
            throw map(error, sources: source.inputs)
        }

        return try IndexReadResult.build(
            workMeter: workMeter,
            ordering: .orderedByIndex,
            expectedCount: fused.count
        ) { builder in
            for entry in fused {
                try builder.append(
                    IndexReadRow(
                        fields: entry.payload.fields,
                        annotations: [
                            FusionSource.scoreAnnotation: .float64(entry.score)
                        ],
                        version: entry.payload.version
                    )
                )
            }
        }
    }

    private func validate(_ source: FusionSource) throws {
        guard !source.inputs.isEmpty else {
            throw CanonicalFusionExecutionError.noInputs
        }
        guard !source.identityField.isEmpty else {
            throw CanonicalFusionExecutionError.emptyIdentityField
        }
    }

    private func validateRows(
        _ rows: [IndexReadRow],
        indexName: String,
        identityField: String
    ) throws {
        for row in rows {
            guard row.fields[identityField] != nil else {
                throw CanonicalFusionExecutionError.missingIdentity(
                    indexName: indexName,
                    field: identityField
                )
            }
            _ = try canonicalSignal(in: row, indexName: indexName)
        }
    }

    private func canonicalSignal(
        in row: borrowing IndexReadRow,
        indexName: String = "fusion input"
    ) throws -> CanonicalFusionSignal {
        var selected: CanonicalFusionSignal?
        if let annotation = row.annotations["score"] {
            guard let score = annotation.float64Value else {
                throw CanonicalFusionExecutionError.invalidScoreAnnotation(
                    indexName: indexName,
                    annotation: "score"
                )
            }
            selected = .higherIsBetter(score)
        }
        if let annotation = row.annotations["distance"] {
            guard let distance = annotation.float64Value else {
                throw CanonicalFusionExecutionError.invalidScoreAnnotation(
                    indexName: indexName,
                    annotation: "distance"
                )
            }
            guard selected == nil else {
                throw CanonicalFusionExecutionError
                    .inconsistentScoreAnnotation(indexName: indexName)
            }
            selected = .lowerIsBetter(distance)
        }
        if let annotation = row.annotations["rank"] {
            guard let rank = annotation.int64Value else {
                throw CanonicalFusionExecutionError.invalidScoreAnnotation(
                    indexName: indexName,
                    annotation: "rank"
                )
            }
            guard selected == nil else {
                throw CanonicalFusionExecutionError
                    .inconsistentScoreAnnotation(indexName: indexName)
            }
            selected = .lowerIsBetter(Double(rank))
        }
        return selected ?? .position
    }

    private func map(
        _ error: CanonicalFusionAlgebraError<FieldValue>,
        sources: [IndexScanSource]
    ) -> CanonicalFusionExecutionError {
        func indexName(_ index: Int) -> String {
            sources.indices.contains(index)
                ? sources[index].indexName
                : "fusion input"
        }
        switch error {
        case .weightCountMismatch(let expected, let actual):
            return .weightCountMismatch(expected: expected, actual: actual)
        case .nonFiniteWeight(let index):
            return .nonFiniteWeight(index: index)
        case .negativeWeight(let index):
            return .negativeWeight(index: index)
        case .duplicateIdentity(let sourceIndex, let identity):
            return .duplicateIdentity(
                indexName: indexName(sourceIndex),
                identity: identity
            )
        case .inconsistentPayload(let identity):
            return .inconsistentRows(identity: identity)
        case .inconsistentSignal(let sourceIndex):
            return .inconsistentScoreAnnotation(
                indexName: indexName(sourceIndex)
            )
        case .nonFiniteSignal(let sourceIndex):
            return .nonFiniteScore(indexName: indexName(sourceIndex))
        case .unorderedRankSource(let sourceIndex):
            return .unorderedSourceRequiresScore(
                indexName: indexName(sourceIndex)
            )
        case .scoreOverflow(let identity):
            return .scoreOverflow(identity: identity)
        case .inputCountOverflow:
            return .inputCountOverflow
        }
    }
}
