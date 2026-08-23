import DatabaseKit
import DatabaseTypes
import StorageKitSystemClock
import Testing
@testable import DatabaseEngine

@Suite("Canonical fusion executor")
struct CanonicalFusionExecutorTests {
    @Test("Reciprocal rank fusion combines inputs by canonical identity")
    func reciprocalRankFusion() async throws {
        let meter = makeWorkMeter()
        let first = try result(
            meter: meter,
            rows: [row(id: "a"), row(id: "b")]
        )
        let second = try result(
            meter: meter,
            rows: [row(id: "b"), row(id: "c")]
        )
        let source = FusionSource(
            inputs: [input("first"), input("second")],
            strategy: .reciprocalRank(rankConstant: 0)
        )

        let fused = try await CanonicalFusionExecutor(
            workMeter: meter
        ).execute(source: source) { input in
            input.indexName == "first" ? first : second
        }

        #expect(fused.rows.map { $0.fields["id"] } == [
            .string("b"),
            .string("a"),
            .string("c"),
        ])
        #expect(
            fused.rows[0].annotations[FusionSource.scoreAnnotation]
                == .float64(1.5)
        )
        #expect(
            fused.rows[1].annotations[FusionSource.scoreAnnotation]
                == .float64(1.0)
        )
        #expect(
            fused.rows[2].annotations[FusionSource.scoreAnnotation]
                == .float64(0.5)
        )
    }

    @Test("Weighted fusion normalizes each input before applying weights")
    func weightedFusion() async throws {
        let meter = makeWorkMeter()
        let scoreSource = try result(
            meter: meter,
            rows: [
                row(id: "a", annotations: ["score": .float64(10)]),
                row(id: "b", annotations: ["score": .float64(0)]),
            ]
        )
        let distanceSource = try result(
            meter: meter,
            rows: [
                row(id: "b", annotations: ["distance": .float64(1)]),
                row(id: "a", annotations: ["distance": .float64(5)]),
            ]
        )
        let source = FusionSource(
            inputs: [input("score"), input("distance")],
            strategy: .weighted([0.25, 0.75])
        )

        let fused = try await CanonicalFusionExecutor(
            workMeter: meter
        ).execute(source: source) { input in
            input.indexName == "score" ? scoreSource : distanceSource
        }

        #expect(fused.rows.map { $0.fields["id"] } == [
            .string("b"),
            .string("a"),
        ])
        #expect(
            fused.rows[0].annotations[FusionSource.scoreAnnotation]
                == .float64(0.75)
        )
        #expect(
            fused.rows[1].annotations[FusionSource.scoreAnnotation]
                == .float64(0.25)
        )
    }

    @Test("Malformed fusion inputs fail explicitly")
    func malformedInputsFail() async throws {
        let meter = makeWorkMeter()
        let valid = try result(meter: meter, rows: [row(id: "a")])
        let missingIdentity = try result(
            meter: meter,
            rows: [IndexReadRow(fields: ["value": .string("missing")])]
        )
        let inconsistentSignal = try result(
            meter: meter,
            rows: [
                row(id: "a"),
                row(id: "b", annotations: ["score": .float64(1)]),
            ]
        )

        await #expect(throws: CanonicalFusionExecutionError.noInputs) {
            try await CanonicalFusionExecutor(workMeter: meter).execute(
                source: FusionSource(inputs: [])
            ) { _ in valid }
        }
        await #expect(
            throws: CanonicalFusionExecutionError.weightCountMismatch(
                expected: 1,
                actual: 2
            )
        ) {
            try await CanonicalFusionExecutor(workMeter: meter).execute(
                source: FusionSource(
                    inputs: [input("one")],
                    strategy: .weighted([0.5, 0.5])
                )
            ) { _ in valid }
        }
        await #expect(
            throws: CanonicalFusionExecutionError.negativeWeight(index: 0)
        ) {
            try await CanonicalFusionExecutor(workMeter: meter).execute(
                source: FusionSource(
                    inputs: [input("one")],
                    strategy: .weighted([-1])
                )
            ) { _ in valid }
        }
        await #expect(
            throws: CanonicalFusionExecutionError.missingIdentity(
                indexName: "missing",
                field: "id"
            )
        ) {
            try await CanonicalFusionExecutor(workMeter: meter).execute(
                source: FusionSource(inputs: [input("missing")])
            ) { _ in missingIdentity }
        }
        await #expect(
            throws: CanonicalFusionExecutionError
                .inconsistentScoreAnnotation(indexName: "inconsistent")
        ) {
            try await CanonicalFusionExecutor(workMeter: meter).execute(
                source: FusionSource(inputs: [input("inconsistent")])
            ) { _ in inconsistentSignal }
        }
    }

    @Test("Duplicate, inconsistent, non-finite, and unordered inputs fail")
    func semanticInputViolationsFail() async throws {
        let meter = makeWorkMeter()
        let duplicate = try result(
            meter: meter,
            rows: [row(id: "a"), row(id: "a")]
        )
        let first = try result(
            meter: meter,
            rows: [row(id: "same")]
        )
        let inconsistent = try result(
            meter: meter,
            rows: [
                IndexReadRow(
                    fields: [
                        "id": .string("same"),
                        "value": .string("different"),
                    ]
                )
            ]
        )
        let nonFinite = try result(
            meter: meter,
            rows: [
                row(
                    id: "nan",
                    annotations: ["score": .float64(.nan)]
                )
            ]
        )
        let invalidScore = try result(
            meter: meter,
            rows: [
                row(
                    id: "invalid",
                    annotations: ["score": .string("not-a-score")]
                )
            ]
        )
        let unordered = try result(
            meter: meter,
            rows: [row(id: "unordered")],
            ordering: .unordered
        )

        await #expect(
            throws: CanonicalFusionExecutionError.duplicateIdentity(
                indexName: "duplicate",
                identity: .string("a")
            )
        ) {
            try await CanonicalFusionExecutor(workMeter: meter).execute(
                source: FusionSource(inputs: [input("duplicate")])
            ) { _ in duplicate }
        }
        await #expect(
            throws: CanonicalFusionExecutionError.inconsistentRows(
                identity: .string("same")
            )
        ) {
            try await CanonicalFusionExecutor(workMeter: meter).execute(
                source: FusionSource(
                    inputs: [input("first"), input("second")]
                )
            ) { source in
                source.indexName == "first" ? first : inconsistent
            }
        }
        await #expect(
            throws: CanonicalFusionExecutionError.nonFiniteScore(
                indexName: "nonfinite"
            )
        ) {
            try await CanonicalFusionExecutor(workMeter: meter).execute(
                source: FusionSource(inputs: [input("nonfinite")])
            ) { _ in nonFinite }
        }
        await #expect(
            throws: CanonicalFusionExecutionError.invalidScoreAnnotation(
                indexName: "invalid",
                annotation: "score"
            )
        ) {
            try await CanonicalFusionExecutor(workMeter: meter).execute(
                source: FusionSource(inputs: [input("invalid")])
            ) { _ in invalidScore }
        }
        await #expect(
            throws: CanonicalFusionExecutionError
                .unorderedSourceRequiresScore(indexName: "unordered")
        ) {
            try await CanonicalFusionExecutor(workMeter: meter).execute(
                source: FusionSource(inputs: [input("unordered")])
            ) { _ in unordered }
        }
    }

    @Test("Equal fused scores are ordered by canonical identity")
    func equalScoresUseCanonicalIdentityTieBreak() async throws {
        let meter = makeWorkMeter()
        let inputResult = try result(
            meter: meter,
            rows: [
                row(id: "z", annotations: ["score": .float64(1)]),
                row(id: "a", annotations: ["score": .float64(1)]),
            ]
        )

        let fused = try await CanonicalFusionExecutor(
            workMeter: meter
        ).execute(
            source: FusionSource(
                inputs: [input("tie")],
                strategy: .sum
            )
        ) { _ in inputResult }

        #expect(fused.rows.map { $0.fields["id"] } == [
            .string("a"),
            .string("z"),
        ])
    }

    private func makeWorkMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1_000,
                maximumWorkUnits: 10_000,
                maximumIntermediateRows: 1_000,
                maximumIntermediateBytes: 1_000_000,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: SystemStorageClock()
        )
    }

    private func input(_ name: String) -> IndexScanSource {
        IndexScanSource(indexName: name, indexType: .custom(name))
    }

    private func row(
        id: String,
        annotations: [String: FieldValue] = [:]
    ) -> IndexReadRow {
        IndexReadRow(
            fields: [
                "id": .string(id),
                "value": .string("value-\(id)"),
            ],
            annotations: annotations
        )
    }

    private func result(
        meter: DatabaseWorkMeter,
        rows: [IndexReadRow],
        ordering: IndexReadResult.Ordering = .orderedByIndex
    ) throws -> IndexReadResult {
        try IndexReadResult.build(
            workMeter: meter,
            ordering: ordering,
            expectedCount: rows.count
        ) { builder in
            for row in rows {
                try builder.append(row)
            }
        }
    }
}
