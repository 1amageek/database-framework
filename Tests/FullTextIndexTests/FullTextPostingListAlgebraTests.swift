import DatabaseEngine
import DatabaseKit
import StorageKit
import TestSupport
import Testing
@testable import FullTextIndex

@Suite("Full-text posting-list algebra")
struct FullTextPostingListAlgebraTests {
    @Test("Intersection preserves storage order and tuple identity")
    func intersectionPreservesOrder() throws {
        let lhs = try identifiers([1, 3, 5, 7])
        let rhs = try identifiers([0, 1, 2, 5, 8])

        let result = try FullTextPostingListAlgebra.intersection(lhs, rhs)

        #expect(values(result) == [1, 5])
        #expect(result.map(\.canonicalKey) == (try identifiers([1, 5])).map(\.canonicalKey))

        let canonical = Tuple(Int64(1)).pack()
        let decoderAcceptedNonCanonical = ByteString([0x16, 0x00, 0x01])
        let nonCanonicalCandidate = try candidate(
            decoderAcceptedNonCanonical
        )
        let canonicalCandidate = try candidate(canonical)
        let equalIntersection = try FullTextPostingListAlgebra.intersection(
            [nonCanonicalCandidate],
            [canonicalCandidate]
        )
        let deduplicatedUnion = try FullTextPostingListAlgebra.union(
            [nonCanonicalCandidate],
            [canonicalCandidate]
        )
        #expect(equalIntersection.map(\.canonicalKey) == [canonical])
        #expect(deduplicatedUnion.count == 1)
    }

    @Test("Union is ordered and removes identifiers present in both inputs")
    func unionPreservesOrderAndUniqueness() throws {
        let lhs = try identifiers([1, 3, 5, 7])
        let rhs = try identifiers([0, 1, 2, 5, 8])

        let result = try FullTextPostingListAlgebra.union(lhs, rhs)

        #expect(values(result) == [0, 1, 2, 3, 5, 7, 8])
    }

    @Test("Admission occurs before an identifier is retained")
    func admissionFailureStopsBeforeAppend() throws {
        enum AdmissionError: Error {
            case rejected
        }

        var admitted: [Int64] = []
        #expect(throws: AdmissionError.rejected) {
            _ = try FullTextPostingListAlgebra.union(
                try identifiers([1, 3]),
                try identifiers([2, 4])
            ) { candidate in
                let value = try #require(candidate.identifier.element(at: 0) as? Int64)
                if value == 3 {
                    throw AdmissionError.rejected
                }
                admitted.append(value)
            }
        }
        #expect(admitted == [1, 2])
    }

    @Test("Ordered merge matches reference set algebra")
    func matchesReferenceSetAlgebra() throws {
        let lhsValues = (0..<2_048).filter { value in
            value.isMultiple(of: 3) || value.isMultiple(of: 11)
        }.map(Int64.init)
        let rhsValues = (0..<2_048).filter { value in
            value.isMultiple(of: 5) || value.isMultiple(of: 13)
        }.map(Int64.init)
        let lhs = try identifiers(lhsValues)
        let rhs = try identifiers(rhsValues)

        let intersection = values(
            try FullTextPostingListAlgebra.intersection(lhs, rhs)
        )
        let union = values(
            try FullTextPostingListAlgebra.union(lhs, rhs)
        )

        #expect(intersection == Array(Set(lhsValues).intersection(rhsValues)).sorted())
        #expect(union == Array(Set(lhsValues).union(rhsValues)).sorted())
    }

    @Test("Packed multi-element candidates preserve encoded ordering")
    func packedMultiElementCandidatesPreserveOrdering() throws {
        let lhs = try multiElementIdentifiers([
            ("alpha", 1),
            ("alpha", 3),
            ("beta", 1),
        ])
        let rhs = try multiElementIdentifiers([
            ("alpha", 3),
            ("beta", 1),
            ("beta", 2),
        ])

        let result = try FullTextPostingListAlgebra.intersection(lhs, rhs)

        #expect(result.map(\.identifier) == [
            Tuple("alpha", Int64(3)),
            Tuple("beta", Int64(1)),
        ])
        #expect(result.map(\.canonicalKey) == result.map { $0.identifier.pack() })
    }

    @Test("Malformed packed suffix fails before algebraic exclusion")
    func malformedPackedSuffixFailsBeforeAlgebraicExclusion() {
        let malformed = ByteString([0xFF])

        #expect(throws: TupleError.self) {
            let candidate = try FullTextPostingCandidate(
                packedSuffix: malformed,
                admitting: { _ in }
            )
            _ = try FullTextPostingListAlgebra.intersection(
                [candidate],
                try identifiers([1])
            )
        }
    }

    @Test("Batch admission failure leaves no retained candidate")
    func batchAdmissionFailureLeavesNoRetainedCandidate() throws {
        let meter = makeMeter(maximumIntermediateBytes: 64)
        var batch = try FullTextCandidateBatch(workMeter: meter)
        let largeIdentifier = Tuple(String(repeating: "x", count: 256)).pack()

        #expect(throws: DatabaseWorkLimitError.self) {
            try batch.append(scannedSuffix: largeIdentifier)
        }
        #expect(batch.count == 0)
        batch.release()
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Batch reservation is released after successful and failed scans")
    func batchReservationIsReleasedAfterSuccessfulAndFailedScans() throws {
        let meter = makeMeter()
        do {
            var batch = try FullTextCandidateBatch(workMeter: meter)
            try batch.append(scannedSuffix: Tuple(Int64(1)).pack())
            #expect(batch.count == 1)
            #expect(meter.retainedIntermediateRows > 0)
            batch.release()
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)

        let nestedIdentifier = Tuple(
            Tuple(String(repeating: "nested", count: 96)),
            String(repeating: "payload", count: 512)
        )
        let initialFootprint = DatabaseIntermediateFootprint(
            rows: 1,
            bytes: 987_654
        )
        let nestedCandidate = try candidate(
            nestedIdentifier.pack(),
            retainedFootprint: initialFootprint
        )
        var observedFootprints: [DatabaseIntermediateFootprint] = []
        _ = FullTextPostingListAlgebra.union(
            [],
            [nestedCandidate]
        ) { mergedCandidate in
            observedFootprints.append(mergedCandidate.retainedFootprint)
        }
        #expect(observedFootprints == [nestedCandidate.retainedFootprint])

        do {
            var batch = try FullTextCandidateBatch(workMeter: meter)
            #expect(throws: TupleError.self) {
                try batch.append(scannedSuffix: ByteString([0xFF]))
            }
            #expect(batch.count == 0)
            batch.release()
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private func makeMeter(
        maximumIntermediateBytes: UInt64 = 100_000
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 100,
                maximumWorkUnits: 10_000,
                maximumIntermediateRows: 100,
                maximumIntermediateBytes: maximumIntermediateBytes,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    private func identifiers(
        _ values: [Int64]
    ) throws -> [FullTextPostingCandidate] {
        try values.map { value in
            let identifier = Tuple(value)
            return try candidate(identifier.pack())
        }
    }

    private func identifiers(
        _ values: [Int]
    ) throws -> [FullTextPostingCandidate] {
        try identifiers(values.map(Int64.init))
    }

    private func values(
        _ candidates: [FullTextPostingCandidate]
    ) -> [Int64] {
        candidates.compactMap { candidate in
            candidate.identifier[0] as? Int64
        }
    }

    private func multiElementIdentifiers(
        _ values: [(String, Int64)]
    ) throws -> [FullTextPostingCandidate] {
        try values.map { value in
            let identifier = Tuple(value.0, value.1)
            return try candidate(identifier.pack())
        }
    }

    private func candidate(
        _ packedSuffix: ByteString,
        retainedFootprint: DatabaseIntermediateFootprint = DatabaseIntermediateFootprint()
    ) throws -> FullTextPostingCandidate {
        try FullTextPostingCandidate(
            packedSuffix: packedSuffix,
            retainedFootprint: retainedFootprint,
            admitting: { _ in }
        )
    }
}
