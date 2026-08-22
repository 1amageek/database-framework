import StorageKit
import Testing
@testable import FullTextIndex

@Suite("Full-text posting-list algebra")
struct FullTextPostingListAlgebraTests {
    @Test("Intersection preserves storage order and tuple identity")
    func intersectionPreservesOrder() throws {
        let lhs = identifiers([1, 3, 5, 7])
        let rhs = identifiers([0, 1, 2, 5, 8])

        let result = try FullTextPostingListAlgebra.intersection(lhs, rhs)

        #expect(try values(result) == [1, 5])
    }

    @Test("Union is ordered and removes identifiers present in both inputs")
    func unionPreservesOrderAndUniqueness() throws {
        let lhs = identifiers([1, 3, 5, 7])
        let rhs = identifiers([0, 1, 2, 5, 8])

        let result = try FullTextPostingListAlgebra.union(lhs, rhs)

        #expect(try values(result) == [0, 1, 2, 3, 5, 7, 8])
    }

    @Test("Admission occurs before an identifier is retained")
    func admissionFailureStopsBeforeAppend() {
        enum AdmissionError: Error {
            case rejected
        }

        var admitted: [Int64] = []
        #expect(throws: AdmissionError.rejected) {
            _ = try FullTextPostingListAlgebra.union(
                identifiers([1, 3]),
                identifiers([2, 4])
            ) { identifier, _ in
                let value = try #require(identifier[0] as? Int64)
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
        let lhs = identifiers(lhsValues)
        let rhs = identifiers(rhsValues)

        let intersection = try values(
            try FullTextPostingListAlgebra.intersection(lhs, rhs)
        )
        let union = try values(
            try FullTextPostingListAlgebra.union(lhs, rhs)
        )

        #expect(intersection == Array(Set(lhsValues).intersection(rhsValues)).sorted())
        #expect(union == Array(Set(lhsValues).union(rhsValues)).sorted())
    }

    private func identifiers(
        _ values: [Int64]
    ) -> [[any TupleElement]] {
        values.map { value in [value as any TupleElement] }
    }

    private func identifiers(
        _ values: [Int]
    ) -> [[any TupleElement]] {
        identifiers(values.map(Int64.init))
    }

    private func values(
        _ identifiers: [[any TupleElement]]
    ) throws -> [Int64] {
        try identifiers.map { identifier in
            try #require(identifier[0] as? Int64)
        }
    }
}
