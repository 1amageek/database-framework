import DatabaseKit
import DatabaseTypes
import StorageKit
import Testing
@testable import DatabaseEngine

@Suite("Transaction storage reader")
struct TransactionStorageReaderTests {
    @Test("Range bounds preserve tuple-prefix semantics in both directions")
    func rangeBoundsAndDirection() async throws {
        let engine = InMemoryEngine()
        let subspace = Subspace(prefix: Tuple("reader", "range").pack())
        try await engine.withTransaction { transaction in
            for value in 1...4 {
                try transaction.setValue(
                    [UInt8(value)],
                    for: subspace.pack(
                        Tuple([Int64(value), "entity-\(value)"])
                    )
                )
            }
            try transaction.setValue(
                [0xFF],
                for: Tuple("reader", "neighbor").pack()
            )
        }

        func scan(
            start: Int64?,
            end: Int64?,
            startInclusive: Bool,
            endInclusive: Bool,
            reverse: Bool
        ) async throws -> [UInt8] {
            try await engine.withTransaction { transaction in
                let reader = TransactionStorageReader(
                    transaction: transaction
                )
                var cursor = try reader.rangeCursor(
                    subspace: subspace,
                    start: start.map { Tuple($0) },
                    end: end.map { Tuple($0) },
                    startInclusive: startInclusive,
                    endInclusive: endInclusive,
                    reverse: reverse
                )
                var values: [UInt8] = []
                try await cursor.consume { _, value in
                    guard value.count == 1, let byte = value.first else {
                        throw TransactionStorageReaderTestError.invalidValue
                    }
                    values.append(byte)
                }
                return values
            }
        }

        #expect(
            try await scan(
                start: 2,
                end: 4,
                startInclusive: true,
                endInclusive: false,
                reverse: false
            ) == [2, 3]
        )
        #expect(
            try await scan(
                start: 2,
                end: 4,
                startInclusive: false,
                endInclusive: true,
                reverse: false
            ) == [3, 4]
        )
        #expect(
            try await scan(
                start: 2,
                end: 4,
                startInclusive: true,
                endInclusive: true,
                reverse: true
            ) == [4, 3, 2]
        )
        #expect(
            try await scan(
                start: nil,
                end: nil,
                startInclusive: true,
                endInclusive: false,
                reverse: false
            ) == [1, 2, 3, 4]
        )
    }

    @Test("An early consumer explicitly finishes the backend cursor")
    func earlyFinish() async throws {
        let engine = InMemoryEngine()
        let subspace = Subspace(prefix: Tuple("reader", "finish").pack())
        try await engine.withTransaction { transaction in
            try transaction.setValue(
                [0x01],
                for: subspace.pack(Tuple("first"))
            )
            try transaction.setValue(
                [0x02],
                for: subspace.pack(Tuple("second"))
            )
        }

        try await engine.withTransaction { transaction in
            let reader = TransactionStorageReader(transaction: transaction)
            var cursor = try reader.subspaceCursor(
                subspace,
                reverse: false
            )
            #expect(try await cursor.next() != nil)
            try await cursor.finish()
            #expect(try await cursor.next() == nil)
        }
    }
}

private enum TransactionStorageReaderTestError: Error {
    case invalidValue
}
