import Testing
import FoundationDB
import TestSupport
@testable import DatabaseEngine

@Suite("Transaction Basic Tests")
struct TransactionBasicTests {

    @Test func simpleReadWrite() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let runner = TransactionRunner(database: database)
        
        // Simple write
        try await runner.run(configuration: .default) { tx in
            tx.setValue([1, 2, 3], for: [0, 0, 1])
        }
        
        // Simple read
        let value = try await runner.run(configuration: .default) { tx in
            try await tx.getValue(for: [0, 0, 1])
        }
        
        #expect(value == [1, 2, 3])
    }
    
    @Test func simpleGetRange() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let runner = TransactionRunner(database: database)
        
        // Write multiple keys
        try await runner.run(configuration: .default) { tx in
            tx.setValue([1], for: [0, 0, 2, 1])
            tx.setValue([2], for: [0, 0, 2, 2])
            tx.setValue([3], for: [0, 0, 2, 3])
        }
        
        // Read with getRange
        let results = try await runner.run(configuration: .default) { tx in
            var items: [(FDB.Bytes, FDB.Bytes)] = []
            let seq = tx.getRange(
                from: .firstGreaterOrEqual([0, 0, 2]),
                to: .firstGreaterOrEqual([0, 0, 3])
            )
            for try await (k, v) in seq {
                items.append((k, v))
            }
            return items
        }
        
        #expect(results.count == 3)
    }
}
