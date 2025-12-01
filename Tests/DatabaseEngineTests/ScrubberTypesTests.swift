import Testing
import Foundation
@testable import DatabaseEngine

@Suite("ScrubberTypes Tests")
struct ScrubberTypesTests {

    // MARK: - ScrubberConfiguration

    @Test("Default configuration")
    func testDefaultConfiguration() {
        let config = ScrubberConfiguration.default

        #expect(config.entriesScanLimit == 1_000)
        #expect(config.maxTransactionBytes == 9_000_000)
        #expect(config.transactionTimeoutMillis == 4_000)
        #expect(config.allowRepair == false)
        #expect(config.maxRetries == 10)
        #expect(config.retryDelayMillis == 100)
        #expect(config.throttleDelayMs == 0)
    }

    @Test("Conservative configuration")
    func testConservativeConfiguration() {
        let config = ScrubberConfiguration.conservative

        #expect(config.entriesScanLimit == 100)
        #expect(config.maxTransactionBytes == 1_000_000)
        #expect(config.transactionTimeoutMillis == 2_000)
        #expect(config.allowRepair == false)
        #expect(config.maxRetries == 5)
        #expect(config.retryDelayMillis == 200)
        #expect(config.throttleDelayMs == 50)
    }

    @Test("Aggressive configuration")
    func testAggressiveConfiguration() {
        let config = ScrubberConfiguration.aggressive

        #expect(config.entriesScanLimit == 10_000)
        #expect(config.maxTransactionBytes == 9_000_000)
        #expect(config.transactionTimeoutMillis == 4_000)
        #expect(config.allowRepair == true)
        #expect(config.maxRetries == 20)
        #expect(config.retryDelayMillis == 50)
        #expect(config.throttleDelayMs == 0)
    }

    @Test("Custom configuration")
    func testCustomConfiguration() {
        let config = ScrubberConfiguration(
            entriesScanLimit: 500,
            maxTransactionBytes: 5_000_000,
            transactionTimeoutMillis: 3_000,
            allowRepair: true,
            maxRetries: 15,
            retryDelayMillis: 150,
            throttleDelayMs: 25
        )

        #expect(config.entriesScanLimit == 500)
        #expect(config.maxTransactionBytes == 5_000_000)
        #expect(config.transactionTimeoutMillis == 3_000)
        #expect(config.allowRepair == true)
        #expect(config.maxRetries == 15)
        #expect(config.retryDelayMillis == 150)
        #expect(config.throttleDelayMs == 25)
    }

    // MARK: - ScrubberSummary

    @Test("ScrubberSummary computed properties")
    func testScrubberSummaryComputedProperties() {
        let summary = ScrubberSummary(
            timeElapsed: 10.5,
            entriesScanned: 1000,
            itemsScanned: 500,
            danglingEntriesDetected: 5,
            danglingEntriesRepaired: 3,
            missingEntriesDetected: 10,
            missingEntriesRepaired: 8,
            indexName: "test_index"
        )

        #expect(summary.issuesDetected == 15)  // 5 + 10
        #expect(summary.issuesRepaired == 11)  // 3 + 8
    }

    @Test("ScrubberSummary description")
    func testScrubberSummaryDescription() {
        let summary = ScrubberSummary(
            timeElapsed: 10.5,
            entriesScanned: 1000,
            itemsScanned: 500,
            danglingEntriesDetected: 5,
            danglingEntriesRepaired: 3,
            missingEntriesDetected: 10,
            missingEntriesRepaired: 8,
            indexName: "test_index"
        )

        let description = summary.description

        #expect(description.contains("test_index"))
        #expect(description.contains("1000"))
        #expect(description.contains("500"))
        #expect(description.contains("5"))
        #expect(description.contains("10"))
    }

    // MARK: - ScrubberResult

    @Test("Healthy result")
    func testHealthyResult() {
        let summary = ScrubberSummary(
            timeElapsed: 5.0,
            entriesScanned: 100,
            itemsScanned: 100,
            danglingEntriesDetected: 0,
            danglingEntriesRepaired: 0,
            missingEntriesDetected: 0,
            missingEntriesRepaired: 0,
            indexName: "test_index"
        )

        let result = ScrubberResult(
            isHealthy: true,
            completedSuccessfully: true,
            summary: summary
        )

        #expect(result.isHealthy == true)
        #expect(result.completedSuccessfully == true)
        #expect(result.terminationReason == nil)
        #expect(result.error == nil)
    }

    @Test("Unhealthy result with issues")
    func testUnhealthyResult() {
        let summary = ScrubberSummary(
            timeElapsed: 5.0,
            entriesScanned: 100,
            itemsScanned: 100,
            danglingEntriesDetected: 5,
            danglingEntriesRepaired: 0,
            missingEntriesDetected: 3,
            missingEntriesRepaired: 0,
            indexName: "test_index"
        )

        let result = ScrubberResult(
            isHealthy: false,
            completedSuccessfully: true,
            summary: summary
        )

        #expect(result.isHealthy == false)
        #expect(result.completedSuccessfully == true)
    }

    @Test("Failed result with error")
    func testFailedResult() {
        let summary = ScrubberSummary(
            timeElapsed: 1.0,
            entriesScanned: 50,
            itemsScanned: 0,
            danglingEntriesDetected: 0,
            danglingEntriesRepaired: 0,
            missingEntriesDetected: 0,
            missingEntriesRepaired: 0,
            indexName: "test_index"
        )

        let error = ScrubberError.indexNotFound("test_index")

        let result = ScrubberResult(
            isHealthy: false,
            completedSuccessfully: false,
            summary: summary,
            terminationReason: "Index not found",
            error: error
        )

        #expect(result.isHealthy == false)
        #expect(result.completedSuccessfully == false)
        #expect(result.terminationReason == "Index not found")
        #expect(result.error != nil)
    }

    @Test("ScrubberResult description - completed")
    func testScrubberResultDescriptionCompleted() {
        let summary = ScrubberSummary(
            timeElapsed: 5.0,
            entriesScanned: 100,
            itemsScanned: 100,
            danglingEntriesDetected: 2,
            danglingEntriesRepaired: 2,
            missingEntriesDetected: 1,
            missingEntriesRepaired: 1,
            indexName: "test_index"
        )

        let result = ScrubberResult(
            isHealthy: false,
            completedSuccessfully: true,
            summary: summary
        )

        let description = result.description

        #expect(description.contains("healthy: false"))
        #expect(description.contains("issues: 3"))
        #expect(description.contains("repaired: 3"))
    }

    @Test("ScrubberResult description - incomplete")
    func testScrubberResultDescriptionIncomplete() {
        let summary = ScrubberSummary(
            timeElapsed: 1.0,
            entriesScanned: 50,
            itemsScanned: 0,
            danglingEntriesDetected: 0,
            danglingEntriesRepaired: 0,
            missingEntriesDetected: 0,
            missingEntriesRepaired: 0,
            indexName: "test_index"
        )

        let result = ScrubberResult(
            isHealthy: false,
            completedSuccessfully: false,
            summary: summary,
            terminationReason: "timeout"
        )

        let description = result.description

        #expect(description.contains("incomplete"))
        #expect(description.contains("timeout"))
    }

    // MARK: - ScrubberIssue

    @Test("Dangling entry issue")
    func testDanglingEntryIssue() {
        let issue = ScrubberIssue(
            type: .danglingEntry,
            indexKey: [1, 2, 3],
            primaryKey: ["user123"],
            repaired: false,
            context: "Index entry without item"
        )

        #expect(issue.type == .danglingEntry)
        #expect(issue.indexKey == [1, 2, 3])
        #expect(issue.repaired == false)
        #expect(issue.context == "Index entry without item")
    }

    @Test("Missing entry issue")
    func testMissingEntryIssue() {
        let issue = ScrubberIssue(
            type: .missingEntry,
            indexKey: [4, 5, 6],
            primaryKey: ["user456"],
            repaired: true,
            context: nil
        )

        #expect(issue.type == .missingEntry)
        #expect(issue.repaired == true)
        #expect(issue.context == nil)
    }

    @Test("IssueType raw values")
    func testIssueTypeRawValues() {
        #expect(ScrubberIssue.IssueType.danglingEntry.rawValue == "dangling_entry")
        #expect(ScrubberIssue.IssueType.missingEntry.rawValue == "missing_entry")
    }

    // MARK: - ScrubberError

    @Test("Index not found error")
    func testIndexNotFoundError() {
        let error = ScrubberError.indexNotFound("my_index")

        #expect(error.description == "Index 'my_index' not found")
    }

    @Test("Index not readable error")
    func testIndexNotReadableError() {
        let error = ScrubberError.indexNotReadable(
            indexName: "my_index",
            currentState: "writeOnly"
        )

        #expect(error.description.contains("my_index"))
        #expect(error.description.contains("writeOnly"))
    }

    @Test("Unsupported index type error")
    func testUnsupportedIndexTypeError() {
        let error = ScrubberError.unsupportedIndexType(
            indexName: "my_index",
            indexType: "vector"
        )

        #expect(error.description.contains("my_index"))
        #expect(error.description.contains("vector"))
    }

    @Test("Retry limit exceeded error")
    func testRetryLimitExceededError() {
        struct MockError: Error, CustomStringConvertible {
            var description: String { "connection failed" }
        }

        let error = ScrubberError.retryLimitExceeded(
            phase: "Phase 1",
            attempts: 10,
            lastError: MockError()
        )

        #expect(error.description.contains("Phase 1"))
        #expect(error.description.contains("10"))
    }

    @Test("Invalid item type error")
    func testInvalidItemTypeError() {
        let error = ScrubberError.invalidItemType("Unknown")

        #expect(error.description == "Invalid item type: Unknown")
    }
}
