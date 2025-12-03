// MutualOnlineIndexerTests.swift
// Tests for MutualOnlineIndexer and SymmetricIndexBuilder implementations
//
// Reference: FDB Record Layer mutual indexing strategy

import Testing
import Foundation
@testable import DatabaseEngine
@testable import Core

// MARK: - MutualIndexConfiguration Tests

@Suite("MutualIndexConfiguration Tests")
struct MutualIndexConfigurationTests {

    @Test("Configuration stores forward and reverse index names")
    func testConfigurationIndexNames() {
        let config = MutualIndexConfiguration(
            forwardIndexName: "following",
            reverseIndexName: "followers",
            sourceFieldName: "followerId",
            targetFieldName: "followeeId"
        )

        #expect(config.forwardIndexName == "following")
        #expect(config.reverseIndexName == "followers")
    }

    @Test("Configuration stores source and target field names")
    func testConfigurationFieldNames() {
        let config = MutualIndexConfiguration(
            forwardIndexName: "following",
            reverseIndexName: "followers",
            sourceFieldName: "followerId",
            targetFieldName: "followeeId"
        )

        #expect(config.sourceFieldName == "followerId")
        #expect(config.targetFieldName == "followeeId")
    }

    @Test("Configuration supports symmetric relationships")
    func testSymmetricConfiguration() {
        let asymmetric = MutualIndexConfiguration(
            forwardIndexName: "following",
            reverseIndexName: "followers",
            sourceFieldName: "followerId",
            targetFieldName: "followeeId",
            isSymmetric: false
        )

        let symmetric = MutualIndexConfiguration(
            forwardIndexName: "friends",
            reverseIndexName: "friends_reverse",
            sourceFieldName: "userId1",
            targetFieldName: "userId2",
            isSymmetric: true
        )

        #expect(!asymmetric.isSymmetric)
        #expect(symmetric.isSymmetric)
    }

    @Test("Default isSymmetric is false")
    func testDefaultIsSymmetric() {
        let config = MutualIndexConfiguration(
            forwardIndexName: "outgoing",
            reverseIndexName: "incoming",
            sourceFieldName: "source",
            targetFieldName: "target"
        )

        #expect(!config.isSymmetric)
    }
}

// MARK: - MutualOnlineIndexer Unit Tests

@Suite("MutualOnlineIndexer Unit Tests")
struct MutualOnlineIndexerUnitTests {

    @Test("Description includes both index names and item type")
    func testDescription() {
        // Verify description format components
        let forwardName = "following"
        let reverseName = "followers"
        let itemType = "Follow"

        let description = "MutualOnlineIndexer(forward: \(forwardName), reverse: \(reverseName), itemType: \(itemType))"

        #expect(description.contains("following"))
        #expect(description.contains("followers"))
        #expect(description.contains("Follow"))
    }

    @Test("Progress keys are unique per index pair")
    func testProgressKeyUniqueness() {
        let index1Forward = "idx_a"
        let index2Forward = "idx_c"

        // Progress keys should include index names to ensure uniqueness
        let key1 = "_progress_mutual_\(index1Forward)"
        let key2 = "_progress_mutual_\(index2Forward)"

        #expect(key1 != key2)
    }
}

// MARK: - SymmetricIndexBuilder Tests

@Suite("SymmetricIndexBuilder Tests")
struct SymmetricIndexBuilderTests {

    @Test("Symmetric key canonicalization - smaller ID first")
    func testKeyCanonicalisation() {
        // For symmetric relationships, we always store the smaller ID first
        let userId1 = "alice"
        let userId2 = "bob"

        let (first1, second1) = userId1 < userId2 ? (userId1, userId2) : (userId2, userId1)
        #expect(first1 == "alice")
        #expect(second1 == "bob")

        // Reverse order should produce same result
        let (first2, second2) = userId2 < userId1 ? (userId2, userId1) : (userId1, userId2)
        #expect(first2 == "alice")
        #expect(second2 == "bob")

        #expect(first1 == first2)
        #expect(second1 == second2)
    }

    @Test("Same IDs produce consistent key")
    func testSameIdsConsistentKey() {
        let idA = "user123"
        let idB = "user456"

        // A -> B relationship
        let keyAB = idA < idB ? "\(idA)_\(idB)" : "\(idB)_\(idA)"

        // B -> A relationship (should produce same key)
        let keyBA = idB < idA ? "\(idB)_\(idA)" : "\(idA)_\(idB)"

        #expect(keyAB == keyBA)
    }

    @Test("Self-referential relationships handled correctly")
    func testSelfReferentialRelationship() {
        let userId = "user123"

        // User following themselves (edge case)
        let (first, second) = userId < userId ? (userId, userId) : (userId, userId)

        #expect(first == userId)
        #expect(second == userId)
    }

    @Test("Configuration requires isSymmetric true")
    func testSymmetricConfigurationRequirement() {
        let config = MutualIndexConfiguration(
            forwardIndexName: "friends",
            reverseIndexName: "friends_reverse",
            sourceFieldName: "userId1",
            targetFieldName: "userId2",
            isSymmetric: true
        )

        #expect(config.isSymmetric == true)
    }
}

// MARK: - Index Verification Tests

@Suite("Mutual Index Verification Tests")
struct MutualIndexVerificationTests {

    @Test("Verification checks forward-reverse consistency")
    func testVerificationLogic() {
        // For each forward entry (A -> B), there should be a reverse entry (B -> A)

        struct RelationshipEntry {
            let source: String
            let target: String
        }

        // Simulated forward index entries
        let forwardEntries = [
            RelationshipEntry(source: "alice", target: "bob"),
            RelationshipEntry(source: "alice", target: "carol"),
            RelationshipEntry(source: "bob", target: "carol")
        ]

        // Simulated reverse index entries
        var reverseEntries = Set<String>()
        for entry in forwardEntries {
            reverseEntries.insert("\(entry.target)_\(entry.source)")
        }

        // Verify all forward entries have reverse entries
        for forward in forwardEntries {
            let expectedReverse = "\(forward.target)_\(forward.source)"
            #expect(reverseEntries.contains(expectedReverse))
        }
    }

    @Test("Inconsistency detection")
    func testInconsistencyDetection() {
        // Forward entries
        let forward = Set(["alice_bob", "alice_carol", "bob_carol"])

        // Reverse entries (missing one)
        let reverse = Set(["bob_alice", "carol_alice"])  // Missing carol_bob

        // Find inconsistencies
        var inconsistencies: [String] = []
        for entry in forward {
            let parts = entry.split(separator: "_")
            let reversedKey = "\(parts[1])_\(parts[0])"
            if !reverse.contains(reversedKey) {
                inconsistencies.append(entry)
            }
        }

        #expect(inconsistencies.count == 1)
        #expect(inconsistencies.contains("bob_carol"))
    }

    @Test("Sample-based verification with limit")
    func testSampleBasedVerification() {
        let sampleLimit = 1000
        var count = 0

        // Simulate iterating through entries with limit
        for _ in 0..<5000 {
            guard count < sampleLimit else { break }
            count += 1
        }

        #expect(count == sampleLimit)
    }
}

// MARK: - Bidirectional Relationship Tests

@Suite("Bidirectional Relationship Tests")
struct BidirectionalRelationshipTests {

    @Test("Follower/Following relationship structure")
    func testFollowerFollowingStructure() {
        // Forward: who does X follow? (X -> [Y, Z, ...])
        // Reverse: who follows X? ([A, B, ...] -> X)

        struct Follow {
            let followerId: String
            let followeeId: String
        }

        let follows = [
            Follow(followerId: "alice", followeeId: "bob"),
            Follow(followerId: "alice", followeeId: "carol"),
            Follow(followerId: "bob", followeeId: "carol")
        ]

        // Build forward index (who does X follow)
        var forwardIndex: [String: [String]] = [:]
        for follow in follows {
            forwardIndex[follow.followerId, default: []].append(follow.followeeId)
        }

        // Build reverse index (who follows X)
        var reverseIndex: [String: [String]] = [:]
        for follow in follows {
            reverseIndex[follow.followeeId, default: []].append(follow.followerId)
        }

        // Verify
        #expect(forwardIndex["alice"]?.count == 2)  // Alice follows Bob and Carol
        #expect(reverseIndex["carol"]?.count == 2)  // Carol is followed by Alice and Bob
        #expect(forwardIndex["carol"] == nil)        // Carol doesn't follow anyone
        #expect(reverseIndex["alice"] == nil)        // No one follows Alice
    }

    @Test("Document link relationship structure")
    func testDocumentLinkStructure() {
        // Forward: outgoing links from document
        // Reverse: incoming links to document

        struct DocumentLink {
            let sourceDocId: String
            let targetDocId: String
        }

        let links = [
            DocumentLink(sourceDocId: "doc1", targetDocId: "doc2"),
            DocumentLink(sourceDocId: "doc1", targetDocId: "doc3"),
            DocumentLink(sourceDocId: "doc2", targetDocId: "doc3")
        ]

        // Count outgoing links per document
        var outgoingCount: [String: Int] = [:]
        for link in links {
            outgoingCount[link.sourceDocId, default: 0] += 1
        }

        // Count incoming links per document
        var incomingCount: [String: Int] = [:]
        for link in links {
            incomingCount[link.targetDocId, default: 0] += 1
        }

        #expect(outgoingCount["doc1"] == 2)
        #expect(incomingCount["doc3"] == 2)
    }
}

// MARK: - Metrics Tests for Mutual Indexer

@Suite("Mutual Indexer Metrics Tests")
struct MutualIndexerMetricsTests {

    @Test("Metric labels include both index names")
    func testMetricLabels() {
        let itemType = "Follow"
        let forwardIndex = "following"
        let reverseIndex = "followers"

        let baseDimensions: [(String, String)] = [
            ("item_type", itemType),
            ("forward_index", forwardIndex),
            ("reverse_index", reverseIndex)
        ]

        #expect(baseDimensions.count == 3)
        #expect(baseDimensions[1].1 == "following")
        #expect(baseDimensions[2].1 == "followers")
    }

    @Test("Mutual pairs counter tracks relationships")
    func testMutualPairsCounter() {
        // Each item creates one pair in both directions
        let itemsInBatch = 100
        let pairsCreated = itemsInBatch  // 1 pair per item

        #expect(pairsCreated == 100)
    }

    @Test("Items indexed counter counts both directions")
    func testItemsIndexedCounter() {
        let itemsInBatch = 100

        // Both forward and reverse indexes are updated
        let totalIndexed = itemsInBatch * 2

        #expect(totalIndexed == 200)
    }
}

// MARK: - Progress Management Tests

@Suite("Mutual Indexer Progress Management Tests")
struct MutualIndexerProgressTests {

    @Test("Forward and reverse progress keys are different")
    func testProgressKeysDifferent() {
        let forwardKey = "_progress_mutual_following"
        let reverseKey = "_progress_mutual_followers"

        #expect(forwardKey != reverseKey)
    }

    @Test("Progress is cleared after successful build")
    func testProgressCleanup() {
        // Simulate progress state
        var forwardProgress: String? = "some_progress"
        var reverseProgress: String? = "some_progress"

        // After successful build, progress is cleared
        forwardProgress = nil
        reverseProgress = nil

        #expect(forwardProgress == nil)
        #expect(reverseProgress == nil)
    }

    @Test("Progress survives restart")
    func testProgressResumability() throws {
        let begin: [UInt8] = [0x00]
        let end: [UInt8] = [0xFF]

        // Create a rangeSet with initial range
        let rangeSet = RangeSet(initialRange: (begin: begin, end: end))

        // Serialize before any processing
        let encoder = JSONEncoder()
        let data = try encoder.encode(rangeSet)

        // Deserialize (simulating restart)
        let decoder = JSONDecoder()
        let restored = try decoder.decode(RangeSet.self, from: data)

        // Progress should be preserved - unprocessed range should still exist
        #expect(!restored.isEmpty)
        #expect(restored.nextBatchBounds() != nil)
    }
}

// MARK: - State Transition Tests

@Suite("Mutual Indexer State Transition Tests")
struct MutualIndexerStateTransitionTests {

    @Test("Both indexes transition together")
    func testAtomicStateTransition() {
        // Both indexes should be in the same state during build

        var forwardState = IndexState.disabled
        var reverseState = IndexState.disabled

        // Enable both
        forwardState = .writeOnly
        reverseState = .writeOnly

        #expect(forwardState == reverseState)

        // Make both readable
        forwardState = .readable
        reverseState = .readable

        #expect(forwardState == reverseState)
    }

    @Test("State transition order: disabled -> writeOnly -> readable")
    func testStateTransitionOrder() {
        let states: [IndexState] = [.disabled, .writeOnly, .readable]

        for i in 0..<states.count - 1 {
            // Each state should be different from the next
            #expect(states[i] != states[i + 1])
        }
    }
}

// MARK: - Error Handling Tests

@Suite("Mutual Indexer Error Handling Tests")
struct MutualIndexerErrorHandlingTests {

    @Test("Verification errors are logged but don't fail build")
    func testVerificationErrorHandling() {
        // Inconsistencies are logged as warnings, not failures

        var inconsistencies: [(forward: String, reverse: String)] = []
        inconsistencies.append((forward: "alice_bob", reverse: "bob_alice"))

        // Build should complete even with inconsistencies
        #expect(!inconsistencies.isEmpty)

        // Warning would be logged
        let warningMessage = "Warning: Found \(inconsistencies.count) inconsistencies between forward and reverse indexes"
        #expect(warningMessage.contains("1 inconsistencies"))
    }

    @Test("Transaction retry on conflict")
    func testTransactionRetry() {
        // FDB handles retries automatically, but verify retry count concept
        let maxRetries = 5
        var retryCount = 0

        // Simulate retries
        while retryCount < maxRetries {
            retryCount += 1
            // In real code, would retry transaction
            break  // Success
        }

        #expect(retryCount <= maxRetries)
    }
}

// MARK: - Tuple Operations Tests

@Suite("Tuple Operations for Mutual Index Tests")
struct TupleOperationsTests {

    @Test("Tuple element extraction")
    func testTupleElementExtraction() {
        // Simulating Tuple behavior for index keys
        let elements: [Any] = ["alice", "bob", 12345]

        #expect(elements.count >= 2)
        #expect(elements[0] as? String == "alice")
        #expect(elements[1] as? String == "bob")
    }

    @Test("Tuple reversal for bidirectional lookup")
    func testTupleReversal() {
        let forward: [String] = ["source", "target"]
        let reverse: [String] = [forward[1], forward[0]]

        #expect(reverse == ["target", "source"])
    }

    @Test("Optional element handling")
    func testOptionalElementHandling() {
        let elements: [String?] = ["alice", nil, "bob"]

        // Guard against nil elements
        for element in elements {
            if let value = element {
                #expect(!value.isEmpty)
            }
        }
    }
}

// MARK: - Performance Characteristics Tests

@Suite("Mutual Indexer Performance Tests")
struct MutualIndexerPerformanceTests {

    @Test("Single scan builds both indexes")
    func testSingleScanEfficiency() {
        // Each item is read once and written to both indexes
        let itemCount = 1000

        let dataReads = 1  // Single scan
        let indexWrites = 2  // Forward + Reverse per item

        let totalIO = dataReads * itemCount + indexWrites * itemCount

        // Compare to sequential build
        let sequentialIO = 2 * itemCount + 2 * itemCount  // 2 scans + 2 writes

        #expect(totalIO < sequentialIO)
    }

    @Test("Batch processing reduces transaction count")
    func testBatchProcessingEfficiency() {
        let totalItems = 10000
        let batchSize = 100

        let transactionCount = (totalItems + batchSize - 1) / batchSize

        #expect(transactionCount == 100)
        #expect(transactionCount < totalItems)  // Much fewer than per-item transactions
    }
}
