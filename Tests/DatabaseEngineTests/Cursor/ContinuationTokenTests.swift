// ContinuationTokenTests.swift
// DatabaseEngine Tests - ContinuationToken serialization and deserialization tests

import Testing
import Foundation
import FoundationDB
import Core
@testable import DatabaseEngine

@Suite("ContinuationToken Tests", .serialized)
struct ContinuationTokenTests {

    // MARK: - Token Creation Tests

    @Test("Empty token represents end of results")
    func emptyTokenIsEndOfResults() {
        let token = ContinuationToken.endOfResults
        #expect(token.isEndOfResults == true)
        #expect(token.data.isEmpty)
    }

    @Test("Non-empty token is not end of results")
    func nonEmptyTokenIsNotEndOfResults() {
        let token = ContinuationToken(data: [1, 2, 3])
        #expect(token.isEndOfResults == false)
        #expect(token.byteCount == 3)
    }

    // MARK: - Base64 Encoding Tests

    @Test("Base64 round-trip preserves data")
    func base64RoundTrip() throws {
        let originalData: [UInt8] = [0x01, 0x02, 0x03, 0x04, 0xFF, 0xFE]
        let token = ContinuationToken(data: originalData)

        let base64String = token.base64String
        #expect(!base64String.isEmpty)

        let decoded = try ContinuationToken.fromBase64(base64String)
        #expect(decoded.data == originalData)
    }

    @Test("Invalid base64 throws error")
    func invalidBase64ThrowsError() throws {
        #expect(throws: ContinuationError.self) {
            _ = try ContinuationToken.fromBase64("not-valid-base64!!!")
        }
    }

    // MARK: - ContinuationState Tests

    @Test("ContinuationState serialization round-trip")
    func continuationStateRoundTrip() throws {
        let fingerprint = PlanFingerprint.compute(
            operatorDescription: "TestOperator",
            indexNames: ["idx1", "idx2"],
            sortFields: ["field1"]
        )

        let originalState = ContinuationState(
            scanType: .indexScan,
            lastKey: [0x01, 0x02, 0x03],
            reverse: true,
            remainingLimit: 50,
            originalLimit: 100,
            planFingerprint: fingerprint
        )

        let token = originalState.toToken()
        #expect(!token.isEndOfResults)

        let decodedState = try ContinuationState.fromToken(token)

        #expect(decodedState.version == ContinuationToken.currentVersion)
        #expect(decodedState.scanType == .indexScan)
        #expect(decodedState.lastKey == [0x01, 0x02, 0x03])
        #expect(decodedState.reverse == true)
        #expect(decodedState.remainingLimit == 50)
        #expect(decodedState.originalLimit == 100)
        #expect(decodedState.planFingerprint == fingerprint)
    }

    @Test("ContinuationState with nil limits")
    func continuationStateWithNilLimits() throws {
        let fingerprint = PlanFingerprint.compute(
            operatorDescription: "Test",
            indexNames: [],
            sortFields: []
        )

        let originalState = ContinuationState(
            scanType: .tableScan,
            lastKey: [0xAA, 0xBB],
            reverse: false,
            remainingLimit: nil,
            originalLimit: nil,
            planFingerprint: fingerprint
        )

        let token = originalState.toToken()
        let decodedState = try ContinuationState.fromToken(token)

        #expect(decodedState.remainingLimit == nil)
        #expect(decodedState.originalLimit == nil)
    }

    @Test("ContinuationState progress calculation")
    func progressCalculation() {
        let fingerprint: [UInt8] = []

        // With limits
        let state1 = ContinuationState(
            scanType: .indexScan,
            lastKey: [],
            remainingLimit: 25,
            originalLimit: 100,
            planFingerprint: fingerprint
        )
        #expect(state1.progress == 0.75)

        // Without limits
        let state2 = ContinuationState(
            scanType: .indexScan,
            lastKey: [],
            remainingLimit: nil,
            originalLimit: nil,
            planFingerprint: fingerprint
        )
        #expect(state2.progress == nil)
    }

    // MARK: - Error Cases

    @Test("End of results token cannot be deserialized")
    func endOfResultsCannotBeDeserialized() throws {
        let token = ContinuationToken.endOfResults
        #expect(throws: ContinuationError.self) {
            _ = try ContinuationState.fromToken(token)
        }
    }

    @Test("Corrupted token throws error")
    func corruptedTokenThrowsError() throws {
        let corruptedToken = ContinuationToken(data: [0x01, 0x02])  // Too short
        #expect(throws: ContinuationError.self) {
            _ = try ContinuationState.fromToken(corruptedToken)
        }
    }

    // MARK: - Operator State Tests

    @Test("OperatorContinuationState serialization round-trip")
    func operatorStateRoundTrip() throws {
        let originalOpState = OperatorContinuationState(
            unionChildIndex: 2,
            childContinuation: [0x10, 0x20, 0x30],
            exhaustedChildren: [0, 1]
        )

        let serialized = originalOpState.serialize()
        let decoded = try OperatorContinuationState.deserialize(serialized)

        #expect(decoded.unionChildIndex == 2)
        #expect(decoded.childContinuation == [0x10, 0x20, 0x30])
        #expect(decoded.exhaustedChildren == [0, 1])
    }

    @Test("OperatorContinuationState with nil values")
    func operatorStateWithNilValues() throws {
        let originalOpState = OperatorContinuationState(
            unionChildIndex: nil,
            childContinuation: nil,
            exhaustedChildren: nil
        )

        let serialized = originalOpState.serialize()
        let decoded = try OperatorContinuationState.deserialize(serialized)

        #expect(decoded.unionChildIndex == nil)
        #expect(decoded.childContinuation == nil)
    }

    // MARK: - Plan Fingerprint Tests

    @Test("Same plan produces same fingerprint")
    func samePlanSameFingerprint() {
        let fp1 = PlanFingerprint.compute(
            operatorDescription: "IndexScan",
            indexNames: ["idx_a", "idx_b"],
            sortFields: ["name", "age"]
        )

        let fp2 = PlanFingerprint.compute(
            operatorDescription: "IndexScan",
            indexNames: ["idx_a", "idx_b"],
            sortFields: ["name", "age"]
        )

        #expect(fp1 == fp2)
    }

    @Test("Different plan produces different fingerprint")
    func differentPlanDifferentFingerprint() {
        let fp1 = PlanFingerprint.compute(
            operatorDescription: "IndexScan",
            indexNames: ["idx_a"],
            sortFields: ["name"]
        )

        let fp2 = PlanFingerprint.compute(
            operatorDescription: "TableScan",
            indexNames: ["idx_a"],
            sortFields: ["name"]
        )

        #expect(fp1 != fp2)
    }

    // MARK: - NoNextReason Tests

    @Test("NoNextReason descriptions are meaningful")
    func noNextReasonDescriptions() {
        #expect(NoNextReason.sourceExhausted.description == "Source exhausted")
        #expect(NoNextReason.returnLimitReached.description == "Return limit reached")
        #expect(NoNextReason.timeLimitReached.description == "Time limit reached")
        #expect(NoNextReason.transactionLimitReached.description == "Transaction limit reached")
        #expect(NoNextReason.scanLimitReached.description == "Scan limit reached")
    }

    // MARK: - CursorResult Tests

    @Test("CursorResult.more has continuation")
    func cursorResultMoreHasContinuation() {
        let token = ContinuationToken(data: [1, 2, 3])
        let result: CursorResult<CursorTestUser> = .more(items: [], continuation: token)

        #expect(result.hasMore == true)
        #expect(result.continuation != nil)
        #expect(result.noNextReason == nil)
    }

    @Test("CursorResult.done has no continuation")
    func cursorResultDoneHasNoContinuation() {
        let result: CursorResult<CursorTestUser> = .done(items: [], reason: .sourceExhausted)

        #expect(result.hasMore == false)
        #expect(result.continuation == nil)
        #expect(result.noNextReason == .sourceExhausted)
    }

    @Test("CursorResult.empty is empty")
    func cursorResultEmptyIsEmpty() {
        let result: CursorResult<CursorTestUser> = .empty()

        #expect(result.isEmpty == true)
        #expect(result.count == 0)
        #expect(result.hasMore == false)
    }
}

// MARK: - Test Model

/// Test user model for cursor tests
fileprivate struct CursorTestUser: Persistable {
    typealias ID = String

    var id: String
    var name: String
    var age: Int

    init(id: String = UUID().uuidString, name: String, age: Int) {
        self.id = id
        self.name = name
        self.age = age
    }

    static var persistableType: String { "CursorTestUser" }

    static var allFields: [String] { ["id", "name", "age"] }

    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }

    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "age": return age
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<CursorTestUser, Value>) -> String {
        switch keyPath {
        case \CursorTestUser.id: return "id"
        case \CursorTestUser.name: return "name"
        case \CursorTestUser.age: return "age"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<CursorTestUser>) -> String {
        switch keyPath {
        case \CursorTestUser.id: return "id"
        case \CursorTestUser.name: return "name"
        case \CursorTestUser.age: return "age"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<CursorTestUser> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}
