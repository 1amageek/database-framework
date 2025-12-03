// SynchronizedSessionTests.swift
// DatabaseEngine Tests - Synchronized session tests

import Testing
import Foundation
@testable import DatabaseEngine

// MARK: - SessionConfiguration Tests

@Suite("SessionConfiguration Tests")
struct SessionConfigurationTests {

    @Test("Default configuration values")
    func defaultConfiguration() {
        let config = SessionConfiguration.default(name: "test-session")

        #expect(config.sessionName == "test-session")
        #expect(config.lockTimeoutSeconds == 30.0)
        #expect(config.renewalIntervalSeconds == 10.0)
        #expect(config.allowLockStealing == true)
        #expect(config.staleThresholdSeconds == 60.0)
    }

    @Test("Long running configuration")
    func longRunningConfiguration() {
        let config = SessionConfiguration.longRunning(name: "long-session")

        #expect(config.sessionName == "long-session")
        #expect(config.lockTimeoutSeconds == 300.0)
        #expect(config.renewalIntervalSeconds == 60.0)
        #expect(config.staleThresholdSeconds == 600.0)
    }

    @Test("Custom configuration")
    func customConfiguration() {
        let config = SessionConfiguration(
            sessionId: UUID(),
            sessionName: "custom",
            lockTimeoutSeconds: 60.0,
            renewalIntervalSeconds: 20.0,
            allowLockStealing: false,
            staleThresholdSeconds: 120.0
        )

        #expect(config.sessionName == "custom")
        #expect(config.lockTimeoutSeconds == 60.0)
        #expect(config.renewalIntervalSeconds == 20.0)
        #expect(config.allowLockStealing == false)
        #expect(config.staleThresholdSeconds == 120.0)
    }

    @Test("Configuration equality")
    func configurationEquality() {
        let id = UUID()
        let config1 = SessionConfiguration(sessionId: id, sessionName: "test")
        let config2 = SessionConfiguration(sessionId: id, sessionName: "test")

        #expect(config1 == config2)
    }
}

// MARK: - LockHolder Tests

@Suite("LockHolder Tests")
struct LockHolderTests {

    @Test("Lock holder creation")
    func lockHolderCreation() {
        let now = Date()
        let expiry = now.addingTimeInterval(30)

        let holder = LockHolder(
            sessionId: UUID(),
            sessionName: "test",
            acquiredAt: now,
            lastHeartbeat: now,
            expiresAt: expiry
        )

        #expect(holder.sessionName == "test")
        #expect(holder.acquiredAt == now)
        #expect(holder.expiresAt == expiry)
    }

    @Test("Lock expired check")
    func lockExpiredCheck() {
        let past = Date().addingTimeInterval(-100)

        let holder = LockHolder(
            sessionId: UUID(),
            sessionName: "test",
            acquiredAt: past,
            lastHeartbeat: past,
            expiresAt: past.addingTimeInterval(30) // Already expired
        )

        #expect(holder.isExpired == true)
    }

    @Test("Lock not expired check")
    func lockNotExpiredCheck() {
        let now = Date()

        let holder = LockHolder(
            sessionId: UUID(),
            sessionName: "test",
            acquiredAt: now,
            lastHeartbeat: now,
            expiresAt: now.addingTimeInterval(300) // 5 minutes from now
        )

        #expect(holder.isExpired == false)
    }

    @Test("Lock stale check")
    func lockStaleCheck() {
        let past = Date().addingTimeInterval(-100)

        let holder = LockHolder(
            sessionId: UUID(),
            sessionName: "test",
            acquiredAt: past,
            lastHeartbeat: past, // Old heartbeat
            expiresAt: Date().addingTimeInterval(300) // Not expired
        )

        // With 60 second threshold, should be stale (100 seconds old)
        #expect(holder.isStale(threshold: 60) == true)

        // With 200 second threshold, should not be stale
        #expect(holder.isStale(threshold: 200) == false)
    }

    @Test("Lock holder encoding and decoding")
    func lockHolderEncodingDecoding() throws {
        let original = LockHolder(
            sessionId: UUID(),
            sessionName: "encode-test",
            acquiredAt: Date(),
            lastHeartbeat: Date(),
            expiresAt: Date().addingTimeInterval(30)
        )

        let encoded = try original.encode()
        let decoded = try LockHolder.decode(from: encoded)

        #expect(decoded.sessionId == original.sessionId)
        #expect(decoded.sessionName == original.sessionName)
    }
}

// MARK: - LockStatus Tests

@Suite("LockStatus Tests")
struct LockStatusTests {

    @Test("Lock not held status")
    func lockNotHeldStatus() {
        let status = LockStatus(
            isHeld: false,
            holder: nil,
            isOurs: false,
            isStale: false
        )

        #expect(status.isHeld == false)
        #expect(status.holder == nil)
        #expect(status.isOurs == false)
    }

    @Test("Lock held by us status")
    func lockHeldByUsStatus() {
        let holder = LockHolder(
            sessionId: UUID(),
            sessionName: "test",
            acquiredAt: Date(),
            lastHeartbeat: Date(),
            expiresAt: Date().addingTimeInterval(30)
        )

        let status = LockStatus(
            isHeld: true,
            holder: holder,
            isOurs: true,
            isStale: false
        )

        #expect(status.isHeld == true)
        #expect(status.isOurs == true)
        #expect(status.isStale == false)
    }

    @Test("Stale lock status")
    func staleLockStatus() {
        let holder = LockHolder(
            sessionId: UUID(),
            sessionName: "stale",
            acquiredAt: Date().addingTimeInterval(-1000),
            lastHeartbeat: Date().addingTimeInterval(-1000),
            expiresAt: Date().addingTimeInterval(30)
        )

        let status = LockStatus(
            isHeld: true,
            holder: holder,
            isOurs: false,
            isStale: true
        )

        #expect(status.isStale == true)
        #expect(status.isOurs == false)
    }
}

// MARK: - SessionError Tests

@Suite("SessionError Tests")
struct SessionErrorTests {

    @Test("Lock not acquired error description")
    func lockNotAcquiredError() {
        let error = SessionError.lockNotAcquired(sessionName: "my-session")

        #expect(error.description.contains("my-session"))
        #expect(error.description.contains("acquire"))
    }

    @Test("Lock lost error description")
    func lockLostError() {
        let error = SessionError.lockLost(sessionName: "my-session")

        #expect(error.description.contains("my-session"))
        #expect(error.description.contains("lost"))
    }

    @Test("Invalid lock data error description")
    func invalidLockDataError() {
        let error = SessionError.invalidLockData("corrupted data")

        #expect(error.description.contains("corrupted data"))
    }
}
