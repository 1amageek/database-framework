// FormatVersionTests.swift
// Tests for FormatVersion and FormatVersionManager

import Testing
import Foundation
@testable import DatabaseEngine

@Suite("FormatVersion Tests")
struct FormatVersionTests {

    // MARK: - Version Creation Tests

    @Test func createVersion() {
        let version = FormatVersion(major: 1, minor: 2, patch: 3)

        #expect(version.major == 1)
        #expect(version.minor == 2)
        #expect(version.patch == 3)
    }

    @Test func versionStringRepresentation() {
        let version = FormatVersion(major: 1, minor: 2, patch: 3)

        #expect(version.description == "1.2.3")
    }

    // MARK: - Well-Known Versions Tests

    @Test func wellKnownVersions() {
        #expect(FormatVersion.v1_0_0 == FormatVersion(major: 1, minor: 0, patch: 0))
        #expect(FormatVersion.v1_1_0 == FormatVersion(major: 1, minor: 1, patch: 0))
        #expect(FormatVersion.v1_2_0 == FormatVersion(major: 1, minor: 2, patch: 0))
        #expect(FormatVersion.v1_3_0 == FormatVersion(major: 1, minor: 3, patch: 0))
    }

    @Test func currentVersion() {
        // Current should be v1.3.0
        #expect(FormatVersion.current == FormatVersion.v1_3_0)
    }

    @Test func minimumSupportedVersion() {
        // Minimum supported should be v1.0.0
        #expect(FormatVersion.minimumSupported == FormatVersion.v1_0_0)
        #expect(FormatVersion.minimumWritable == FormatVersion.v1_0_0)
    }

    // MARK: - Version Comparison Tests

    @Test func versionEquality() {
        let v1 = FormatVersion(major: 1, minor: 2, patch: 3)
        let v2 = FormatVersion(major: 1, minor: 2, patch: 3)
        let v3 = FormatVersion(major: 1, minor: 2, patch: 4)

        #expect(v1 == v2)
        #expect(v1 != v3)
    }

    @Test func versionOrdering() {
        let v100 = FormatVersion(major: 1, minor: 0, patch: 0)
        let v110 = FormatVersion(major: 1, minor: 1, patch: 0)
        let v111 = FormatVersion(major: 1, minor: 1, patch: 1)
        let v200 = FormatVersion(major: 2, minor: 0, patch: 0)

        #expect(v100 < v110)
        #expect(v110 < v111)
        #expect(v111 < v200)

        #expect(v200 > v100)
        #expect(v111 > v110)
    }

    @Test func majorVersionTakesPrecedence() {
        let v199 = FormatVersion(major: 1, minor: 99, patch: 99)
        let v200 = FormatVersion(major: 2, minor: 0, patch: 0)

        #expect(v199 < v200)
    }

    // MARK: - Serialization Tests

    @Test func toAndFromBytes() {
        let original = FormatVersion(major: 1, minor: 2, patch: 3)

        let bytes = original.toBytes()
        let decoded = FormatVersion.fromBytes(bytes)

        #expect(decoded == original)
    }

    @Test func fromBytesWithInvalidData() {
        // Too short
        let shortBytes: [UInt8] = [1, 2, 3]
        #expect(FormatVersion.fromBytes(shortBytes) == nil)

        // Empty
        #expect(FormatVersion.fromBytes([]) == nil)
    }

    @Test func tupleElements() {
        let version = FormatVersion(major: 1, minor: 2, patch: 3)

        let elements = version.tupleElements
        #expect(elements.count == 3)
        #expect(elements[0] as? Int == 1)
        #expect(elements[1] as? Int == 2)
        #expect(elements[2] as? Int == 3)
    }

    // MARK: - Hashable Tests

    @Test func versionHashable() {
        let v1 = FormatVersion(major: 1, minor: 2, patch: 3)
        let v2 = FormatVersion(major: 1, minor: 2, patch: 3)

        var set = Set<FormatVersion>()
        set.insert(v1)
        set.insert(v2)

        #expect(set.count == 1)
    }

    // MARK: - FormatFeature Tests

    @Test func featureMinimumVersions() {
        #expect(FormatFeature.largeValueSplitting.minimumVersion == FormatVersion.v1_1_0)
        #expect(FormatFeature.compression.minimumVersion == FormatVersion.v1_2_0)
        #expect(FormatFeature.encryption.minimumVersion == FormatVersion.v1_2_0)
        #expect(FormatFeature.indexOnlyScans.minimumVersion == FormatVersion.v1_0_0)
        #expect(FormatFeature.weakReadSemantics.minimumVersion == FormatVersion.v1_0_0)
        // v1.3.0 features
        #expect(FormatFeature.instrumentation.minimumVersion == FormatVersion.v1_3_0)
        #expect(FormatFeature.remoteFetch.minimumVersion == FormatVersion.v1_3_0)
        #expect(FormatFeature.synchronizedSessions.minimumVersion == FormatVersion.v1_3_0)
        #expect(FormatFeature.preloadCache.minimumVersion == FormatVersion.v1_3_0)
        #expect(FormatFeature.transactionListeners.minimumVersion == FormatVersion.v1_3_0)
    }

    @Test func featureDescriptions() {
        #expect(FormatFeature.largeValueSplitting.description == "Large Value Splitting")
        #expect(FormatFeature.compression.description == "Compression")
        #expect(FormatFeature.encryption.description == "Encryption")
        #expect(FormatFeature.indexOnlyScans.description == "Index-Only Scans")
        #expect(FormatFeature.weakReadSemantics.description == "Weak Read Semantics")
        // v1.3.0 feature descriptions
        #expect(FormatFeature.instrumentation.description == "Instrumentation")
        #expect(FormatFeature.remoteFetch.description == "Remote Fetch")
        #expect(FormatFeature.synchronizedSessions.description == "Synchronized Sessions")
        #expect(FormatFeature.preloadCache.description == "Preload Cache")
        #expect(FormatFeature.transactionListeners.description == "Transaction Listeners")
    }

    @Test func allFeaturesCovered() {
        // Ensure all cases are covered (5 original + 5 from v1.3.0)
        #expect(FormatFeature.allCases.count == 10)
    }

    // MARK: - FormatVersionError Tests

    @Test func tooOldErrorDescription() {
        let error = FormatVersionError.tooOld(
            stored: FormatVersion(major: 0, minor: 9, patch: 0),
            minimum: FormatVersion.v1_0_0
        )

        let description = error.description
        #expect(description.contains("too old"))
        #expect(description.contains("0.9.0"))
        #expect(description.contains("1.0.0"))
    }

    @Test func tooNewErrorDescription() {
        let error = FormatVersionError.tooNew(
            stored: FormatVersion(major: 2, minor: 0, patch: 0),
            current: FormatVersion.v1_2_0
        )

        let description = error.description
        #expect(description.contains("too new"))
        #expect(description.contains("2.0.0"))
        #expect(description.contains("1.2.0"))
    }

    @Test func majorVersionMismatchErrorDescription() {
        let error = FormatVersionError.majorVersionMismatch(
            stored: FormatVersion(major: 2, minor: 0, patch: 0),
            current: FormatVersion(major: 1, minor: 2, patch: 0)
        )

        let description = error.description
        #expect(description.contains("incompatible"))
        #expect(description.contains("Major version"))
    }

    @Test func upgradeFailedErrorDescription() {
        let error = FormatVersionError.upgradeFailed(
            from: FormatVersion.v1_0_0,
            to: FormatVersion.v1_1_0,
            reason: "Test failure"
        )

        let description = error.description
        #expect(description.contains("Failed to upgrade"))
        #expect(description.contains("1.0.0"))
        #expect(description.contains("1.1.0"))
        #expect(description.contains("Test failure"))
    }

    @Test func featureNotAvailableErrorDescription() {
        let error = FormatVersionError.featureNotAvailable(
            feature: .compression,
            version: FormatVersion.v1_0_0
        )

        let description = error.description
        #expect(description.contains("Compression"))
        #expect(description.contains("not available"))
        #expect(description.contains("1.0.0"))
    }
}
