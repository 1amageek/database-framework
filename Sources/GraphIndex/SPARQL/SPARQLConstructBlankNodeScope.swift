import DatabaseDigest
import DatabaseEngine
import DatabaseValue

/// Deterministic, request-accounted blank-node identity for one solution.
struct SPARQLConstructBlankNodeScope: ~Copyable {
    private static let domain: DatabaseBytes = [0x43, 0x42, 0x4e, 0x01]
    private static let cacheContainerByteCount: UInt64 = 64
    private static let cacheSlotByteCount: UInt64 = 128
    private static let stringStorageByteCount: UInt64 = 16
    private static let identifierUTF8Count: UInt64 = 65

    private let resultScope: DatabaseGraphResultScope
    private let bindingFingerprint: DatabaseBytes
    private let occurrence: UInt64
    private let workMeter: DatabaseWorkMeter
    private let reservation: DatabaseIntermediateReservation
    private var identifiers: [String: String]
    private var accountedCapacity: Int

    init(
        resultScope: DatabaseGraphResultScope,
        bindingFingerprint: DatabaseBytes,
        occurrence: UInt64,
        workMeter: DatabaseWorkMeter
    ) throws {
        self.resultScope = resultScope
        self.bindingFingerprint = bindingFingerprint
        self.occurrence = occurrence
        self.workMeter = workMeter
        self.reservation = try workMeter.reserveIntermediate(
            bytes: Self.cacheContainerByteCount,
            at: .resultMaterialization
        )
        self.identifiers = [:]
        self.accountedCapacity = 0
    }

    mutating func identifier(for label: String) throws -> String {
        if let identifier = identifiers[label] {
            return identifier
        }
        let requiredCount = try checkedIncrement(identifiers.count)
        let targetCapacity = try targetCapacity(
            current: accountedCapacity,
            requiredCount: requiredCount
        )
        let capacityBytes = try checkedMultiply(
            UInt64(targetCapacity - accountedCapacity),
            Self.cacheSlotByteCount
        )
        let retainedStrings = try checkedAdd(
            try checkedAdd(
                Self.stringStorageByteCount,
                UInt64(label.utf8.count)
            ),
            try checkedAdd(
                Self.stringStorageByteCount,
                Self.identifierUTF8Count
            )
        )
        try reservation.reserveAdditional(
            rows: 1,
            bytes: try checkedAdd(capacityBytes, retainedStrings),
            at: .resultMaterialization
        )
        if targetCapacity != accountedCapacity {
            identifiers.reserveCapacity(targetCapacity)
            accountedCapacity = targetCapacity
        }

        let hashWork = try checkedAdd(
            UInt64(label.utf8.count),
            UInt64(
                resultScope.bytes.count
                    + bindingFingerprint.count
                    + Self.domain.count
                    + 32
            )
        )
        try workMeter.consume(hashWork, at: .resultMaterialization)
        let identifier = makeIdentifier(for: label)
        let previous = identifiers.updateValue(identifier, forKey: label)
        precondition(
            previous == nil,
            "Blank-node cache membership changed during admitted insertion"
        )
        return identifier
    }

    private func makeIdentifier(for label: String) -> String {
        var hasher = SHA256Accumulator()
        update(Self.domain, hasher: &hasher)
        update(resultScope.bytes, hasher: &hasher)
        update(bindingFingerprint, hasher: &hasher)
        update(occurrence, hasher: &hasher)
        update(label, hasher: &hasher)
        return "g" + hasher.withUnsafeDigestBytes(Self.lowercaseHex)
    }

    private static func lowercaseHex(
        _ digest: UnsafeRawBufferPointer
    ) -> String {
        String(unsafeUninitializedCapacity: 64) { output in
            var outputIndex = 0
            for byte in digest {
                let high = byte >> 4
                let low = byte & 0x0F
                output[outputIndex] = high < 10 ? high + 0x30 : high + 0x57
                output[outputIndex + 1] = low < 10 ? low + 0x30 : low + 0x57
                outputIndex += 2
            }
            precondition(
                outputIndex == output.count,
                "SHA-256 digest length changed unexpectedly"
            )
            return outputIndex
        }
    }

    private func update(
        _ bytes: DatabaseBytes,
        hasher: inout SHA256Accumulator
    ) {
        update(UInt64(bytes.count), hasher: &hasher)
        bytes.withUnsafeBytes {
            hasher.update($0)
        }
    }

    private func update(
        _ value: UInt64,
        hasher: inout SHA256Accumulator
    ) {
        var encoded = value.bigEndian
        withUnsafeBytes(of: &encoded) {
            hasher.update($0)
        }
    }

    private func update(
        _ value: String,
        hasher: inout SHA256Accumulator
    ) {
        update(UInt64(value.utf8.count), hasher: &hasher)
        let usedContiguousStorage = value.utf8.withContiguousStorageIfAvailable {
            bytes in
            hasher.update(UnsafeRawBufferPointer(bytes))
            return true
        } ?? false
        guard !usedContiguousStorage else { return }
        for byte in value.utf8 {
            var byte = byte
            withUnsafeBytes(of: &byte) {
                hasher.update($0)
            }
        }
    }

    private func targetCapacity(
        current: Int,
        requiredCount: Int
    ) throws -> Int {
        guard requiredCount > current else { return current }
        var capacity = max(1, current)
        while capacity < requiredCount {
            let (next, overflow) = capacity.multipliedReportingOverflow(by: 2)
            guard !overflow else { throw limitError() }
            capacity = next
        }
        return capacity
    }

    private func checkedIncrement(_ value: Int) throws -> Int {
        let (result, overflow) = value.addingReportingOverflow(1)
        guard !overflow else { throw limitError() }
        return result
    }

    private func checkedAdd(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = left.addingReportingOverflow(right)
        guard !overflow else { throw limitError() }
        return result
    }

    private func checkedMultiply(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = left.multipliedReportingOverflow(by: right)
        guard !overflow else { throw limitError() }
        return result
    }

    private func limitError() -> DatabaseWorkLimitError {
        DatabaseWorkLimitError.maximumIntermediateBytes(
            stage: .resultMaterialization,
            consumed: workMeter.retainedIntermediateBytes,
            requested: UInt64.max,
            maximum: workMeter.budget.maximumIntermediateBytes
        )
    }
}
