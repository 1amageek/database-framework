import DatabaseKit
import DatabaseEngine
import DatabaseTypes

/// Deterministic, request-accounted blank-node identity for one solution.
struct SPARQLConstructBlankNodeResolver: ~Copyable {
    private static let domain: ByteString = [0x43, 0x42, 0x4e, 0x01]

    private let nodeNamespace: GraphResultNodeNamespace
    private let bindingFingerprint: ByteString
    private let occurrence: UInt64
    private let workMeter: DatabaseWorkMeter

    init(
        nodeNamespace: GraphResultNodeNamespace,
        bindingFingerprint: ByteString,
        occurrence: UInt64,
        workMeter: DatabaseWorkMeter
    ) {
        self.nodeNamespace = nodeNamespace
        self.bindingFingerprint = bindingFingerprint
        self.occurrence = occurrence
        self.workMeter = workMeter
    }

    mutating func identifier(for label: String) throws -> String {
        let hashWork = try checkedAdd(
            UInt64(label.utf8.count),
            UInt64(
                nodeNamespace.bytes.count
                    + bindingFingerprint.count
                    + Self.domain.count
                    + 32
            )
        )
        try workMeter.consume(hashWork, at: .resultMaterialization)
        return makeIdentifier(for: label)
    }

    private func makeIdentifier(for label: String) -> String {
        var hasher = SHA256Accumulator()
        update(Self.domain, hasher: &hasher)
        update(nodeNamespace.bytes, hasher: &hasher)
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
        _ bytes: ByteString,
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

    private func checkedAdd(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = left.addingReportingOverflow(right)
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
