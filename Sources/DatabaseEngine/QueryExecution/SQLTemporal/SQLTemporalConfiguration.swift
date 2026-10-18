import DatabaseTypes

/// Explicit immutable SQL zone policy and fixed abbreviation authority.
public struct SQLTemporalConfiguration: Sendable {
    public enum TimeZone: Sendable {
        case utc
        case fixed(offsetSeconds: Int32)
        case named(name: String, rules: SQLTimeZoneRules)
    }

    /// One caller-supplied fixed binding, independent of IANA designations.
    public struct Abbreviation: Sendable {
        public let name: String
        public let offsetSeconds: Int32

        public init(name: String, offsetSeconds: Int32) throws(SQLTimeZoneError) {
            guard (1...10).contains(name.utf8.count),
                  name.utf8.allSatisfy({ (65...90).contains($0) || (97...122).contains($0) }) else {
                throw .malformedData
            }
            self.name = name
            self.offsetSeconds = offsetSeconds
        }
    }

    public struct Limits: Sendable {
        public let maximumAbbreviations: UInt64
        public let maximumRetainedBytes: UInt64

        public init(maximumAbbreviations: UInt64, maximumRetainedBytes: UInt64) {
            self.maximumAbbreviations = maximumAbbreviations
            self.maximumRetainedBytes = maximumRetainedBytes
        }
    }

    public let timeZone: TimeZone
    public let rulesProvider: (any SQLTimeZoneRulesProvider)?
    private let fixedBindings: ByteString

    // Each immutable record has one length byte, ten ASCII bytes and four
    // big-endian offset bytes. The exact ByteString owner is admitted before
    // copying; caller String backing and Array capacity are never retained.
    private static let bindingStride = 15

    public static let utc = SQLTemporalConfiguration()

    private init() {
        timeZone = .utc
        rulesProvider = nil
        fixedBindings = ByteString()
    }

    public init(
        timeZone: TimeZone = .utc,
        rulesProvider: (any SQLTimeZoneRulesProvider)? = nil,
        abbreviations: [Abbreviation] = [],
        limits: Limits
    ) throws(SQLTimeZoneError) {
        let size = abbreviations.count.multipliedReportingOverflow(by: Self.bindingStride)
        let zoneBytes: Int
        if case .named(let name, _) = timeZone {
            try Self.validateZoneName(name)
            zoneBytes = name.utf8.count
        } else { zoneBytes = 0 }
        let retained = size.partialValue.addingReportingOverflow(zoneBytes)
        guard !size.overflow, !retained.overflow,
              UInt64(abbreviations.count) <= limits.maximumAbbreviations,
              UInt64(retained.partialValue) <= limits.maximumRetainedBytes else {
            throw .admissionExceeded
        }
        for index in abbreviations.indices {
            let name = abbreviations[index].name
            guard !Self.isBuiltin(name) else { throw .malformedData }
            for prior in abbreviations[..<index] {
                guard !Self.equalASCII(name, prior.name) else { throw .malformedData }
            }
        }
        self.timeZone = timeZone
        self.rulesProvider = rulesProvider
        fixedBindings = ByteString.copying(count: size.partialValue) { destination in
            _ = destination.initializeMemory(as: UInt8.self, repeating: 0)
            for (index, binding) in abbreviations.enumerated() {
                let start = index * Self.bindingStride
                destination[start] = UInt8(binding.name.utf8.count)
                for (position, byte) in binding.name.utf8.enumerated() {
                    destination[start + 1 + position] = Self.fold(byte)
                }
                let bits = UInt32(bitPattern: binding.offsetSeconds)
                for byte in 0..<4 {
                    destination[start + 11 + byte] = UInt8(truncatingIfNeeded: bits >> ((3 - byte) * 8))
                }
            }
        }
    }

    /// Changes the invocation zone while retaining the admitted provider and bindings.
    func replacing(timeZone: TimeZone) -> Self {
        Self(timeZone: timeZone, rulesProvider: rulesProvider, fixedBindings: fixedBindings)
    }

    private init(timeZone: TimeZone, rulesProvider: (any SQLTimeZoneRulesProvider)?, fixedBindings: ByteString) {
        self.timeZone = timeZone
        self.rulesProvider = rulesProvider
        self.fixedBindings = fixedBindings
    }

    public var retainedByteCount: Int {
        if case .named(let name, _) = timeZone { return fixedBindings.count + name.utf8.count }
        return fixedBindings.count
    }

    static func validateZoneName(_ name: String) throws(SQLTimeZoneError) {
        guard !name.isEmpty, !name.utf8.contains(0) else { throw .malformedData }
    }

    public func fixedOffset(
        forAbbreviation name: String,
        workMeter: DatabaseWorkMeter
    ) throws -> Int32? {
        try workMeter.consume(UInt64(name.utf8.count), at: .expressionEvaluation)
        guard (1...10).contains(name.utf8.count) else { return nil }
        if Self.isBuiltin(name) { return 0 }
        return try fixedBindings.withUnsafeBytes { bytes in
            for start in stride(from: 0, to: bytes.count, by: Self.bindingStride) {
                try workMeter.consume(at: .expressionEvaluation)
                guard Int(bytes[start]) == name.utf8.count else { continue }
                var matches = true
                for (position, byte) in name.utf8.enumerated() {
                    if Self.fold(byte) != bytes[start + 1 + position] { matches = false; break }
                }
                guard matches else { continue }
                var bits: UInt32 = 0
                for byte in 0..<4 { bits = (bits << 8) | UInt32(bytes[start + 11 + byte]) }
                return Int32(bitPattern: bits)
            }
            return nil
        }
    }

    private static func fold(_ byte: UInt8) -> UInt8 {
        (97...122).contains(byte) ? byte - 32 : byte
    }

    private static func equalASCII(_ lhs: String, _ rhs: String) -> Bool {
        lhs.utf8.elementsEqual(rhs.utf8, by: { fold($0) == fold($1) })
    }

    private static func isBuiltin(_ name: String) -> Bool {
        equalASCII(name, "UTC") || equalASCII(name, "GMT") || equalASCII(name, "Z")
    }
}
