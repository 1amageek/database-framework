import DatabaseTypes

/// An admitted immutable snapshot of explicitly supplied IANA rules.
public struct SQLTimeZoneDatabase: SQLTimeZoneRulesProvider, Sendable {
    public struct Definition: Sendable {
        public let name: String
        public let tzif: ByteString
        public init(name: String, tzif: ByteString) { self.name = name; self.tzif = tzif }
    }
    public struct Alias: Sendable {
        public let name: String
        public let target: String
        public init(name: String, target: String) { self.name = name; self.target = target }
    }
    public struct Limits: Sendable {
        public let maximumInputBytes: UInt64
        public let maximumRetainedBytes: UInt64
        public let maximumTransitions: UInt64
        public init(maximumInputBytes: UInt64, maximumRetainedBytes: UInt64, maximumTransitions: UInt64) {
            self.maximumInputBytes = maximumInputBytes
            self.maximumRetainedBytes = maximumRetainedBytes
            self.maximumTransitions = maximumTransitions
        }
    }

    private struct Record: Sendable {
        let name: ByteString
        let rules: SQLTimeZoneRules
    }

    // All slots are initialized before publication, then read immutably. The
    // private pointer never escapes. The owner deinitializes/deallocates the
    // exact typed region once; failed construction releases its initialized
    // prefix. This immutable backing proof is identical on every target.
    private final class Storage: @unchecked Sendable {
        let records: UnsafeMutablePointer<Record>
        let count: Int
        let provenance: ByteString
        let inputBytes: UInt64
        let retainedBytes: UInt64
        let transitions: UInt64

        init(definitions: [Definition], aliases: [Alias], provenance: ByteString,
             inputBytes: UInt64, retainedBytes: UInt64, limits: Limits) throws {
            let count = definitions.count + aliases.count
            let records = UnsafeMutablePointer<Record>.allocate(capacity: max(1, count))
            var initialized = 0
            var published = false
            defer {
                if !published { records.deinitialize(count: initialized); records.deallocate() }
            }
            var transitions: UInt64 = 0
            for definition in definitions {
                let (block, recurrence) = try SQLTimeZoneDataParser.parse(definition.tzif)
                try SQLTimeZoneDatabase.add(UInt64(block.timeCount), to: &transitions, maximum: limits.maximumTransitions)
                let record = Record(name: ByteString(utf8: definition.name),
                    rules: SQLTimeZoneRules(bytes: SQLTimeZoneDatabase.owned(definition.tzif), block: block, recurrence: recurrence))
                try Self.checkCollision(record.name, records: records, count: initialized)
                records.advanced(by: initialized).initialize(to: record)
                initialized += 1
            }
            for alias in aliases {
                var target = alias.target
                var matched: Int?
                for _ in 0...aliases.count {
                    for index in 0..<definitions.count {
                        if SQLTimeZoneDatabase.equal(definitions[index].name, target) { matched = index; break }
                    }
                    if matched != nil { break }
                    var followed = false
                    for candidate in aliases {
                        if SQLTimeZoneDatabase.equal(candidate.name, target) { target = candidate.target; followed = true; break }
                    }
                    if !followed { break }
                }
                guard let matched else { throw SQLTimeZoneError.malformedData }
                let name = ByteString(utf8: alias.name)
                try Self.checkCollision(name, records: records, count: initialized)
                records.advanced(by: initialized).initialize(to: Record(name: name, rules: records[matched].rules))
                initialized += 1
            }
            self.records = records
            self.count = count
            self.provenance = SQLTimeZoneDatabase.owned(provenance)
            self.inputBytes = inputBytes
            self.retainedBytes = retainedBytes
            self.transitions = transitions
            published = true
        }

        private static func checkCollision(_ name: ByteString, records: UnsafeMutablePointer<Record>, count: Int) throws(SQLTimeZoneError) {
            for index in 0..<count {
                let existing = records[index].name
                if name.count == existing.count {
                    var equal = true
                    for offset in 0..<name.count {
                        if SQLTimeZoneDatabase.fold(name[offset]) != SQLTimeZoneDatabase.fold(existing[offset]) { equal = false; break }
                    }
                    if equal { throw .malformedData }
                }
            }
        }

        deinit { records.deinitialize(count: count); records.deallocate() }
    }

    private let storage: Storage
    public var inputByteCount: UInt64 { storage.inputBytes }
    public var retainedByteCount: UInt64 { storage.retainedBytes }
    public var transitionCount: UInt64 { storage.transitions }
    /// The caller-verified release and artifact manifest, retained without String capacity assumptions.
    public var provenance: ByteString { storage.provenance }

    public init(definitions: [Definition], aliases: [Alias] = [], provenance: ByteString, limits: Limits) throws {
        let count = Int128(definitions.count) + Int128(aliases.count)
        guard let exactCount = Int(exactly: count) else { throw SQLTimeZoneError.admissionExceeded }
        var input: UInt64 = 0
        // Canonical owner metadata plus the exact typed table, not guessed Array capacity.
        let metadata = Int128(MemoryLayout<SQLTimeZoneDatabase>.stride + MemoryLayout<ByteString>.stride)
            + 32 + max(1, count) * Int128(MemoryLayout<Record>.stride) + 5 * Int128(MemoryLayout<UInt64>.stride)
        guard let retainedMetadata = UInt64(exactly: metadata) else { throw SQLTimeZoneError.admissionExceeded }
        var retained = retainedMetadata
        guard retained <= limits.maximumRetainedBytes, !provenance.isEmpty else { throw SQLTimeZoneError.admissionExceeded }
        try Self.add(UInt64(provenance.count), to: &input, maximum: limits.maximumInputBytes)
        try Self.add(Self.extent(provenance), to: &retained, maximum: limits.maximumRetainedBytes)
        var totalTransitions: UInt64 = 0
        for definition in definitions {
            try Self.validateName(definition.name)
            try Self.add(UInt64(definition.name.utf8.count), to: &input, maximum: limits.maximumInputBytes)
            try Self.add(UInt64(definition.tzif.count), to: &input, maximum: limits.maximumInputBytes)
            try Self.add(UInt64(definition.name.utf8.count), to: &retained, maximum: limits.maximumRetainedBytes)
            try Self.add(Self.extent(definition.tzif), to: &retained, maximum: limits.maximumRetainedBytes)
        }
        for alias in aliases {
            try Self.validateName(alias.name)
            try Self.validateName(alias.target)
            try Self.add(UInt64(alias.name.utf8.count), to: &input, maximum: limits.maximumInputBytes)
            try Self.add(UInt64(alias.target.utf8.count), to: &input, maximum: limits.maximumInputBytes)
            try Self.add(UInt64(alias.name.utf8.count), to: &retained, maximum: limits.maximumRetainedBytes)
        }
        // Validate and admit all transition counts before allocating the table
        // or any copied payload. The immutable input is replayed to fill records.
        for definition in definitions {
            let (block, _) = try SQLTimeZoneDataParser.parse(definition.tzif)
            try Self.add(UInt64(block.timeCount), to: &totalTransitions, maximum: limits.maximumTransitions)
        }
        guard exactCount == definitions.count + aliases.count else { throw SQLTimeZoneError.admissionExceeded }
        storage = try Storage(definitions: definitions, aliases: aliases, provenance: provenance,
            inputBytes: input, retainedBytes: retained, limits: limits)
    }

    public func rules(named name: String, workMeter: DatabaseWorkMeter) throws -> SQLTimeZoneRules {
        try workMeter.consume(at: .expressionEvaluation)
        var nameCount = 0
        for _ in name.utf8 {
            try workMeter.consume(at: .expressionEvaluation)
            nameCount += 1
        }
        for index in 0..<storage.count {
            try workMeter.consume(at: .expressionEvaluation)
            let record = storage.records[index]
            guard record.name.count == nameCount else { continue }
            var offset = 0
            var matches = true
            for byte in name.utf8 {
                try workMeter.consume(at: .expressionEvaluation)
                if Self.fold(byte) != Self.fold(record.name[offset]) { matches = false; break }
                offset += 1
            }
            if matches { return record.rules }
        }
        throw SQLTimeZoneError.unknownZone
    }

    private static func validateName(_ name: String) throws(SQLTimeZoneError) {
        guard !name.isEmpty else { throw .malformedData }
        for byte in name.utf8 {
            guard byte >= 33, byte <= 126 else { throw .malformedData }
        }
    }
    private static func fold(_ byte: UInt8) -> UInt8 { byte >= 65 && byte <= 90 ? byte + 32 : byte }
    private static func equal(_ left: String, _ right: String) -> Bool {
        guard left.utf8.count == right.utf8.count else { return false }
        return zip(left.utf8, right.utf8).allSatisfy { fold($0.0) == fold($0.1) }
    }
    private static func extent(_ bytes: ByteString) -> UInt64 {
        UInt64(bytes.retainedByteCount ?? bytes.count)
    }
    private static func owned(_ bytes: ByteString) -> ByteString {
        guard bytes.retainedByteCount == nil else { return bytes }
        // Unknown foreign extent is replaced once, after exact-copy bootstrap admission.
        return bytes.withUnsafeBytes { source in
            ByteString.copying(count: source.count) { destination in destination.copyMemory(from: source) }
        }
    }
    private static func add(_ value: UInt64, to total: inout UInt64, maximum: UInt64) throws(SQLTimeZoneError) {
        guard total <= maximum, value <= maximum - total else { throw .admissionExceeded }
        total += value
    }
}
