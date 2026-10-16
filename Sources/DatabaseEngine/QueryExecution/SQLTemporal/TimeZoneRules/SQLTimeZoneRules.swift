import DatabaseTypes

/// Validated immutable rules retained by admitted configuration or its operations.
public struct SQLTimeZoneRules: Sendable {
    public struct Transition: Sendable, Equatable {
        public let instant: Timestamp
        public let offsetBefore: Int32
        public let offsetAfter: Int32

        public init(instant: Timestamp, offsetBefore: Int32, offsetAfter: Int32) {
            self.instant = instant
            self.offsetBefore = offsetBefore
            self.offsetAfter = offsetAfter
        }
    }

    public struct Designation: Sendable, Equatable {
        public let offsetSeconds: Int32
        public let abbreviation: ByteString

        public init(offsetSeconds: Int32, abbreviation: ByteString) {
            self.offsetSeconds = offsetSeconds
            self.abbreviation = abbreviation
        }
    }

    let bytes: ByteString
    let block: SQLTimeZoneDataParser.Block
    let recurrence: SQLTimeZoneRecurrence?

    public func offset(at instant: Timestamp, workMeter: DatabaseWorkMeter) throws -> Int32 {
        try designation(at: instant, workMeter: workMeter).offsetSeconds
    }

    public func designation(at instant: Timestamp, workMeter: DatabaseWorkMeter) throws -> Designation {
        try workMeter.consume(at: .expressionEvaluation)
        let second = instant.secondsSinceUnixEpoch
        if let recurrence, block.timeCount == 0 || second >= transitionSecond(block.timeCount - 1) {
            return try recurrence.designation(at: second, bytes: bytes, workMeter: workMeter)
        }
        try requireCoverage(second)
        let next = try upperBound(second, workMeter: workMeter)
        let type = next == 0 ? 0 : transitionType(next - 1)
        return try designation(type: type, workMeter: workMeter)
    }

    public func transition(beforeOrAt instant: Timestamp, workMeter: DatabaseWorkMeter) throws -> Transition? {
        try workMeter.consume(at: .expressionEvaluation)
        let second = instant.secondsSinceUnixEpoch
        try requireCoverage(second)
        let next = try upperBound(second, workMeter: workMeter)
        var result = next == 0 ? nil : explicitTransition(next - 1)
        if let recurrence {
            let candidate = try recurrence.transition(beforeOrAt: second, workMeter: workMeter)
            if let candidate, (block.timeCount == 0 || candidate.instant.secondsSinceUnixEpoch > transitionSecond(block.timeCount - 1)),
               result == nil || candidate.instant > result!.instant {
                result = candidate
            }
        }
        return result
    }

    public func transition(after instant: Timestamp, workMeter: DatabaseWorkMeter) throws -> Transition? {
        try workMeter.consume(at: .expressionEvaluation)
        let second = instant.secondsSinceUnixEpoch
        try requireCoverage(second)
        let next = try upperBound(second, workMeter: workMeter)
        if next < block.timeCount { return explicitTransition(next) }
        guard let recurrence else { return nil }
        return try recurrence.transition(after: second, workMeter: workMeter)
    }

    private func requireCoverage(_ second: Int64) throws(SQLTimeZoneError) {
        if recurrence == nil, block.timeCount > 0, second >= transitionSecond(block.timeCount - 1) {
            throw SQLTimeZoneError.rangeUnavailable
        }
    }

    private func upperBound(_ second: Int64, workMeter: DatabaseWorkMeter) throws -> Int {
        var low = 0
        var high = block.timeCount
        while low < high {
            try workMeter.consume(at: .expressionEvaluation)
            let mid = low + (high - low) / 2
            if transitionSecond(mid) <= second { low = mid + 1 } else { high = mid }
        }
        return low
    }

    func transitionSecond(_ index: Int) -> Int64 {
        bytes.withUnsafeBytes { SQLTimeZoneDataParser.signed64($0, block.times + index * 8) }
    }

    private func transitionType(_ index: Int) -> Int {
        Int(bytes[block.indices + index])
    }

    private func typeOffset(_ index: Int) -> Int32 {
        bytes.withUnsafeBytes { SQLTimeZoneDataParser.signed32($0, block.types + index * 6) }
    }

    private func explicitTransition(_ index: Int) -> Transition {
        Transition(instant: Timestamp(secondsSinceUnixEpoch: transitionSecond(index)),
            offsetBefore: typeOffset(index == 0 ? 0 : transitionType(index - 1)),
            offsetAfter: typeOffset(transitionType(index)))
    }

    private func designation(type: Int, workMeter: DatabaseWorkMeter) throws -> Designation {
        let start = block.designations + Int(bytes[block.types + type * 6 + 5])
        var end = start
        while bytes[end] != 0 { try workMeter.consume(at: .expressionEvaluation); end += 1 }
        let abbreviation = bytes[start..<end]
        if abbreviation.count == 3, abbreviation[0] == 45, abbreviation[1] == 48, abbreviation[2] == 48 {
            throw SQLTimeZoneError.rangeUnavailable
        }
        return Designation(offsetSeconds: typeOffset(type), abbreviation: abbreviation)
    }
}
