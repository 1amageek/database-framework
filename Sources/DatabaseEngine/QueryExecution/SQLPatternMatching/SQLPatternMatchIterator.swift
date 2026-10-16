import DatabaseKit

struct SQLPatternMatchIterator: ~Copyable {
    private struct Thread: Sendable {
        let start: String.Index
        let captures: DatabaseSharedRetainedArray<SQLPatternMatch.Capture>
    }
    private struct Pending: Sendable { let state: Int; let thread: Thread }
    private let program: SQLPatternProgram
    private let input: SQLPatternInput
    private var searchStart: String.Index
    private var ended = false

    init(program: SQLPatternProgram, input: SQLPatternInput, start: Int32) throws {
        guard program.meter === input.meter else { throw SQLPatternError.meterMismatch }
        guard start > 0 else { throw SQLPatternError.invalidStart }
        var position = input.bounds.lowerBound
        var remaining = start - 1
        while remaining > 0, position < input.bounds.upperBound {
            try input.meter.consume(at: .expressionEvaluation)
            input.text.unicodeScalars.formIndex(after: &position); remaining -= 1
        }
        self.program = program; self.input = input
        searchStart = position
        ended = remaining > 0
    }
    mutating func next() throws -> SQLPatternMatch? {
        guard !ended else { return nil }
        do {
            guard let match = try find() else { ended = true; return nil }
            searchStart = match.range.upperBound
            if match.range.isEmpty {
                if searchStart == input.bounds.upperBound { ended = true }
                else { input.text.unicodeScalars.formIndex(after: &searchStart) }
            }
            if program.wholeString { ended = true }
            return match
        } catch { ended = true; throw error }
    }
    private func emptyCaptures() throws -> DatabaseSharedRetainedArray<SQLPatternMatch.Capture> {
        var values = try SQLPatternStorage<SQLPatternMatch.Capture>(meter: input.meter, count: program.captureCount)
        for _ in 0..<program.captureCount { try values.append(.init()) }
        return try values.finish()
    }
    private func changed(_ thread: Thread, group: Int, position: String.Index, opening: Bool) throws -> Thread {
        var values = try SQLPatternStorage<SQLPatternMatch.Capture>(meter: input.meter, count: program.captureCount)
        for index in 0..<program.captureCount {
            var capture = thread.captures[index]
            if index == group - 1 {
                if opening { capture.lower = position; capture.upper = nil }
                else { capture.upper = position }
            }
            try values.append(capture)
        }
        return try Thread(start: thread.start, captures: values.finish())
    }
    private func better(_ left: Thread, than right: Thread) throws -> Bool {
        if left.start != right.start { return left.start < right.start }
        for index in 0..<program.captureCount {
            try input.meter.consume(at: .expressionEvaluation)
            let a = left.captures[index]; let b = right.captures[index]
            if a.lower != b.lower {
                guard let lower = a.lower else { return false }
                return b.lower == nil || lower < b.lower!
            }
            if a.upper != b.upper {
                guard let upper = a.upper else { return false }
                return b.upper == nil || upper > b.upper!
            }
        }
        return false
    }
    private func closure(_ state: Int, thread: Thread, at position: String.Index,
                         into states: inout SQLPatternStorage<Thread?>,
                         pending: inout SQLPatternStorage<Pending>) throws {
        try pending.append(.init(state: state, thread: thread))
        while let item = pending.popLast() {
            try input.meter.consume(at: .expressionEvaluation)
            if let existing = states[item.state], try !better(item.thread, than: existing) { continue }
            states[item.state] = item.thread
            switch program.instructions[item.state] {
            case .consume, .accept: break
            case .split(let left, let right):
                try pending.append(.init(state: right, thread: item.thread))
                try pending.append(.init(state: left, thread: item.thread))
            case .saveStart(let group, let next):
                try pending.append(.init(state: next, thread: changed(item.thread, group: group, position: position, opening: true)))
            case .saveEnd(let group, let next):
                try pending.append(.init(state: next, thread: changed(item.thread, group: group, position: position, opening: false)))
            case .start(let next):
                if position == input.bounds.lowerBound || (program.options.newlineAnchors && previousNewline(position)) {
                    try pending.append(.init(state: next, thread: item.thread))
                }
            case .end(let next):
                if position == input.bounds.upperBound || (program.options.newlineAnchors && input.text.unicodeScalars[position] == "\n") {
                    try pending.append(.init(state: next, thread: item.thread))
                }
            case .invalidEscape:
                if position != input.bounds.upperBound { throw SQLPatternError.invalidEscape }
            }
        }
    }
    private func previousNewline(_ position: String.Index) -> Bool {
        position != input.bounds.lowerBound && input.text.unicodeScalars[input.text.unicodeScalars.index(before: position)] == "\n"
    }
    private func find() throws -> SQLPatternMatch? {
        var current = try SQLPatternStorage<Thread?>(meter: input.meter, count: program.instructions.count)
        var next = try SQLPatternStorage<Thread?>(meter: input.meter, count: program.instructions.count)
        for _ in 0..<program.instructions.count { try current.append(nil); try next.append(nil) }
        var pending = try SQLPatternStorage<Pending>(meter: input.meter)
        let captures = try emptyCaptures()
        var position = searchStart
        var candidate: SQLPatternMatch?
        while true {
            try input.meter.consume(at: .expressionEvaluation)
            if candidate == nil && (!program.wholeString || position == input.bounds.lowerBound) {
                try closure(program.startState, thread: .init(start: position, captures: captures),
                            at: position, into: &current, pending: &pending)
            }
            var active = false
            for state in 0..<program.instructions.count {
                try input.meter.consume(at: .expressionEvaluation)
                guard let thread = current[state] else { continue }
                if case .accept = program.instructions[state], !program.wholeString || position == input.bounds.upperBound {
                    if candidate == nil || thread.start < candidate!.range.lowerBound ||
                        (thread.start == candidate!.range.lowerBound && position > candidate!.range.upperBound) {
                        candidate = .init(input: input, range: thread.start..<position, captures: thread.captures)
                    } else if let old = candidate, thread.start == old.range.lowerBound, position == old.range.upperBound,
                              try better(thread, than: .init(start: old.range.lowerBound, captures: old.captures)) {
                        candidate = .init(input: input, range: thread.start..<position, captures: thread.captures)
                    }
                }
                if case .consume = program.instructions[state], candidate == nil || thread.start <= candidate!.range.lowerBound { active = true }
            }
            if let candidate, !active { return candidate }
            if position == input.bounds.upperBound { return candidate }
            let scalar = input.text.unicodeScalars[position]
            let following = input.text.unicodeScalars.index(after: position)
            for state in 0..<program.instructions.count { try input.meter.consume(at: .expressionEvaluation); next[state] = nil }
            for state in 0..<program.instructions.count {
                try input.meter.consume(at: .expressionEvaluation)
                guard let thread = current[state], candidate == nil || thread.start <= candidate!.range.lowerBound,
                      case .consume(let characterClass, let successor) = program.instructions[state],
                      try program.accepts(characterClass, scalar: scalar) else { continue }
                try closure(successor, thread: thread, at: following, into: &next, pending: &pending)
            }
            swap(&current, &next)
            position = following
        }
    }
}
