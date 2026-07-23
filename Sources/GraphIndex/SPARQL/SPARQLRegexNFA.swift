/// Immutable prioritized Thompson NFA used for bounded SPARQL matching.
struct SPARQLRegexNFA: Sendable {
    enum State: Sendable {
        case consume(SPARQLRegexCharacterClass, next: Int)
        case epsilon(next: Int)
        case split(first: Int, second: Int)
        case assertStart(next: Int)
        case assertEnd(next: Int)
        case saveStart(group: Int, next: Int)
        case saveEnd(group: Int, next: Int)
        case accept
    }

    struct Match {
        let range: Range<String.Index>
        let captures: CaptureRegisters

        func captureRange(_ group: Int) -> Range<String.Index>? {
            captures.range(for: group)
        }
    }

    struct Scratch {
        var marks: [Int]
        var generation = 0
        var current: [Thread] = []
        var next: [Thread] = []
        var closureStack: [Thread] = []

        init(stateCount: Int) {
            marks = [Int](repeating: 0, count: stateCount)
            current.reserveCapacity(stateCount)
            next.reserveCapacity(stateCount)
            closureStack.reserveCapacity(stateCount)
        }

        mutating func resetLists() {
            current.removeAll(keepingCapacity: true)
            next.removeAll(keepingCapacity: true)
            closureStack.removeAll(keepingCapacity: true)
        }

        mutating func advanceGeneration() -> Int {
            if generation == Int.max {
                for index in marks.indices {
                    marks[index] = 0
                }
                generation = 1
            } else {
                generation += 1
            }
            return generation
        }
    }

    struct Thread {
        let stateIndex: Int
        let matchStart: String.Index
        var captures: CaptureRegisters
    }

    struct CaptureRegisters {
        private struct Slot {
            var lowerBound: String.Index?
            var upperBound: String.Index?

            var range: Range<String.Index>? {
                guard let lowerBound, let upperBound else {
                    return nil
                }
                return lowerBound..<upperBound
            }
        }

        private var first = Slot()
        private var second = Slot()
        private var third = Slot()
        private var fourth = Slot()
        private var fifth = Slot()
        private var sixth = Slot()
        private var seventh = Slot()
        private var eighth = Slot()
        private var ninth = Slot()

        mutating func begin(_ group: Int, at index: String.Index) {
            withSlot(group) { slot in
                slot.lowerBound = index
                slot.upperBound = nil
            }
        }

        mutating func end(_ group: Int, at index: String.Index) {
            withSlot(group) { slot in
                slot.upperBound = index
            }
        }

        func range(for group: Int) -> Range<String.Index>? {
            switch group {
            case 1: return first.range
            case 2: return second.range
            case 3: return third.range
            case 4: return fourth.range
            case 5: return fifth.range
            case 6: return sixth.range
            case 7: return seventh.range
            case 8: return eighth.range
            case 9: return ninth.range
            default: return nil
            }
        }

        private mutating func withSlot(
            _ group: Int,
            _ body: (inout Slot) -> Void
        ) {
            switch group {
            case 1: body(&first)
            case 2: body(&second)
            case 3: body(&third)
            case 4: body(&fourth)
            case 5: body(&fifth)
            case 6: body(&sixth)
            case 7: body(&seventh)
            case 8: body(&eighth)
            case 9: body(&ninth)
            default: break
            }
        }
    }

    let states: [State]
    let startState: Int
    let multiline: Bool

    func firstMatch(
        in input: String,
        from searchStart: String.Index,
        budget: inout SPARQLRegexWorkBudget,
        scratch: inout Scratch
    ) throws -> Match? {
        scratch.resetLists()
        var position = searchStart
        var candidate: Match?
        var generation = scratch.advanceGeneration()

        while true {
            if candidate == nil {
                try addClosure(
                    Thread(
                        stateIndex: startState,
                        matchStart: position,
                        captures: CaptureRegisters()
                    ),
                    at: position,
                    in: input,
                    to: &scratch.current,
                    marks: &scratch.marks,
                    generation: generation,
                    stack: &scratch.closureStack,
                    budget: &budget
                )
            }

            if let acceptedOffset = try firstAcceptedThreadOffset(
                scratch.current,
                budget: &budget
            ) {
                let accepted = scratch.current[acceptedOffset]
                candidate = Match(
                    range: accepted.matchStart..<position,
                    captures: accepted.captures
                )
                if acceptedOffset == 0 {
                    return candidate
                }
                scratch.current.removeSubrange(
                    acceptedOffset..<scratch.current.endIndex
                )
            }

            if scratch.current.isEmpty, let candidate {
                return candidate
            }
            guard position != input.endIndex else {
                return candidate
            }

            let scalar = input.unicodeScalars[position]
            let nextPosition = input.unicodeScalars.index(after: position)
            scratch.next.removeAll(keepingCapacity: true)
            generation = scratch.advanceGeneration()

            for thread in scratch.current {
                try budget.consume(1)
                guard case .consume(let characterClass, let successor) =
                    states[thread.stateIndex]
                else {
                    continue
                }
                try budget.consume(characterClass.matchWork)
                guard characterClass.contains(scalar) else {
                    continue
                }
                try addClosure(
                    Thread(
                        stateIndex: successor,
                        matchStart: thread.matchStart,
                        captures: thread.captures
                    ),
                    at: nextPosition,
                    in: input,
                    to: &scratch.next,
                    marks: &scratch.marks,
                    generation: generation,
                    stack: &scratch.closureStack,
                    budget: &budget
                )
            }

            swap(&scratch.current, &scratch.next)
            position = nextPosition
        }
    }

    private func firstAcceptedThreadOffset(
        _ threads: [Thread],
        budget: inout SPARQLRegexWorkBudget
    ) throws -> Int? {
        for offset in threads.indices {
            try budget.consume(1)
            if case .accept = states[threads[offset].stateIndex] {
                return offset
            }
        }
        return nil
    }

    private func addClosure(
        _ initialThread: Thread,
        at position: String.Index,
        in input: String,
        to destination: inout [Thread],
        marks: inout [Int],
        generation: Int,
        stack: inout [Thread],
        budget: inout SPARQLRegexWorkBudget
    ) throws {
        stack.append(initialThread)
        while var thread = stack.popLast() {
            try budget.consume(1)
            guard marks[thread.stateIndex] != generation else {
                continue
            }
            marks[thread.stateIndex] = generation

            switch states[thread.stateIndex] {
            case .consume, .accept:
                destination.append(thread)
            case .epsilon(let successor):
                thread = successorThread(thread, stateIndex: successor)
                stack.append(thread)
            case .split(let first, let second):
                stack.append(successorThread(thread, stateIndex: second))
                stack.append(successorThread(thread, stateIndex: first))
            case .assertStart(let successor):
                if isStartPosition(position, in: input) {
                    stack.append(
                        successorThread(thread, stateIndex: successor)
                    )
                }
            case .assertEnd(let successor):
                if isEndPosition(position, in: input) {
                    stack.append(
                        successorThread(thread, stateIndex: successor)
                    )
                }
            case .saveStart(let group, let successor):
                thread.captures.begin(group, at: position)
                stack.append(successorThread(thread, stateIndex: successor))
            case .saveEnd(let group, let successor):
                thread.captures.end(group, at: position)
                stack.append(successorThread(thread, stateIndex: successor))
            }
        }
    }

    private func successorThread(
        _ thread: Thread,
        stateIndex: Int
    ) -> Thread {
        Thread(
            stateIndex: stateIndex,
            matchStart: thread.matchStart,
            captures: thread.captures
        )
    }

    private func isStartPosition(
        _ position: String.Index,
        in input: String
    ) -> Bool {
        guard position != input.startIndex else {
            return true
        }
        guard multiline else {
            return false
        }
        let previous = input.unicodeScalars.index(before: position)
        return SPARQLRegexCharacterClass.isLineSeparator(
            input.unicodeScalars[previous].value
        )
    }

    private func isEndPosition(
        _ position: String.Index,
        in input: String
    ) -> Bool {
        guard position != input.endIndex else {
            return true
        }
        guard multiline else {
            return false
        }
        return SPARQLRegexCharacterClass.isLineSeparator(
            input.unicodeScalars[position].value
        )
    }
}
