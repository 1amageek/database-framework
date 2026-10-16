struct SQLPatternSplitIterator: ~Copyable {
    private let input: SQLPatternInput
    private var matches: SQLPatternMatchIterator
    private var emittedThrough: String.Index
    private var ended = false
    init(program: SQLPatternProgram, input: SQLPatternInput) throws {
        let preparedMatches = try program.makeMatchIterator(in: input)
        self.input = input
        matches = consume preparedMatches
        emittedThrough = input.bounds.lowerBound
    }
    mutating func next() throws -> SQLPatternSlice? {
        guard !ended else { return nil }
        do {
            while let match = try matches.next() {
                let range = match.range
                if range.isEmpty && (range.lowerBound == emittedThrough || range.lowerBound == input.bounds.upperBound) { continue }
                let result = SQLPatternSlice(input: input, range: emittedThrough..<range.lowerBound)
                emittedThrough = range.upperBound
                return result
            }
            ended = true
            return .init(input: input, range: emittedThrough..<input.bounds.upperBound)
        } catch { ended = true; throw error }
    }
}
