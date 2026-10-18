/// A factual selected-plan property; verbose properties are omitted by default.
struct SQLExplainPlanDetail: Sendable {
    let name: String
    let value: String
    let verboseOnly: Bool

    init(name: String, value: String, verboseOnly: Bool = false) {
        self.name = name
        self.value = value
        self.verboseOnly = verboseOnly
    }
}
