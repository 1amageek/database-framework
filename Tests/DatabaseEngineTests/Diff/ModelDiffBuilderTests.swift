import Core
import DatabaseEngine
import Testing

@Persistable
private struct ModelDiffTestEntity {
    var name: String
    var score: Int64
}

@Suite("Model diff execution")
struct ModelDiffBuilderTests {
    @Test("A field comparator controls the emitted change type")
    func comparatorControlsChangeType() throws {
        let old = ModelDiffTestEntity(name: "Document", score: 10)
        var new = old
        new.score = 11
        let options = DiffOptions(
            includesUnchangedFields: true,
            comparators: [
                "score": { _, _ in true }
            ]
        )

        let diff = try ModelDiffBuilder.diff(
            old: old,
            new: new,
            options: options
        )
        let scoreChange = try #require(diff.change(for: "score"))

        #expect(scoreChange.changeType == .unchanged)
    }

    @Test("Excluded fields do not enter the result")
    func excludedFieldsDoNotEnterResult() throws {
        let old = ModelDiffTestEntity(name: "Old", score: 10)
        var new = old
        new.name = "New"
        new.score = 11

        let diff = try ModelDiffBuilder.diff(
            old: old,
            new: new,
            options: DiffOptions(excludedFields: ["score"])
        )

        #expect(diff.changedFields == ["name"])
        #expect(diff.change(for: "score") == nil)
    }

    @Test("Diff options retain comparator behavior without serialization")
    func optionsRetainComparatorBehavior() throws {
        let options = DiffOptions().comparing(field: "name") { left, right in
            left == right
        }
        let comparator = try #require(options.comparators["name"])

        #expect(comparator(.string("same"), .string("same")))
        #expect(!comparator(.string("left"), .string("right")))
    }
}
