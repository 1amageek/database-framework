import Testing
@testable import DatabaseCLICore

@Suite("JSONParser")
struct JSONParserTests {

    // MARK: - parse

    @Test func parseSimpleObject() throws {
        let dict = try JSONParser.parse(#"{"name": "Alice", "age": 30}"#)
        #expect(dict["name"] as? String == "Alice")
        #expect(dict["age"] as? Int == 30)
    }

    @Test func parseNestedObject() throws {
        let dict = try JSONParser.parse(#"{"user": {"name": "Bob"}}"#)
        let nested = dict["user"] as? [String: Any]
        #expect(nested?["name"] as? String == "Bob")
    }

    @Test func parseArrayField() throws {
        let dict = try JSONParser.parse(#"{"tags": ["a", "b", "c"]}"#)
        let tags = dict["tags"] as? [String]
        #expect(tags == ["a", "b", "c"])
    }

    @Test func parseEmptyObject() throws {
        let dict = try JSONParser.parse("{}")
        #expect(dict.isEmpty)
    }

    @Test func parseInvalidJSON() {
        #expect(throws: CLIError.self) {
            _ = try JSONParser.parse("not json")
        }
    }

    @Test func parseJSONArray() {
        // Top-level array is not a dictionary → error
        #expect(throws: CLIError.self) {
            _ = try JSONParser.parse("[1, 2, 3]")
        }
    }

    @Test func parseEmptyString() {
        #expect(throws: CLIError.self) {
            _ = try JSONParser.parse("")
        }
    }

    // MARK: - stringify

    @Test func stringifySimple() throws {
        let dict: [String: Any] = ["name": "Alice", "age": 30]
        let json = try JSONParser.stringify(dict)
        #expect(json.contains("\"name\""))
        #expect(json.contains("\"Alice\""))
        #expect(json.contains("30"))
    }

    @Test func stringifyEmpty() throws {
        let json = try JSONParser.stringify([:])
        let trimmed = json.trimmingCharacters(in: .whitespacesAndNewlines)
        #expect(trimmed == "{\n\n}" || trimmed == "{}")
    }

    // MARK: - Roundtrip

    @Test func roundtrip() throws {
        let original: [String: Any] = ["key": "value", "num": 42]
        let json = try JSONParser.stringify(original, pretty: false)
        let restored = try JSONParser.parse(json)
        #expect(restored["key"] as? String == "value")
        #expect(restored["num"] as? Int == 42)
    }
}
