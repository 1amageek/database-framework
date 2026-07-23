#if FOUNDATION_DB
import Testing
import TestHeartbeat
@testable import DatabaseCLICore

@Suite("CommandRouter.tokenize", .heartbeat)
struct TokenizerTests {
    @Test func separatesWhitespace() throws {
        #expect(try CommandRouter.tokenize("raw get key") == ["raw", "get", "key"])
        #expect(try CommandRouter.tokenize("  schema\tshow  Event  ") == ["schema", "show", "Event"])
    }

    @Test func preservesQuotedAndNestedTokens() throws {
        #expect(
            try CommandRouter.tokenize(#"raw get ("event key", [1, 2])"#)
                == ["raw", "get", #"("event key", [1, 2])"#]
        )
        #expect(
            try CommandRouter.tokenize(#"command {"key": ["a", "b"]}"#)
                == ["command", #"{"key": ["a", "b"]}"#]
        )
    }

    @Test func rejectsUnterminatedQuote() {
        #expect(throws: CLIError.self) {
            _ = try CommandRouter.tokenize(#"raw get "unterminated"#)
        }
    }

    @Test func rejectsUnterminatedDelimiter() {
        #expect(throws: CLIError.self) {
            _ = try CommandRouter.tokenize(#"raw get ("key""#)
        }
    }

    @Test func rejectsMismatchedDelimiter() {
        #expect(throws: CLIError.self) {
            _ = try CommandRouter.tokenize("raw get ([key)]")
        }
    }

    @Test func rejectsDanglingEscape() {
        #expect(throws: CLIError.self) {
            _ = try CommandRouter.tokenize(#"raw get "key\"#)
        }
    }
}
#endif
