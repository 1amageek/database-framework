import Testing
@testable import DatabaseCLICore

@Suite("CommandRouter.tokenize")
struct TokenizerTests {

    // MARK: - Basic

    @Test func emptyInput() {
        #expect(CommandRouter.tokenize("").isEmpty)
    }

    @Test func whitespaceOnly() {
        #expect(CommandRouter.tokenize("   ").isEmpty)
        #expect(CommandRouter.tokenize("\t\t").isEmpty)
        #expect(CommandRouter.tokenize(" \t ").isEmpty)
    }

    @Test func singleToken() {
        #expect(CommandRouter.tokenize("help") == ["help"])
    }

    @Test func simpleTokens() {
        #expect(CommandRouter.tokenize("find User --limit 10") == ["find", "User", "--limit", "10"])
    }

    @Test func multipleSpacesAndTabs() {
        #expect(CommandRouter.tokenize("find  User\t\t--limit   10") == ["find", "User", "--limit", "10"])
    }

    // MARK: - Quoted Strings

    @Test func doubleQuotedString() {
        let tokens = CommandRouter.tokenize(#"find User --where name == "Alice Bob""#)
        #expect(tokens == ["find", "User", "--where", "name", "==", #""Alice Bob""#])
    }

    @Test func singleQuotedString() {
        let tokens = CommandRouter.tokenize("find User --where name == 'Alice Bob'")
        #expect(tokens == ["find", "User", "--where", "name", "==", "'Alice Bob'"])
    }

    @Test func emptyQuotedString() {
        let tokens = CommandRouter.tokenize(#"find User --where name == """#)
        #expect(tokens == ["find", "User", "--where", "name", "==", #""""#])
    }

    // MARK: - JSON Objects

    @Test func simpleJSON() {
        let tokens = CommandRouter.tokenize(#"insert User {"name": "Alice"}"#)
        #expect(tokens == ["insert", "User", #"{"name": "Alice"}"#])
    }

    @Test func nestedJSON() {
        let input = #"insert User {"name": "Alice", "address": {"city": "Tokyo", "zip": "100"}}"#
        let tokens = CommandRouter.tokenize(input)
        #expect(tokens.count == 3)
        #expect(tokens[0] == "insert")
        #expect(tokens[1] == "User")
        #expect(tokens[2] == #"{"name": "Alice", "address": {"city": "Tokyo", "zip": "100"}}"#)
    }

    @Test func jsonWithArray() {
        let tokens = CommandRouter.tokenize(#"insert User {"tags": ["a", "b"]}"#)
        #expect(tokens == ["insert", "User", #"{"tags": ["a", "b"]}"#])
    }

    // MARK: - Array Tokens

    @Test func standaloneArray() {
        let tokens = CommandRouter.tokenize(#"cmd [1, 2, 3]"#)
        #expect(tokens == ["cmd", "[1, 2, 3]"])
    }

    @Test func nestedArrays() {
        let tokens = CommandRouter.tokenize(#"cmd [[1, 2], [3, 4]]"#)
        #expect(tokens == ["cmd", "[[1, 2], [3, 4]]"])
    }

    // MARK: - Edge Cases

    @Test func trailingWhitespace() {
        #expect(CommandRouter.tokenize("help   ") == ["help"])
    }

    @Test func leadingWhitespace() {
        #expect(CommandRouter.tokenize("   help") == ["help"])
    }

    @Test func unclosedBrace() {
        // Unclosed brace: rest of input becomes one token
        let tokens = CommandRouter.tokenize(#"insert User {"name": "Alice"#)
        #expect(tokens.count == 3)
        #expect(tokens[2] == #"{"name": "Alice"#)
    }

    @Test func unclosedQuote() {
        // Unclosed quote: rest of input becomes part of current token
        let tokens = CommandRouter.tokenize(#"find User "unclosed"#)
        #expect(tokens.count == 3)
        #expect(tokens[2] == #""unclosed"#)
    }

    // MARK: - Real CLI Commands

    @Test func fullFindCommand() {
        let tokens = CommandRouter.tokenize(#"find User --where age > 30 --sort name desc --limit 10"#)
        #expect(tokens == ["find", "User", "--where", "age", ">", "30", "--sort", "name", "desc", "--limit", "10"])
    }

    @Test func commandWithPartition() {
        let tokens = CommandRouter.tokenize("get Order order-001 --partition tenantId=t_123")
        #expect(tokens == ["get", "Order", "order-001", "--partition", "tenantId=t_123"])
    }

    @Test func insertWithPartition() {
        let tokens = CommandRouter.tokenize(#"insert Order {"total": 100} --partition tenantId=t_123"#)
        #expect(tokens == ["insert", "Order", #"{"total": 100}"#, "--partition", "tenantId=t_123"])
    }
}
