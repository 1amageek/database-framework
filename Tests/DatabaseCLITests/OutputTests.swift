// OutputTests.swift
// DatabaseCLI - Tests for Output formatting utilities

import Testing
import Foundation
@testable import DatabaseCLI

// MARK: - Output Initialization Tests

@Suite("Output - Initialization")
struct OutputInitTests {

    @Test func initWithColors() {
        let output = Output(useColors: true)
        // Should not crash
        output.print("Test")
    }

    @Test func initWithoutColors() {
        let output = Output(useColors: false)
        // Should not crash
        output.print("Test")
    }

    @Test func defaultInit() {
        let output = Output()
        // Should not crash - defaults to colors enabled
        output.print("Test")
    }
}

// MARK: - Table Formatting Tests

@Suite("Output - Table Formatting")
struct OutputTableTests {

    @Test func tableWithEmptyHeaders() {
        let output = Output(useColors: false)
        // Empty headers should not crash
        output.table(headers: [], rows: [])
    }

    @Test func tableWithEmptyRows() {
        let output = Output(useColors: false)
        // Empty rows should not crash
        output.table(headers: ["A", "B"], rows: [])
    }

    @Test func tableWithData() {
        let output = Output(useColors: false)
        // Should format properly without crash
        output.table(
            headers: ["Name", "Value"],
            rows: [
                ["Alice", "100"],
                ["Bob", "200"]
            ]
        )
    }

    @Test func tableWithUnevenRows() {
        let output = Output(useColors: false)
        // Rows with different column counts should not crash
        output.table(
            headers: ["A", "B", "C"],
            rows: [
                ["1", "2", "3"],
                ["4", "5"],  // Missing one column
                ["6"]        // Missing two columns
            ]
        )
    }

    @Test func tableWithLongValues() {
        let output = Output(useColors: false)
        // Long values should be handled
        let longValue = String(repeating: "x", count: 100)
        output.table(
            headers: ["Short", "Long"],
            rows: [
                ["a", longValue],
                ["b", "short"]
            ]
        )
    }
}

// MARK: - Progress Tests

@Suite("Output - Progress")
struct OutputProgressTests {

    @Test func progressZeroTotal() {
        let output = Output(useColors: false)
        // Progress 0/0 should not crash (division by zero protection)
        output.progress(0, 0)
    }

    @Test func progressNormal() {
        let output = Output(useColors: false)
        output.progress(50, 100)  // 50%
        output.progress(100, 100) // 100%
    }

    @Test func progressWithPrefix() {
        let output = Output(useColors: false)
        output.progress(25, 100, prefix: "Building")
    }

    @Test func progressComplete() {
        let output = Output(useColors: false)
        output.progress(100, 100)
        output.progressComplete()
    }

    @Test func progressOverTotal() {
        let output = Output(useColors: false)
        // Progress > total should not crash
        output.progress(150, 100)
    }
}

// MARK: - Styled Output Tests

@Suite("Output - Styled Output")
struct OutputStyledTests {

    @Test func successOutput() {
        let output = Output(useColors: false)
        output.success("Operation completed")
    }

    @Test func errorOutput() {
        let output = Output(useColors: false)
        output.error("Something went wrong")
    }

    @Test func warningOutput() {
        let output = Output(useColors: false)
        output.warning("This might be a problem")
    }

    @Test func infoOutput() {
        let output = Output(useColors: false)
        output.info("Information message")
    }

    @Test func mutedOutput() {
        let output = Output(useColors: false)
        output.muted("Secondary information")
    }

    @Test func headerOutput() {
        let output = Output(useColors: false)
        output.header("Section Title")
    }
}

// MARK: - Structured Output Tests

@Suite("Output - Structured Output")
struct OutputStructuredTests {

    @Test func keyValueOutput() {
        let output = Output(useColors: false)
        output.keyValue("Name", "Alice")
    }

    @Test func listItemOutput() {
        let output = Output(useColors: false)
        output.listItem("First item")
        output.listItem("Nested item", indent: 1)
        output.listItem("Deeply nested", indent: 2)
    }

    @Test func numberedItemOutput() {
        let output = Output(useColors: false)
        output.numberedItem(1, "First")
        output.numberedItem(2, "Second")
    }

    @Test func jsonOutput() {
        let output = Output(useColors: false)
        output.json("{\"name\": \"test\"}")
    }

    @Test func newlineOutput() {
        let output = Output(useColors: false)
        output.newline()
    }
}

// MARK: - Help Output Tests

@Suite("Output - Help")
struct OutputHelpTests {

    @Test func helpAllOutput() {
        let output = Output(useColors: false)
        // Should output all help without crash
        output.helpAll()
    }

    @Test func helpCommandGet() {
        let output = Output(useColors: false)
        output.helpCommand("get")
    }

    @Test func helpCommandQuery() {
        let output = Output(useColors: false)
        output.helpCommand("query")
    }

    @Test func helpCommandInsert() {
        let output = Output(useColors: false)
        output.helpCommand("insert")
    }

    @Test func helpCommandVersions() {
        let output = Output(useColors: false)
        output.helpCommand("versions")
    }

    @Test func helpCommandDiff() {
        let output = Output(useColors: false)
        output.helpCommand("diff")
    }

    @Test func helpCommandIndex() {
        let output = Output(useColors: false)
        output.helpCommand("index")
    }

    @Test func helpCommandRaw() {
        let output = Output(useColors: false)
        output.helpCommand("raw")
    }

    @Test func helpCommandUnknown() {
        let output = Output(useColors: false)
        // Unknown command help should not crash
        output.helpCommand("nonexistent")
    }
}

// MARK: - Welcome/Goodbye Tests

@Suite("Output - Welcome/Goodbye")
struct OutputWelcomeGoodbyeTests {

    @Test func welcomeOutput() {
        let output = Output(useColors: false)
        output.welcome()
    }

    @Test func goodbyeOutput() {
        let output = Output(useColors: false)
        output.goodbye()
    }

    @Test func promptOutput() {
        let output = Output(useColors: false)
        output.prompt()
    }

    @Test func promptWithCustomText() {
        let output = Output(useColors: false)
        output.prompt("custom> ")
    }
}
