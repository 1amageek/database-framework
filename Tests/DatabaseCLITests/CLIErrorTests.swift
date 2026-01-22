// CLIErrorTests.swift
// DatabaseCLI - Tests for CLIError descriptions

import Testing
import Foundation
@testable import DatabaseCLI

@Suite("CLIError - Description")
struct CLIErrorTests {

    @Test func unknownTypeDescription() {
        let error = CLIError.unknownType("Foo")
        #expect(error.description.contains("Foo"))
        #expect(error.description.contains("schema list"))
    }

    @Test func itemNotFoundDescription() {
        let error = CLIError.itemNotFound(type: "User", id: "123")
        #expect(error.description.contains("User"))
        #expect(error.description.contains("123"))
    }

    @Test func invalidJSONDescription() {
        let error = CLIError.invalidJSON("missing brace")
        #expect(error.description.contains("missing brace"))
    }

    @Test func operationFailedDescription() {
        let error = CLIError.operationFailed("timeout")
        #expect(error.description.contains("timeout"))
    }

    @Test func notConnectedDescription() {
        let error = CLIError.notConnected
        #expect(error.description.contains("connect"))
    }
}

// MARK: - Error as Error Protocol

@Suite("CLIError - Error Protocol")
struct CLIErrorProtocolTests {

    @Test func conformsToError() {
        let error: Error = CLIError.unknownType("Test")
        #expect(error is CLIError)
    }

    @Test func localizedDescription() {
        let error = CLIError.unknownType("TestType")
        let localized = error.localizedDescription
        // localizedDescription should provide some meaningful text
        #expect(!localized.isEmpty)
    }
}
