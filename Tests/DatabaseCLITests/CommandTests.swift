// CommandTests.swift
// DatabaseCLI - Tests for Command enum descriptions

import Testing
import Foundation
@testable import DatabaseCLI

@Suite("Command - Description")
struct CommandDescriptionTests {

    // MARK: - Schema Commands

    @Test func schemaListDescription() {
        #expect(Command.schemaList.description == "schema list")
    }

    @Test func schemaShowDescription() {
        let cmd = Command.schemaShow(typeName: "User")
        #expect(cmd.description == "schema show User")
    }

    // MARK: - Data Commands

    @Test func getDescription() {
        let cmd = Command.get(typeName: "User", id: "123")
        #expect(cmd.description == "get User 123")
    }

    @Test func queryDescriptionBasic() {
        let cmd = Command.query(typeName: "User", whereClause: nil, limit: nil)
        #expect(cmd.description == "query User")
    }

    @Test func queryDescriptionWithWhere() {
        let cmd = Command.query(typeName: "User", whereClause: "active = true", limit: nil)
        #expect(cmd.description == "query User where active = true")
    }

    @Test func queryDescriptionWithLimit() {
        let cmd = Command.query(typeName: "User", whereClause: nil, limit: 10)
        #expect(cmd.description == "query User limit 10")
    }

    @Test func queryDescriptionFull() {
        let cmd = Command.query(typeName: "User", whereClause: "active = true", limit: 10)
        #expect(cmd.description == "query User where active = true limit 10")
    }

    @Test func countDescriptionBasic() {
        let cmd = Command.count(typeName: "User", whereClause: nil)
        #expect(cmd.description == "count User")
    }

    @Test func countDescriptionWithWhere() {
        let cmd = Command.count(typeName: "User", whereClause: "status = pending")
        #expect(cmd.description == "count User where status = pending")
    }

    @Test func insertDescription() {
        let cmd = Command.insert(typeName: "User", json: "{\"name\": \"Alice\"}")
        #expect(cmd.description == "insert User {...}")
    }

    @Test func deleteDescription() {
        let cmd = Command.delete(typeName: "User", id: "123")
        #expect(cmd.description == "delete User 123")
    }

    // MARK: - Version Commands

    @Test func versionsDescription() {
        let cmd = Command.versions(typeName: "User", id: "123", limit: nil)
        #expect(cmd.description == "versions User 123")
    }

    @Test func versionsDescriptionWithLimit() {
        let cmd = Command.versions(typeName: "User", id: "123", limit: 5)
        // Note: Current implementation doesn't include limit in description
        #expect(cmd.description == "versions User 123")
    }

    @Test func diffDescription() {
        let cmd = Command.diff(typeName: "User", id: "123")
        #expect(cmd.description == "diff User 123")
    }

    // MARK: - Index Commands

    @Test func indexListDescription() {
        #expect(Command.indexList.description == "index list")
    }

    @Test func indexStatusDescription() {
        #expect(Command.indexStatus(name: "User_email").description == "index status User_email")
    }

    @Test func indexBuildDescription() {
        #expect(Command.indexBuild(name: "User_email").description == "index build User_email")
    }

    @Test func indexScrubDescription() {
        #expect(Command.indexScrub(name: "User_email").description == "index scrub User_email")
    }

    // MARK: - Raw Commands

    @Test func rawGetDescription() {
        #expect(Command.rawGet(key: "/R/User").description == "raw get /R/User")
    }

    @Test func rawRangeDescription() {
        #expect(Command.rawRange(prefix: "/R/", limit: 5).description == "raw range /R/ limit 5")
    }

    // MARK: - Help Commands

    @Test func helpDescriptionBasic() {
        #expect(Command.help(command: nil).description == "help")
    }

    @Test func helpDescriptionWithCommand() {
        #expect(Command.help(command: "get").description == "help get")
    }

    // MARK: - Other Commands

    @Test func quitDescription() {
        #expect(Command.quit.description == "quit")
    }

    @Test func unknownDescription() {
        #expect(Command.unknown(input: "foo").description == "unknown: foo")
    }

    @Test func emptyDescription() {
        #expect(Command.empty.description == "(empty)")
    }
}

// MARK: - Equatable Tests

@Suite("Command - Equatable")
struct CommandEquatableTests {

    @Test func simpleCommandsEquality() {
        #expect(Command.schemaList == Command.schemaList)
        #expect(Command.indexList == Command.indexList)
        #expect(Command.quit == Command.quit)
        #expect(Command.empty == Command.empty)
    }

    @Test func commandsWithParametersEquality() {
        #expect(Command.get(typeName: "User", id: "123") == Command.get(typeName: "User", id: "123"))
        #expect(Command.get(typeName: "User", id: "123") != Command.get(typeName: "User", id: "456"))
        #expect(Command.get(typeName: "User", id: "123") != Command.get(typeName: "Order", id: "123"))
    }

    @Test func queryEquality() {
        let q1 = Command.query(typeName: "User", whereClause: "active", limit: 10)
        let q2 = Command.query(typeName: "User", whereClause: "active", limit: 10)
        let q3 = Command.query(typeName: "User", whereClause: nil, limit: 10)

        #expect(q1 == q2)
        #expect(q1 != q3)
    }

    @Test func differentCommandTypesInequality() {
        #expect(Command.schemaList != Command.indexList)
        #expect(Command.quit != Command.empty)
        #expect(Command.help(command: nil) != Command.quit)
    }
}
