// CommandRouterTests.swift
// DatabaseCLI - Tests for CommandRouter parsing

import Testing
import Foundation
@testable import DatabaseCLI

// MARK: - Basic Parsing Tests

@Suite("CommandRouter - Basic Parsing")
struct CommandRouterBasicTests {
    let router = CommandRouter()

    @Test func emptyInput() {
        #expect(router.parse("") == .empty)
        #expect(router.parse("   ") == .empty)
    }

    @Test func unknownCommand() {
        let cmd = router.parse("foobar")
        if case .unknown(let input) = cmd {
            #expect(input == "foobar")
        } else {
            Issue.record("Expected unknown command")
        }
    }

    @Test func quitVariants() {
        #expect(router.parse("quit") == .quit)
        #expect(router.parse("exit") == .quit)
        #expect(router.parse("q") == .quit)
        #expect(router.parse("QUIT") == .quit)
        #expect(router.parse("Exit") == .quit)
    }
}

// MARK: - Schema Command Tests

@Suite("CommandRouter - Schema Commands")
struct CommandRouterSchemaTests {
    let router = CommandRouter()

    @Test func schemaList() {
        #expect(router.parse("schema list") == .schemaList)
        #expect(router.parse("SCHEMA LIST") == .schemaList)
        #expect(router.parse("Schema List") == .schemaList)
    }

    @Test func schemaShow() {
        let cmd = router.parse("schema show User")
        if case .schemaShow(let typeName) = cmd {
            #expect(typeName == "User")
        } else {
            Issue.record("Expected schemaShow")
        }
    }

    @Test func schemaShowMissingType() {
        let cmd = router.parse("schema show")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing type")
        }
    }

    @Test func schemaUnknownSubcommand() {
        let cmd = router.parse("schema foo")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for invalid subcommand")
        }
    }

    @Test func schemaMissingSubcommand() {
        let cmd = router.parse("schema")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing subcommand")
        }
    }
}

// MARK: - Data Command Tests

@Suite("CommandRouter - Data Commands")
struct CommandRouterDataTests {
    let router = CommandRouter()

    // --- get ---
    @Test func getBasic() {
        let cmd = router.parse("get User user-123")
        if case .get(let type, let id) = cmd {
            #expect(type == "User")
            #expect(id == "user-123")
        } else {
            Issue.record("Expected get command")
        }
    }

    @Test func getMissingId() {
        let cmd = router.parse("get User")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing id")
        }
    }

    @Test func getMissingType() {
        let cmd = router.parse("get")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing type")
        }
    }

    // --- query ---
    @Test func queryBasic() {
        let cmd = router.parse("query User")
        if case .query(let type, let where_, let limit) = cmd {
            #expect(type == "User")
            #expect(where_ == nil)
            #expect(limit == nil)
        } else {
            Issue.record("Expected query command")
        }
    }

    @Test func queryWithLimit() {
        let cmd = router.parse("query User limit 10")
        if case .query(let type, _, let limit) = cmd {
            #expect(type == "User")
            #expect(limit == 10)
        } else {
            Issue.record("Expected query with limit")
        }
    }

    @Test func queryWithWhere() {
        let cmd = router.parse("query User where name = Alice")
        if case .query(_, let where_, _) = cmd {
            #expect(where_ == "name = Alice")
        } else {
            Issue.record("Expected query with where")
        }
    }

    @Test func queryWithWhereAndLimit() {
        let cmd = router.parse("query User where status = active limit 5")
        if case .query(let type, let where_, let limit) = cmd {
            #expect(type == "User")
            #expect(where_ == "status = active")
            #expect(limit == 5)
        } else {
            Issue.record("Expected query with where and limit")
        }
    }

    @Test func queryMissingType() {
        let cmd = router.parse("query")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing type")
        }
    }

    // --- count ---
    @Test func countBasic() {
        let cmd = router.parse("count User")
        if case .count(let type, let where_) = cmd {
            #expect(type == "User")
            #expect(where_ == nil)
        } else {
            Issue.record("Expected count command")
        }
    }

    @Test func countWithWhere() {
        let cmd = router.parse("count User where active = true")
        if case .count(_, let where_) = cmd {
            #expect(where_ == "active = true")
        } else {
            Issue.record("Expected count with where")
        }
    }

    @Test func countMissingType() {
        let cmd = router.parse("count")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing type")
        }
    }

    // --- insert ---
    @Test func insertWithJSON() {
        let cmd = router.parse("insert User {\"name\": \"Alice\"}")
        if case .insert(let type, let json) = cmd {
            #expect(type == "User")
            #expect(json == "{\"name\": \"Alice\"}")
        } else {
            Issue.record("Expected insert command")
        }
    }

    @Test func insertWithNestedJSON() {
        let cmd = router.parse("insert User {\"name\": \"Alice\", \"address\": {\"city\": \"Tokyo\"}}")
        if case .insert(_, let json) = cmd {
            #expect(json.contains("address"))
            #expect(json.contains("Tokyo"))
        } else {
            Issue.record("Expected insert with nested JSON")
        }
    }

    @Test func insertMissingJSON() {
        let cmd = router.parse("insert User")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing JSON")
        }
    }

    @Test func insertMissingType() {
        let cmd = router.parse("insert")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing type")
        }
    }

    // --- delete ---
    @Test func deleteBasic() {
        let cmd = router.parse("delete User user-123")
        if case .delete(let type, let id) = cmd {
            #expect(type == "User")
            #expect(id == "user-123")
        } else {
            Issue.record("Expected delete command")
        }
    }

    @Test func deleteMissingId() {
        let cmd = router.parse("delete User")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing id")
        }
    }

    @Test func deleteMissingType() {
        let cmd = router.parse("delete")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing type")
        }
    }
}

// MARK: - Version Command Tests

@Suite("CommandRouter - Version Commands")
struct CommandRouterVersionTests {
    let router = CommandRouter()

    @Test func versionsBasic() {
        let cmd = router.parse("versions User user-123")
        if case .versions(let type, let id, let limit) = cmd {
            #expect(type == "User")
            #expect(id == "user-123")
            #expect(limit == nil)
        } else {
            Issue.record("Expected versions command")
        }
    }

    @Test func versionsWithLimit() {
        let cmd = router.parse("versions User user-123 limit 5")
        if case .versions(_, _, let limit) = cmd {
            #expect(limit == 5)
        } else {
            Issue.record("Expected versions with limit")
        }
    }

    @Test func diffBasic() {
        let cmd = router.parse("diff User user-123")
        if case .diff(let type, let id) = cmd {
            #expect(type == "User")
            #expect(id == "user-123")
        } else {
            Issue.record("Expected diff command")
        }
    }

    @Test func versionsMissingId() {
        let cmd = router.parse("versions User")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing id")
        }
    }

    @Test func versionsMissingType() {
        let cmd = router.parse("versions")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing type")
        }
    }

    @Test func diffMissingId() {
        let cmd = router.parse("diff User")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing id")
        }
    }

    @Test func diffMissingType() {
        let cmd = router.parse("diff")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing type")
        }
    }
}

// MARK: - Index Command Tests

@Suite("CommandRouter - Index Commands")
struct CommandRouterIndexTests {
    let router = CommandRouter()

    @Test func indexList() {
        #expect(router.parse("index list") == .indexList)
        #expect(router.parse("INDEX LIST") == .indexList)
    }

    @Test func indexStatus() {
        let cmd = router.parse("index status User_email")
        if case .indexStatus(let name) = cmd {
            #expect(name == "User_email")
        } else {
            Issue.record("Expected indexStatus")
        }
    }

    @Test func indexBuild() {
        let cmd = router.parse("index build User_email")
        if case .indexBuild(let name) = cmd {
            #expect(name == "User_email")
        } else {
            Issue.record("Expected indexBuild")
        }
    }

    @Test func indexScrub() {
        let cmd = router.parse("index scrub User_email")
        if case .indexScrub(let name) = cmd {
            #expect(name == "User_email")
        } else {
            Issue.record("Expected indexScrub")
        }
    }

    @Test func indexMissingSubcommand() {
        let cmd = router.parse("index")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown")
        }
    }

    @Test func indexStatusMissingName() {
        let cmd = router.parse("index status")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing name")
        }
    }

    @Test func indexBuildMissingName() {
        let cmd = router.parse("index build")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing name")
        }
    }

    @Test func indexScrubMissingName() {
        let cmd = router.parse("index scrub")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing name")
        }
    }

    @Test func indexUnknownSubcommand() {
        let cmd = router.parse("index foo")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for invalid subcommand")
        }
    }
}

// MARK: - Raw Command Tests

@Suite("CommandRouter - Raw Commands")
struct CommandRouterRawTests {
    let router = CommandRouter()

    @Test func rawGet() {
        let cmd = router.parse("raw get /R/User/123")
        if case .rawGet(let key) = cmd {
            #expect(key == "/R/User/123")
        } else {
            Issue.record("Expected rawGet")
        }
    }

    @Test func rawGetMissingKey() {
        let cmd = router.parse("raw get")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing key")
        }
    }

    @Test func rawRangeDefault() {
        let cmd = router.parse("raw range /R/User/")
        if case .rawRange(let prefix, let limit) = cmd {
            #expect(prefix == "/R/User/")
            #expect(limit == 10)  // default
        } else {
            Issue.record("Expected rawRange")
        }
    }

    @Test func rawRangeWithLimitKeyword() {
        let cmd = router.parse("raw range /R/User/ limit 5")
        if case .rawRange(_, let limit) = cmd {
            #expect(limit == 5)
        } else {
            Issue.record("Expected rawRange with limit")
        }
    }

    @Test func rawRangeWithLimitNoKeyword() {
        let cmd = router.parse("raw range /R/User/ 20")
        if case .rawRange(_, let limit) = cmd {
            #expect(limit == 20)
        } else {
            Issue.record("Expected rawRange with numeric limit")
        }
    }

    @Test func rawRangeMissingPrefix() {
        let cmd = router.parse("raw range")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing prefix")
        }
    }

    @Test func rawMissingSubcommand() {
        let cmd = router.parse("raw")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for missing subcommand")
        }
    }

    @Test func rawUnknownSubcommand() {
        let cmd = router.parse("raw foo")
        if case .unknown = cmd { } else {
            Issue.record("Expected unknown for invalid subcommand")
        }
    }
}

// MARK: - Help Command Tests

@Suite("CommandRouter - Help Commands")
struct CommandRouterHelpTests {
    let router = CommandRouter()

    @Test func helpBasic() {
        let cmd = router.parse("help")
        if case .help(let command) = cmd {
            #expect(command == nil)
        } else {
            Issue.record("Expected help")
        }
    }

    @Test func helpWithCommand() {
        let cmd = router.parse("help get")
        if case .help(let command) = cmd {
            #expect(command == "get")
        } else {
            Issue.record("Expected help with command")
        }
    }

    @Test func helpWithIndex() {
        let cmd = router.parse("help index")
        if case .help(let command) = cmd {
            #expect(command == "index")
        } else {
            Issue.record("Expected help with index")
        }
    }
}

// MARK: - Tokenization Tests

@Suite("CommandRouter - Tokenization")
struct CommandRouterTokenizationTests {
    let router = CommandRouter()

    @Test func doubleQuotedString() {
        let cmd = router.parse("query User where email = \"alice@example.com\"")
        if case .query(_, let where_, _) = cmd {
            #expect(where_ == "email = alice@example.com")
        } else {
            Issue.record("Expected query with quoted value")
        }
    }

    @Test func singleQuotedString() {
        let cmd = router.parse("query User where name = 'Bob Smith'")
        if case .query(_, let where_, _) = cmd {
            #expect(where_ == "name = Bob Smith")
        } else {
            Issue.record("Expected query with single-quoted value")
        }
    }

    @Test func mixedSpaces() {
        let cmd = router.parse("  get   User    user-123  ")
        if case .get(let type, let id) = cmd {
            #expect(type == "User")
            #expect(id == "user-123")
        } else {
            Issue.record("Expected get with extra spaces handled")
        }
    }

    @Test func quotedStringWithSpaces() {
        let cmd = router.parse("get User \"user with spaces\"")
        if case .get(let type, let id) = cmd {
            #expect(type == "User")
            #expect(id == "user with spaces")
        } else {
            Issue.record("Expected get with quoted id")
        }
    }

    @Test func caseInsensitiveCommands() {
        #expect(router.parse("SCHEMA LIST") == .schemaList)
        #expect(router.parse("Schema List") == .schemaList)
        #expect(router.parse("INDEX LIST") == .indexList)
        #expect(router.parse("HELP") == .help(command: nil))
        #expect(router.parse("GET User user-1") == .get(typeName: "User", id: "user-1"))
    }

    @Test func preserveTypeCasing() {
        // Type names should preserve original casing
        let cmd = router.parse("get MyCustomType id-123")
        if case .get(let type, _) = cmd {
            #expect(type == "MyCustomType")
        } else {
            Issue.record("Expected type name casing to be preserved")
        }
    }
}
