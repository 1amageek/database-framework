// Output.swift
// DatabaseCLI - Output formatting utilities
//
// Provides consistent output formatting for the CLI.

import Foundation

/// Output formatting for the CLI
public final class Output: Sendable {

    // MARK: - ANSI Colors

    private enum Color: String {
        case reset = "\u{001B}[0m"
        case red = "\u{001B}[31m"
        case green = "\u{001B}[32m"
        case yellow = "\u{001B}[33m"
        case blue = "\u{001B}[34m"
        case magenta = "\u{001B}[35m"
        case cyan = "\u{001B}[36m"
        case gray = "\u{001B}[90m"
        case bold = "\u{001B}[1m"
    }

    private let useColors: Bool

    public init(useColors: Bool = true) {
        self.useColors = useColors
    }

    // MARK: - Basic Output

    /// Print a line of text
    public func print(_ text: String) {
        Swift.print(text)
    }

    /// Print an empty line
    public func newline() {
        Swift.print("")
    }

    /// Print the prompt
    public func prompt(_ text: String = "db> ") {
        Swift.print(colored(text, .cyan), terminator: "")
        fflush(stdout)
    }

    // MARK: - Styled Output

    /// Print success message
    public func success(_ text: String) {
        Swift.print(colored(text, .green))
    }

    /// Print error message
    public func error(_ text: String) {
        Swift.print(colored("Error: \(text)", .red))
    }

    /// Print warning message
    public func warning(_ text: String) {
        Swift.print(colored("Warning: \(text)", .yellow))
    }

    /// Print info message
    public func info(_ text: String) {
        Swift.print(colored(text, .blue))
    }

    /// Print muted/gray text
    public func muted(_ text: String) {
        Swift.print(colored(text, .gray))
    }

    // MARK: - Structured Output

    /// Print welcome message
    public func welcome() {
        Swift.print(colored("Database CLI v1.0", .bold))
        Swift.print("Type 'help' for commands, 'quit' to exit.")
        newline()
    }

    /// Print goodbye message
    public func goodbye() {
        Swift.print(colored("Goodbye.", .cyan))
    }

    /// Print a header
    public func header(_ text: String) {
        Swift.print(colored(text, .bold))
    }

    /// Print JSON data
    public func json(_ data: String) {
        Swift.print(data)
    }

    /// Print a key-value pair
    public func keyValue(_ key: String, _ value: String) {
        Swift.print("  \(colored(key, .cyan)): \(value)")
    }

    /// Print a list item
    public func listItem(_ text: String, indent: Int = 0) {
        let prefix = String(repeating: "  ", count: indent)
        Swift.print("\(prefix)- \(text)")
    }

    /// Print a numbered item
    public func numberedItem(_ number: Int, _ text: String) {
        Swift.print("  [\(number)] \(text)")
    }

    // MARK: - Table Output

    /// Print a table with headers and rows
    public func table(headers: [String], rows: [[String]]) {
        guard !headers.isEmpty else { return }

        // Calculate column widths
        var widths = headers.map { $0.count }
        for row in rows {
            for (i, cell) in row.enumerated() where i < widths.count {
                widths[i] = max(widths[i], cell.count)
            }
        }

        // Print header
        let headerLine = headers.enumerated().map { (i, h) in
            h.padding(toLength: widths[i], withPad: " ", startingAt: 0)
        }.joined(separator: "  ")
        Swift.print(colored(headerLine, .bold))

        // Print separator
        let separator = widths.map { String(repeating: "-", count: $0) }.joined(separator: "  ")
        Swift.print(separator)

        // Print rows
        for row in rows {
            let line = row.enumerated().map { (i, cell) in
                if i < widths.count {
                    return cell.padding(toLength: widths[i], withPad: " ", startingAt: 0)
                }
                return cell
            }.joined(separator: "  ")
            Swift.print(line)
        }
    }

    // MARK: - Progress

    /// Print progress update
    public func progress(_ current: Int, _ total: Int, prefix: String = "Progress") {
        let percent = total > 0 ? (current * 100) / total : 0
        Swift.print("\r\(prefix): \(current.formatted()) / \(total.formatted()) (\(percent)%)", terminator: "")
        fflush(stdout)
    }

    /// Complete progress (move to new line)
    public func progressComplete() {
        Swift.print("")
    }

    // MARK: - Help

    /// Print help for all commands
    public func helpAll() {
        header("Available Commands:")
        newline()

        header("Schema")
        Swift.print("  schema list              List all registered entities")
        Swift.print("  schema show <Type>       Show entity details (fields, indexes)")
        newline()

        header("Data")
        Swift.print("  get <Type> <id>          Get item by ID")
        Swift.print("  query <Type> [where ...] Query items with optional filter")
        Swift.print("  count <Type> [where ...] Count items")
        Swift.print("  insert <Type> <json>     Insert new item")
        Swift.print("  delete <Type> <id>       Delete item by ID")
        newline()

        header("Version History")
        Swift.print("  versions <Type> <id>     Show version history")
        Swift.print("  diff <Type> <id>         Show diff from previous version")
        newline()

        header("Index")
        Swift.print("  index list               List all indexes")
        Swift.print("  index status <name>      Show index status")
        Swift.print("  index build <name>       Build/rebuild index")
        Swift.print("  index scrub <name>       Verify index consistency")
        newline()

        header("Raw Access")
        Swift.print("  raw get <key>            Get raw value by key")
        Swift.print("  raw range <prefix> [n]   Range scan (default limit: 10)")
        newline()

        header("Other")
        Swift.print("  help [command]           Show this help or command-specific help")
        Swift.print("  quit / exit              Exit the CLI")
    }

    /// Print help for a specific command
    public func helpCommand(_ command: String) {
        switch command.lowercased() {
        case "get":
            header("get <Type> <id>")
            Swift.print("  Fetch a single item by its ID and display as JSON.")
            Swift.print("")
            Swift.print("  Example:")
            Swift.print("    db> get User user-123")

        case "query":
            header("query <Type> [where <field> = <value>] [limit <n>]")
            Swift.print("  Query items with optional filtering.")
            Swift.print("")
            Swift.print("  Examples:")
            Swift.print("    db> query User")
            Swift.print("    db> query User where email = \"alice@example.com\"")
            Swift.print("    db> query User limit 10")

        case "insert":
            header("insert <Type> <json>")
            Swift.print("  Insert a new item from JSON data.")
            Swift.print("")
            Swift.print("  Example:")
            Swift.print("    db> insert User {\"name\": \"Alice\", \"email\": \"alice@example.com\"}")

        case "versions":
            header("versions <Type> <id> [limit <n>]")
            Swift.print("  Show version history for an item.")
            Swift.print("")
            Swift.print("  Example:")
            Swift.print("    db> versions User user-123")
            Swift.print("    db> versions User user-123 limit 5")

        case "diff":
            header("diff <Type> <id>")
            Swift.print("  Show changes from the previous version.")
            Swift.print("")
            Swift.print("  Example:")
            Swift.print("    db> diff User user-123")

        case "index":
            header("index <subcommand>")
            Swift.print("  Subcommands:")
            Swift.print("    list              List all indexes")
            Swift.print("    status <name>     Show index state (readable/building/disabled)")
            Swift.print("    build <name>      Build or rebuild an index")
            Swift.print("    scrub <name>      Verify index consistency")

        case "raw":
            header("raw <subcommand>")
            Swift.print("  Subcommands:")
            Swift.print("    get <key>         Get raw value at key")
            Swift.print("    range <prefix> [limit]  Scan keys with prefix (default limit: 10)")

        default:
            error("Unknown command: \(command)")
            Swift.print("Type 'help' for available commands.")
        }
    }

    // MARK: - Private

    private func colored(_ text: String, _ color: Color) -> String {
        if useColors {
            return "\(color.rawValue)\(text)\(Color.reset.rawValue)"
        }
        return text
    }
}
