/// HistoryCommands - Placeholder for version history queries
///
/// Version history queries require compiled @Persistable types.
/// In catalog-only mode, these commands are not available.
/// Use the embedded mode (with FDBContainer) for version history.

import Foundation

struct HistoryCommands {

    private let output: OutputFormatter

    init(output: OutputFormatter) {
        self.output = output
    }

    /// Usage: history <TypeName> <id> [--limit N]
    func execute(_ args: [String]) async throws {
        output.info("History commands require embedded mode (with compiled @Persistable types).")
        output.info("Start the CLI via DatabaseREPL with an FDBContainer for version history.")
    }
}

// MARK: - Help

extension HistoryCommands {
    static var helpText: String {
        """
        History Commands:
          (Not available in standalone mode)

          Version history queries require compiled @Persistable types.
          Use embedded mode with FDBContainer for version history.
        """
    }
}
