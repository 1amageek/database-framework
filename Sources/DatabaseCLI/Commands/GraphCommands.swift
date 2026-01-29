/// GraphCommands - Placeholder for graph queries
///
/// Graph traversal and SPARQL queries require compiled @Persistable types.
/// In catalog-only mode, these commands are not available.
/// Use the embedded mode (with FDBContainer) for graph operations.

import Foundation

struct GraphCommands {

    private let output: OutputFormatter

    init(output: OutputFormatter) {
        self.output = output
    }

    func execute(_ args: [String]) async throws {
        output.info("Graph commands require embedded mode (with compiled @Persistable types).")
        output.info("Start the CLI via DatabaseREPL with an FDBContainer for graph support.")
    }

    func executeSPARQL(_ args: [String]) async throws {
        output.info("SPARQL commands require embedded mode (with compiled @Persistable types).")
        output.info("Start the CLI via DatabaseREPL with an FDBContainer for SPARQL support.")
    }
}

// MARK: - Help

extension GraphCommands {
    static var helpText: String {
        """
        Graph Commands:
          (Not available in standalone mode)

          Graph traversal and SPARQL queries require compiled @Persistable types.
          Use embedded mode with FDBContainer for graph operations.
        """
    }
}
