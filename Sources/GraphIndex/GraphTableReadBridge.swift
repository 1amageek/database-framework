import Foundation
import Core
import QueryIR
import DatabaseEngine
import DatabaseClientProtocol

public enum GraphTableReadBridge {
    public static func registerReadExecutors() {
        LogicalSourceExecutorRegistry.shared.register(RuntimeGraphTableSourceExecutor())
    }
}

private struct RuntimeGraphTableSourceExecutor: GraphTableSourceExecutor {
    func execute(
        context: FDBContext,
        graphTableSource: GraphTableSource
    ) async throws -> [QueryRow] {
        guard let resolution = GraphReadResolver.resolve(
            graphName: graphTableSource.graphName,
            schema: context.container.schema
        ),
              let type = resolution.entity.persistableType else {
            throw ServiceError(
                code: "UNKNOWN_GRAPH",
                message: GraphReadResolver.errorMessage(
                    graphName: graphTableSource.graphName,
                    schema: context.container.schema
                )
            )
        }

        return try await execute(
            context: context,
            type: type,
            graphTableSource: graphTableSource
        )
    }

    private func execute(
        context: FDBContext,
        type: any Persistable.Type,
        graphTableSource: GraphTableSource
    ) async throws -> [QueryRow] {
        try await _execute(
            context: context,
            type: type,
            graphTableSource: graphTableSource
        )
    }

    private func _execute<T: Persistable>(
        context: FDBContext,
        type: T.Type,
        graphTableSource: GraphTableSource
    ) async throws -> [QueryRow] {
        let executor = GraphTableExecutor<T>(
            container: context.container,
            schema: context.container.schema,
            graphTableSource: graphTableSource
        )

        return try await executor.execute().map { row in
            var fields: [String: FieldValue] = [
                "source": .string(row.source),
                "target": .string(row.target),
                "edgeLabel": .string(row.edgeLabel)
            ]
            for (key, value) in row.properties {
                fields[key] = FieldValue(value) ?? .string(String(describing: value))
            }
            return QueryRow(fields: fields)
        }
    }
}
