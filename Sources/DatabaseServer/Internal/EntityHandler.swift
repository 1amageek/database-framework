import Foundation
import Core
import DatabaseEngine
import DatabaseClientProtocol

/// Type-erased handler for entity operations.
struct EntityHandler: Sendable {
    let applyChanges: @Sendable (FDBContext, [ChangeSet.Change]) async throws -> Void

    static func build<T: Persistable>(for type: T.Type) -> EntityHandler {
        let encoder = JSONEncoder()
        let decoder = JSONDecoder()

        return EntityHandler(
            applyChanges: { context, changes in
                for change in changes {
                    switch change.operation {
                    case .insert, .update:
                        guard let fields = change.fields else { continue }
                        let data = try encoder.encode(fields)
                        let item = try decoder.decode(T.self, from: data)
                        context.insert(item)

                    case .delete:
                        if let fields = change.fields {
                            let data = try encoder.encode(fields)
                            let item = try decoder.decode(T.self, from: data)
                            context.delete(item)
                        } else {
                            let idDict: [String: FieldValue] = ["id": .string(change.id)]
                            let data = try encoder.encode(idDict)
                            let item = try decoder.decode(T.self, from: data)
                            context.delete(item)
                        }
                    }
                }
            }
        )
    }
}
