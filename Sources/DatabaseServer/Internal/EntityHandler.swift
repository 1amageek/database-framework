import Foundation
import Core
import DatabaseEngine
import DatabaseClientProtocol

/// Type-erased handler for entity operations.
struct EntityHandler: Sendable {
    let applyChanges: @Sendable (FDBContext, [ChangeSet.Change], [String: WritePrecondition]) async throws -> Void

    static func build<T: Persistable>(for type: T.Type) -> EntityHandler {
        let decoder = JSONDecoder()

        return EntityHandler(
            applyChanges: { context, changes, preconditions in
                for change in changes {
                    let precondition = preconditions[change.id] ?? .none
                    switch change.operation {
                    case .insert, .update:
                        guard let fields = change.fields else { continue }
                        let data = try plainJSONData(from: fields)
                        let item = try decoder.decode(T.self, from: data)
                        context.upsert(item, precondition: precondition)

                    case .delete:
                        if let fields = change.fields {
                            let data = try plainJSONData(from: fields)
                            let item = try decoder.decode(T.self, from: data)
                            context.delete(item, precondition: precondition)
                        } else {
                            let idDict: [String: FieldValue] = ["id": .string(change.id)]
                            let data = try plainJSONData(from: idDict)
                            let item = try decoder.decode(T.self, from: data)
                            context.delete(item, precondition: precondition)
                        }
                    }
                }
            }
        )
    }
}

private func plainJSONData(from fields: [String: FieldValue]) throws -> Data {
    let object = try fields.mapValues { try plainJSONValue(from: $0) }
    return try JSONSerialization.data(withJSONObject: object)
}

private func plainJSONValue(from value: FieldValue) throws -> Any {
    switch value {
    case .int64(let value):
        return value
    case .double(let value):
        return value
    case .string(let value):
        return value
    case .bool(let value):
        return value
    case .data(let value):
        return value.base64EncodedString()
    case .null:
        return NSNull()
    case .array(let values):
        return try values.map { try plainJSONValue(from: $0) }
    }
}
