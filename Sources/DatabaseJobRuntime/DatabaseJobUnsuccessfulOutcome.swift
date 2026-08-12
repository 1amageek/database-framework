import DatabaseOperationCore
import DatabaseTypes
import DatabaseWire

public enum DatabaseJobUnsuccessfulOutcome:
    PersistentJobPayload,
    Sendable,
    Hashable
{
    case failed(RemoteOperationError)
    case cancelled

    public func persistentJobValue()
        throws(PersistentJobPayloadError) -> FieldValue
    {
        let fields: [(key: String, value: FieldValue)]
        switch self {
        case .failed(let error):
            fields = [
                (key: "kind", value: .string("failed")),
                (
                    key: "category",
                    value: .uint8(error.category.rawValue)
                ),
                (key: "code", value: .string(error.code)),
                (key: "message", value: .string(error.message)),
                (
                    key: "retryability",
                    value: .uint8(error.retryability.rawValue)
                ),
                (key: "details", value: .object(error.details)),
            ]
        case .cancelled:
            fields = [
                (key: "kind", value: .string("cancelled")),
            ]
        }
        do {
            return .object(try FieldObject(fields))
        } catch {
            throw .invalidValue("Job outcome fields are not unique")
        }
    }

    public init(
        persistentJobValue: FieldValue
    ) throws(PersistentJobPayloadError) {
        guard let fields = persistentJobValue.objectValue,
              let kind = fields["kind"]?.stringValue else {
            throw .invalidValue("Invalid job outcome")
        }
        switch kind {
        case "cancelled":
            self = .cancelled
        case "failed":
            guard let categoryValue = fields["category"]?.uint8Value,
                  let category = OperationErrorCategory(
                    rawValue: categoryValue
                  ),
                  let code = fields["code"]?.stringValue,
                  let message = fields["message"]?.stringValue,
                  let retryabilityValue =
                    fields["retryability"]?.uint8Value,
                  let retryability = OperationRetryability(
                    rawValue: retryabilityValue
                  ),
                  let details = fields["details"]?.objectValue else {
                throw .invalidValue("Invalid failed job outcome")
            }
            self = .failed(
                RemoteOperationError(
                    category: category,
                    code: code,
                    message: message,
                    retryability: retryability,
                    details: details
                )
            )
        default:
            throw .invalidValue("Unknown job outcome kind")
        }
    }
}
