import DatabaseKit
import DatabaseTypes

struct DirectoryFieldBinding: Sendable {
    let field: FieldIdentity
    let value: FieldValue

    var name: String {
        field.name
    }
}
