import Testing
import Core
import StorageKit
@testable import DatabaseEngine
@testable import BitmapIndex

private struct BitmapTransitionItem: Persistable {
    typealias ID = String

    var id: String

    static var persistableType: String { "BitmapTransitionItem" }
    static var allFields: [String] { ["id"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        member == "id" ? id : nil
    }

    static func fieldName<Value>(for keyPath: KeyPath<BitmapTransitionItem, Value>) -> String {
        "id"
    }

    static func fieldName(for keyPath: PartialKeyPath<BitmapTransitionItem>) -> String {
        "id"
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        "id"
    }
}

@Suite("Bitmap value transition contract")
struct BitmapValueTransitionTests {
    @Test("nil to value adds bitmap membership")
    func nilToValueAddsMembership() {
        let transition = BitmapIndexMaintainer<BitmapTransitionItem>.valueTransition(
            oldValueKey: nil,
            newValueKey: [0x01]
        )

        #expect(transition == .add)
    }

    @Test("value to nil removes bitmap membership")
    func valueToNilRemovesMembership() {
        let transition = BitmapIndexMaintainer<BitmapTransitionItem>.valueTransition(
            oldValueKey: [0x01],
            newValueKey: nil
        )

        #expect(transition == .remove)
    }

    @Test("same value is unchanged and different value replaces membership")
    func valueChangesAreClassifiedExplicitly() {
        #expect(
            BitmapIndexMaintainer<BitmapTransitionItem>.valueTransition(
                oldValueKey: [0x01],
                newValueKey: [0x01]
            ) == .unchanged
        )
        #expect(
            BitmapIndexMaintainer<BitmapTransitionItem>.valueTransition(
                oldValueKey: [0x01],
                newValueKey: [0x02]
            ) == .replace
        )
    }
}
