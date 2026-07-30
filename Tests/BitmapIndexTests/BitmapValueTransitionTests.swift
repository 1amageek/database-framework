import Testing
import DatabaseKit
import DatabaseTypes
import StorageKit
@testable import DatabaseEngine
@testable import BitmapIndex

@Persistable
private struct BitmapTransitionItem {
    var id: String
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
