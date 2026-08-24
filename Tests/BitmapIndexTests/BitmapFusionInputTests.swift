import DatabaseKit
import Testing

@testable import BitmapIndex

@Persistable
private struct BitmapFusionInputItem {
    var id: String
    var status: String
}

@Suite("Bitmap Fusion input")
struct BitmapFusionInputTests {
    @Test("Bitmap lowers eligibility without claiming score ownership")
    func lowersToCanonicalInput() throws {
        let input = try Bitmap(
            BitmapFusionInputItem.fields.status,
            in: ["active", "pending"]
        )
        .index(named: "status_bitmap")
        .limit(8)
        .fusionInput

        #expect(input.scoring == nil)
        #expect(input.requirement == .unrestricted)
        #expect(input.limit == 8)
        guard case .index(let source) = input.operation else {
            Issue.record("Bitmap must lower to an index operation")
            return
        }
        #expect(source.selection == .named(
            name: "status_bitmap",
            type: .bitmap
        ))
        #expect(source.referencedFields == [
            BitmapFusionInputItem.fields.status.identity,
        ])
        #expect(source.parameters[BitmapReadParameter.fieldName] == .string(
            "status"
        ))
        #expect(source.parameters[BitmapReadParameter.operation] == .string(
            BitmapReadParameter.inOperation
        ))
        #expect(source.parameters[BitmapReadParameter.values] == .array([
            .string("active"),
            .string("pending"),
        ]))
    }
}
