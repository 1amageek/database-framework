#if !os(WASI)
#if FOUNDATION_DB
import Core
import DatabaseEngine
import DatabaseWire
import Testing

@Suite("Canonical record storage codec", .heartbeat)
struct DatabaseRecordStorageCodecTests {
    @Test("Typed references round-trip without Codable")
    func typedReferencesRoundTrip() throws {
        let first = try RelationshipReferenceFactory.make(
            RelationshipTarget.self,
            id: "target-1"
        )
        let second = try RelationshipReferenceFactory.make(
            RelationshipTarget.self,
            id: "target-2"
        )
        var owner = RelationshipArrayOwner(targets: [first, second])
        owner.id = "owner-1"

        let bytes = try DataAccess.serialize(owner)
        let decoded: RelationshipArrayOwner = try DataAccess.deserialize(bytes)

        #expect(Array(bytes.prefix(4)) == [0x44, 0x42, 0x52, 0x43])
        #expect(decoded.id == owner.id)
        #expect(decoded.targets == owner.targets)
    }

    @Test("Runtime type decoding preserves complete partition identity")
    func runtimeTypeDecoding() throws {
        let target = try RelationshipReferenceFactory.make(
            RelationshipPartitionedTarget.self,
            id: "target-1",
            partitions: [
                .init(number: 2, name: "tenantID", value: .string("tenant-1"))
            ]
        )
        var owner = RelationshipPartitionedOwner(target: target)
        owner.id = "owner-1"

        let bytes = try DatabaseRecordStorageCodec.encode(owner)
        let decoded: RelationshipPartitionedOwner = try DatabaseRecordStorageCodec.decode(
            RelationshipPartitionedOwner.self,
            from: bytes
        )

        #expect(decoded.id == owner.id)
        #expect(decoded.target.identity == target.identity)
    }

    @Test("Envelope validation rejects invalid magic and version")
    func rejectsInvalidEnvelope() throws {
        var invalidMagic = try DataAccess.serialize(
            RelationshipTarget(name: "target")
        )
        invalidMagic[0] = 0
        #expect(throws: DatabaseRecordFrameError.invalidMagic) {
            let _: RelationshipTarget = try DataAccess.deserialize(invalidMagic)
        }

        var invalidVersion = try DataAccess.serialize(
            RelationshipTarget(name: "target")
        )
        invalidVersion[4] = 2
        invalidVersion[5] = 0
        #expect(throws: DatabaseRecordFrameError.unsupportedVersion(2)) {
            let _: RelationshipTarget = try DataAccess.deserialize(invalidVersion)
        }
    }

    @Test("Envelope validation rejects entity mismatch and truncation")
    func rejectsMismatchedOrTruncatedRecords() throws {
        let bytes = try DataAccess.serialize(
            RelationshipTarget(name: "target")
        )
        #expect(throws: DatabaseRecordFrameError.self) {
            let _: RelationshipArrayOwner = try DataAccess.deserialize(bytes)
        }

        #expect(throws: DatabaseWireError.self) {
            let _: RelationshipTarget = try DataAccess.deserialize(bytes.dropLast())
        }
    }
}
#endif
#endif
