#if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire

struct DatabaseOntologyPageCursor: DatabaseRuntimePayloadValue, Hashable {
    enum Kind: Sendable, Hashable {
        case reason(OntologyExecuteOperation.ReasoningProfile)
        case hierarchy(
            resource: String,
            resourceKind: OntologyExecuteOperation.HierarchyResourceKind,
            direction: OntologyExecuteOperation.HierarchyDirection,
            maximumDepth: UInt32
        )
        case validation
    }

    private static let formatVersion: UInt8 = 1

    let ontology: String
    let dependencyFingerprint: ByteString
    let offset: UInt64
    let kind: Kind

    func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.formatVersion)
        try writer.writeString(ontology)
        try writer.writeBytes(dependencyFingerprint)
        writer.writeUInt64(offset)
        switch kind {
        case .reason(let profile):
            writer.writeUInt8(1)
            writer.writeUInt8(profile.rawValue)
        case .hierarchy(
            let resource,
            let resourceKind,
            let direction,
            let maximumDepth
        ):
            writer.writeUInt8(2)
            try writer.writeString(resource)
            writer.writeUInt8(resourceKind.rawValue)
            writer.writeUInt8(direction.rawValue)
            writer.writeUInt32(maximumDepth)
        case .validation:
            writer.writeUInt8(3)
        }
    }

    init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        let version = try reader.readUInt8()
        guard version == Self.formatVersion else {
            throw .invalidValueTag(version)
        }
        let ontology = try reader.readString()
        let dependencyFingerprint = try reader.readBytes()
        let offset = try reader.readUInt64()
        let kind: Kind
        switch try reader.readUInt8() {
        case 1:
            let rawProfile = try reader.readUInt8()
            guard let profile = OntologyExecuteOperation.ReasoningProfile(
                rawValue: rawProfile
            ) else {
                throw .invalidValueTag(rawProfile)
            }
            kind = .reason(profile)
        case 2:
            let resource = try reader.readString()
            let rawResourceKind = try reader.readUInt8()
            guard let resourceKind = OntologyExecuteOperation.HierarchyResourceKind(
                rawValue: rawResourceKind
            ) else {
                throw .invalidValueTag(rawResourceKind)
            }
            let rawDirection = try reader.readUInt8()
            guard let direction = OntologyExecuteOperation.HierarchyDirection(
                rawValue: rawDirection
            ) else {
                throw .invalidValueTag(rawDirection)
            }
            kind = .hierarchy(
                resource: resource,
                resourceKind: resourceKind,
                direction: direction,
                maximumDepth: try reader.readUInt32()
            )
        case 3:
            kind = .validation
        case let tag:
            throw .invalidValueTag(tag)
        }
        self.ontology = ontology
        self.dependencyFingerprint = dependencyFingerprint
        self.offset = offset
        self.kind = kind
    }

    init(
        ontology: String,
        dependencyFingerprint: ByteString,
        offset: UInt64,
        kind: Kind
    ) {
        self.ontology = ontology
        self.dependencyFingerprint = dependencyFingerprint
        self.offset = offset
        self.kind = kind
    }
}

#endif
