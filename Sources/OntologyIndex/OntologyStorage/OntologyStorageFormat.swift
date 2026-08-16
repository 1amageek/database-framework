import DatabaseEngine
import DatabaseTypes

/// Versioned, bounded binary storage owned by the ontology index.
///
/// The framework-owned StorageFrame encoding writes directly into
/// one exact-size `ByteString`; decoding borrows the retained storage frame.
enum OntologyStorageFormat {
    private static let metadataMagic: UInt32 = 0x314D_544F
    private static let classMagic: UInt32 = 0x3143_544F
    private static let propertyMagic: UInt32 = 0x3150_544F
    private static let stringListMagic: UInt32 = 0x314C_544F

    static func encode(
        _ metadata: OntologyMetadata,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> ByteString {
        let prefixKeys = metadata.prefixes.keys.sorted()
        return try StorageFrameEncoder.encode(limits: limits) {
            (writer: inout StorageFrameEncoder) throws(StorageFrameError) in
            writer.writeUInt32(metadataMagic)
            try writer.writeString(metadata.iri)
            try writer.writeOptionalString(metadata.versionIRI)
            try write(metadata.schemaVersion, into: &writer)
            write(metadata.createdAt, into: &writer)
            write(metadata.updatedAt, into: &writer)
            try write(metadata.imports, into: &writer)
            try write(
                metadata.prefixes,
                orderedKeys: prefixKeys,
                into: &writer
            )
            try write(metadata.statistics, into: &writer)
        }
    }

    static func decodeMetadata(
        _ bytes: ByteString,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> OntologyMetadata {
        var reader = try StorageFrameDecoder(bytes, limits: limits)
        guard try reader.readUInt32() == metadataMagic else {
            throw .invalidMagic
        }
        let metadata = OntologyMetadata(
            iri: try reader.readString(),
            versionIRI: try reader.readOptionalString(),
            schemaVersion: try readSchemaVersion(from: &reader),
            createdAt: try readTimestamp(from: &reader),
            updatedAt: try readTimestamp(from: &reader),
            imports: try readStrings(from: &reader),
            prefixes: try readStringDictionary(from: &reader),
            statistics: try readStatistics(from: &reader)
        )
        try reader.ensureFullyRead()
        return metadata
    }

    static func encode(
        _ definition: StoredClassDefinition,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> ByteString {
        let directSuperClasses = definition.directSuperClasses.sorted()
        let equivalentClasses = definition.equivalentClasses.sorted()
        let disjointClasses = definition.disjointClasses.sorted()
        let annotationKeys = definition.annotations.keys.sorted()
        return try StorageFrameEncoder.encode(limits: limits) {
            (writer: inout StorageFrameEncoder) throws(StorageFrameError) in
            writer.writeUInt32(classMagic)
            try writer.writeString(definition.iri)
            try writer.writeOptionalString(definition.label)
            try writer.writeOptionalString(definition.comment)
            try write(directSuperClasses, into: &writer)
            try write(equivalentClasses, into: &writer)
            try write(disjointClasses, into: &writer)
            writer.writeBool(definition.isPrimitive)
            writer.writeBool(definition.isDeprecated)
            try write(
                definition.annotations,
                orderedKeys: annotationKeys,
                into: &writer
            )
        }
    }

    static func decodeClass(
        _ bytes: ByteString,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> StoredClassDefinition {
        var reader = try StorageFrameDecoder(bytes, limits: limits)
        guard try reader.readUInt32() == classMagic else {
            throw .invalidMagic
        }
        let definition = StoredClassDefinition(
            iri: try reader.readString(),
            label: try reader.readOptionalString(),
            comment: try reader.readOptionalString(),
            directSuperClasses: try readStringSet(from: &reader),
            equivalentClasses: try readStringSet(from: &reader),
            disjointClasses: try readStringSet(from: &reader),
            isPrimitive: try reader.readBool(),
            isDeprecated: try reader.readBool(),
            annotations: try readStringDictionary(from: &reader)
        )
        try reader.ensureFullyRead()
        return definition
    }

    static func encode(
        _ definition: StoredPropertyDefinition,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> ByteString {
        let domains = definition.domains.sorted()
        let ranges = definition.ranges.sorted()
        let directSuperProperties = definition.directSuperProperties.sorted()
        let equivalentProperties = definition.equivalentProperties.sorted()
        let disjointProperties = definition.disjointProperties.sorted()
        let annotationKeys = definition.annotations.keys.sorted()
        return try StorageFrameEncoder.encode(limits: limits) {
            (writer: inout StorageFrameEncoder) throws(StorageFrameError) in
            writer.writeUInt32(propertyMagic)
            try writer.writeString(definition.iri)
            writer.writeUInt8(tag(for: definition.type))
            try writer.writeOptionalString(definition.label)
            try writer.writeOptionalString(definition.comment)
            try write(domains, into: &writer)
            try write(ranges, into: &writer)
            try write(directSuperProperties, into: &writer)
            try write(equivalentProperties, into: &writer)
            try write(disjointProperties, into: &writer)
            writer.writeUInt8(characteristicBits(for: definition))
            try writer.writeOptionalString(definition.inverseOf)
            try write(definition.propertyChains, into: &writer)
            writer.writeBool(definition.isDeprecated)
            try write(
                definition.annotations,
                orderedKeys: annotationKeys,
                into: &writer
            )
        }
    }

    static func decodeProperty(
        _ bytes: ByteString,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> StoredPropertyDefinition {
        var reader = try StorageFrameDecoder(bytes, limits: limits)
        guard try reader.readUInt32() == propertyMagic else {
            throw .invalidMagic
        }
        let iri = try reader.readString()
        let type = try propertyType(for: reader.readUInt8())
        let label = try reader.readOptionalString()
        let comment = try reader.readOptionalString()
        let domains = try readStringSet(from: &reader)
        let ranges = try readStringSet(from: &reader)
        let directSuperProperties = try readStringSet(from: &reader)
        let equivalentProperties = try readStringSet(from: &reader)
        let disjointProperties = try readStringSet(from: &reader)
        let characteristics = try reader.readUInt8()
        guard characteristics & 0x80 == 0 else {
            throw .invalidValue
        }
        let definition = StoredPropertyDefinition(
            iri: iri,
            type: type,
            label: label,
            comment: comment,
            domains: domains,
            ranges: ranges,
            directSuperProperties: directSuperProperties,
            equivalentProperties: equivalentProperties,
            disjointProperties: disjointProperties,
            isFunctional: characteristics & (1 << 0) != 0,
            isInverseFunctional: characteristics & (1 << 1) != 0,
            isTransitive: characteristics & (1 << 2) != 0,
            isSymmetric: characteristics & (1 << 3) != 0,
            isAsymmetric: characteristics & (1 << 4) != 0,
            isReflexive: characteristics & (1 << 5) != 0,
            isIrreflexive: characteristics & (1 << 6) != 0,
            inverseOf: try reader.readOptionalString(),
            propertyChains: try readStringLists(from: &reader),
            isDeprecated: try reader.readBool(),
            annotations: try readStringDictionary(from: &reader)
        )
        try reader.ensureFullyRead()
        return definition
    }

    static func encode(
        _ strings: [String],
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> ByteString {
        try StorageFrameEncoder.encode(limits: limits) {
            (writer: inout StorageFrameEncoder) throws(StorageFrameError) in
            writer.writeUInt32(stringListMagic)
            try write(strings, into: &writer)
        }
    }

    static func decodeStrings(
        _ bytes: ByteString,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> [String] {
        var reader = try StorageFrameDecoder(bytes, limits: limits)
        guard try reader.readUInt32() == stringListMagic else {
            throw .invalidMagic
        }
        let strings = try readStrings(from: &reader)
        try reader.ensureFullyRead()
        return strings
    }

    private static func write(
        _ version: SchemaVersion,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        guard let major = Int64(exactly: version.major),
              let minor = Int64(exactly: version.minor),
              let patch = Int64(exactly: version.patch) else {
            throw .integerOutOfRange
        }
        writer.writeInt64(major)
        writer.writeInt64(minor)
        writer.writeInt64(patch)
    }

    private static func readSchemaVersion(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> SchemaVersion {
        guard let major = Int(exactly: try reader.readInt64()),
              let minor = Int(exactly: try reader.readInt64()),
              let patch = Int(exactly: try reader.readInt64()) else {
            throw .integerOutOfRange
        }
        return SchemaVersion(major: major, minor: minor, patch: patch)
    }

    private static func write(
        _ timestamp: Timestamp,
        into writer: inout StorageFrameEncoder
    ) {
        writer.writeInt64(timestamp.secondsSinceUnixEpoch)
        writer.writeUInt32(timestamp.nanoseconds)
    }

    private static func readTimestamp(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> Timestamp {
        let seconds = try reader.readInt64()
        let nanoseconds = try reader.readUInt32()
        do {
            return try Timestamp(
                secondsSinceUnixEpoch: seconds,
                nanoseconds: nanoseconds
            )
        } catch {
            throw .invalidTimestamp
        }
    }

    private static func write(
        _ statistics: OntologyStatistics,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        for value in [
            statistics.classCount,
            statistics.propertyCount,
            statistics.axiomCount,
            statistics.classHierarchySize,
            statistics.propertyHierarchySize,
            statistics.transitivePropertyCount,
            statistics.propertyChainCount,
            statistics.sameAsEquivalenceClassCount,
        ] {
            guard let value = Int64(exactly: value) else {
                throw .integerOutOfRange
            }
            writer.writeInt64(value)
        }
    }

    private static func readStatistics(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> OntologyStatistics {
        var values: [Int] = []
        values.reserveCapacity(8)
        while values.count < 8 {
            guard let value = Int(exactly: try reader.readInt64()) else {
                throw .integerOutOfRange
            }
            values.append(value)
        }
        return OntologyStatistics(
            classCount: values[0],
            propertyCount: values[1],
            axiomCount: values[2],
            classHierarchySize: values[3],
            propertyHierarchySize: values[4],
            transitivePropertyCount: values[5],
            propertyChainCount: values[6],
            sameAsEquivalenceClassCount: values[7]
        )
    }

    private static func write(
        _ strings: [String],
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeCount(strings.count)
        var index = 0
        while index < strings.count {
            try writer.writeString(strings[index])
            index += 1
        }
    }

    private static func readStrings(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> [String] {
        let count = try reader.readCount()
        var strings: [String] = []
        strings.reserveCapacity(count)
        var index = 0
        while index < count {
            strings.append(try reader.readString())
            index += 1
        }
        return strings
    }

    private static func readStringSet(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> Set<String> {
        let strings = try readStrings(from: &reader)
        var result = Set<String>()
        result.reserveCapacity(strings.count)
        var index = 0
        while index < strings.count {
            guard result.insert(strings[index]).inserted else {
                throw .invalidValue
            }
            index += 1
        }
        return result
    }

    private static func write(
        _ dictionary: [String: String],
        orderedKeys: [String],
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeCount(orderedKeys.count)
        var index = 0
        while index < orderedKeys.count {
            let key = orderedKeys[index]
            guard let value = dictionary[key] else { throw .invalidValue }
            try writer.writeString(key)
            try writer.writeString(value)
            index += 1
        }
    }

    private static func readStringDictionary(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> [String: String] {
        let count = try reader.readCount()
        var result: [String: String] = [:]
        result.reserveCapacity(count)
        var previousKey: String?
        var index = 0
        while index < count {
            let key = try reader.readString()
            guard previousKey == nil || previousKey! < key else {
                throw .invalidValue
            }
            result[key] = try reader.readString()
            previousKey = key
            index += 1
        }
        return result
    }

    private static func write(
        _ lists: [[String]],
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeCount(lists.count)
        var index = 0
        while index < lists.count {
            try writer.writeLengthPrefixed {
                (child: inout StorageFrameEncoder) throws(StorageFrameError) in
                try write(lists[index], into: &child)
            }
            index += 1
        }
    }

    private static func readStringLists(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> [[String]] {
        let count = try reader.readCount()
        var lists: [[String]] = []
        lists.reserveCapacity(count)
        var index = 0
        while index < count {
            lists.append(
                try reader.readLengthPrefixed {
                    (child: inout StorageFrameDecoder) throws(StorageFrameError) in
                    try readStrings(from: &child)
                }
            )
            index += 1
        }
        return lists
    }

    private static func tag(for type: StoredPropertyType) -> UInt8 {
        switch type {
        case .objectProperty: 1
        case .dataProperty: 2
        case .annotationProperty: 3
        }
    }

    private static func propertyType(
        for tag: UInt8
    ) throws(StorageFrameError) -> StoredPropertyType {
        switch tag {
        case 1: .objectProperty
        case 2: .dataProperty
        case 3: .annotationProperty
        default: throw .invalidValueTag(tag)
        }
    }

    private static func characteristicBits(
        for definition: StoredPropertyDefinition
    ) -> UInt8 {
        (definition.isFunctional ? 1 << 0 : 0)
            | (definition.isInverseFunctional ? 1 << 1 : 0)
            | (definition.isTransitive ? 1 << 2 : 0)
            | (definition.isSymmetric ? 1 << 3 : 0)
            | (definition.isAsymmetric ? 1 << 4 : 0)
            | (definition.isReflexive ? 1 << 5 : 0)
            | (definition.isIrreflexive ? 1 << 6 : 0)
    }
}
