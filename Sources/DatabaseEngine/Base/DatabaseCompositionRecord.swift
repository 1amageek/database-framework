#if DATABASE_MULTIPLE_BASES
import DatabaseKit
import StorageKit

/// Durable control-domain definition of one named Base Composition.
@_spi(DatabaseExecution)
public struct DatabaseCompositionRecord: Sendable, Hashable, StorageFrameValue {
    public let composition: Base.Composition
    public let revision: UInt64
    public let generation: UInt64

    package init(
        composition: Base.Composition,
        revision: UInt64,
        generation: UInt64
    ) {
        self.composition = composition
        self.revision = revision
        self.generation = generation
    }

    package func encode(
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeString(composition.id.value)
        try encoder.writeCount(composition.bases.count)
        for baseID in composition.bases {
            try encoder.writeString(baseID.value)
        }
        encoder.writeUInt64(revision)
        encoder.writeUInt64(generation)
    }

    package init(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) {
        do {
            let id = try Base.Composition.ID(decoder.readString())
            let baseCount = try decoder.readCount()
            guard baseCount > 0 else {
                throw StorageFrameError.invalidValue
            }
            var bases: [Base.ID] = []
            bases.reserveCapacity(baseCount)
            for _ in 0..<baseCount {
                bases.append(try Base.ID(decoder.readString()))
            }
            self.composition = try Base.Composition(id: id, bases: bases)
        } catch {
            throw .invalidValue
        }
        self.revision = try decoder.readUInt64()
        self.generation = try decoder.readUInt64()
        guard revision > 0, generation > 0 else {
            throw .invalidValue
        }
    }
}

#endif
