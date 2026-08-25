import DatabaseKit

package struct RDFDatasetReadResolution {
    package let entity: Schema.Entity
    package let indexDescriptor: IndexDescriptor
    package let metadata: RDFDatasetIndexMetadata
}

package enum RDFDatasetReadResolver {
    /// Resolves an optional schema-wide projection source.
    ///
    /// No declared dataset means the authoritative RDF store remains the only
    /// source. Multiple declarations are never equivalent to no declaration.
    package static func resolveOptional(
        schema: Schema
    ) throws -> RDFDatasetReadResolution? {
        let candidates = try resolutions(in: schema)
        switch candidates.count {
        case 0:
            return nil
        case 1:
            return candidates[0]
        default:
            throw RDFDatasetReadResolutionError.ambiguous(
                candidates: candidateNames(candidates)
            )
        }
    }

    /// Resolves the dataset explicitly selected by an entity-bound SQL
    /// SPARQL function.
    package static func resolveRequired(
        entity: Schema.Entity
    ) throws -> RDFDatasetReadResolution {
        let candidates = try resolutions(for: entity)
        switch candidates.count {
        case 0:
            throw RDFDatasetReadResolutionError.missing(
                entityName: entity.name
            )
        case 1:
            return candidates[0]
        default:
            throw RDFDatasetReadResolutionError.ambiguous(
                candidates: candidateNames(candidates)
            )
        }
    }

    private static func resolutions(
        in schema: Schema
    ) throws -> [RDFDatasetReadResolution] {
        var result: [RDFDatasetReadResolution] = []
        for entity in schema.entities {
            result.append(contentsOf: try resolutions(for: entity))
        }
        return result
    }

    private static func resolutions(
        for entity: Schema.Entity
    ) throws -> [RDFDatasetReadResolution] {
        var result: [RDFDatasetReadResolution] = []
        for descriptor in entity.indexDescriptors {
            guard let selection = try RDFDatasetIndexSelection(
                descriptor: descriptor
            ) else {
                continue
            }
            result.append(RDFDatasetReadResolution(
                entity: entity,
                indexDescriptor: descriptor,
                metadata: selection.metadata
            ))
        }
        return result
    }

    private static func candidateNames(
        _ candidates: [RDFDatasetReadResolution]
    ) -> [String] {
        candidates.map {
            "\($0.entity.name):\($0.indexDescriptor.name)"
        }.sorted()
    }
}
