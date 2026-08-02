import DatabaseEngine
import DatabaseTypes

/// The vector module's execution policy after decoding canonical runtime options.
///
/// Hosting adapters use the same resolver as index construction so capability
/// admission cannot diverge from the algorithm the framework will execute.
public struct VectorRuntimePolicy: Sendable {
    public let algorithm: VectorAlgorithm
    public let subspaceKey: String?

    public static func resolve(
        in configurations: [any IndexRuntimeConfiguration]
    ) throws -> VectorRuntimePolicy? {
        guard let configuration = configurations.first(where: {
            $0.kindIdentifier == VectorIndexSpecification.identifier
        }) else {
            return nil
        }

        let options = try configuration.executionOptions
        guard let algorithm = options["algorithm"]?.stringValue else {
            throw VectorIndexError.invalidArgument(
                "Vector execution policy requires an algorithm"
            )
        }

        let resolvedAlgorithm: VectorAlgorithm
        switch algorithm {
        case "flat":
            resolvedAlgorithm = .flat
        case "hnsw":
            resolvedAlgorithm = .hnsw(VectorHNSWParameters(
                m: try positiveInteger("m", in: options),
                efConstruction: try positiveInteger(
                    "efConstruction",
                    in: options
                ),
                efSearch: try positiveInteger("efSearch", in: options)
            ))
        case "ivf":
            let nlist = try positiveInteger("nlist", in: options)
            let nprobe = try positiveInteger("nprobe", in: options)
            guard nprobe <= nlist else {
                throw VectorIndexError.invalidArgument(
                    "Vector IVF nprobe cannot exceed nlist"
                )
            }
            resolvedAlgorithm = .ivf(try VectorIVFParameters(
                nlist: nlist,
                nprobe: nprobe,
                kmeansIterations: try positiveInteger(
                    "kmeansIterations",
                    in: options
                )
            ))
        case "pq":
            resolvedAlgorithm = .pq(try VectorPQParameters(
                m: try positiveInteger("m", in: options),
                niter: try positiveInteger("niter", in: options)
            ))
        default:
            throw VectorIndexError.invalidArgument(
                "Unknown vector algorithm policy"
            )
        }

        return VectorRuntimePolicy(
            algorithm: resolvedAlgorithm,
            subspaceKey: configuration.subspaceKey
        )
    }

    /// Resolves the effective policy for every configured vector index.
    ///
    /// Grouping and selection intentionally match the production maintainer
    /// and reader path: configurations are grouped by canonical index name and
    /// the first vector configuration in each group selects the algorithm.
    public static func resolveConfiguredIndexes(
        in configurations: [any IndexRuntimeConfiguration]
    ) throws -> [String: VectorRuntimePolicy] {
        var configurationsByIndex: [
            String: [any IndexRuntimeConfiguration]
        ] = [:]
        for configuration in configurations where
            configuration.kindIdentifier == VectorIndexSpecification.identifier {
            configurationsByIndex[configuration.indexName, default: []]
                .append(configuration)
        }

        var policies: [String: VectorRuntimePolicy] = [:]
        policies.reserveCapacity(configurationsByIndex.count)
        for (indexName, matchingConfigurations) in configurationsByIndex {
            if let policy = try resolve(in: matchingConfigurations) {
                policies[indexName] = policy
            }
        }
        return policies
    }

    private static func positiveInteger(
        _ name: String,
        in options: FieldObject
    ) throws -> Int {
        guard let value = options[name]?.int64Value,
              value > 0,
              let result = Int(exactly: value) else {
            throw VectorIndexError.invalidArgument(
                "Vector execution policy has an invalid positive integer"
            )
        }
        return result
    }
}
