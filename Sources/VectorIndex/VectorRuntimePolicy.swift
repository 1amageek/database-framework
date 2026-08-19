import DatabaseKit
import DatabaseEngine
import DatabaseTypes

/// The vector module's execution policy after decoding canonical runtime options.
///
/// Hosting adapters use the same resolver as index construction so capability
/// admission cannot diverge from the algorithm the framework will execute.
public struct VectorRuntimePolicy: Sendable {
    public let algorithm: VectorAlgorithm

    public var physicalLayout: IndexPhysicalLayout {
        get throws {
            switch algorithm {
            case .flat:
                return try IndexPhysicalLayout(
                    name: "vector.flat",
                    revision: 1
                )
            case .hnsw(let parameters):
                return try IndexPhysicalLayout(
                    name: "vector.hnsw",
                    revision: 1,
                    parameters: FieldObject([
                        ("efConstruction", .int64(Int64(parameters.efConstruction))),
                        ("m", .int64(Int64(parameters.m))),
                    ])
                )
            case .ivf(let parameters):
                return try IndexPhysicalLayout(
                    name: "vector.ivf",
                    revision: 1,
                    parameters: FieldObject([
                        ("kmeansIterations", .int64(Int64(parameters.kmeansIterations))),
                        ("nlist", .int64(Int64(parameters.nlist))),
                    ])
                )
            case .pq(let parameters):
                return try IndexPhysicalLayout(
                    name: "vector.pq",
                    revision: 1,
                    parameters: FieldObject([
                        ("ksub", .int64(256)),
                        ("m", .int64(Int64(parameters.m))),
                        ("niter", .int64(Int64(parameters.niter))),
                    ])
                )
            }
        }
    }

    public static func resolve(
        in configurations: [any IndexRuntimeConfiguration]
    ) throws -> VectorRuntimePolicy? {
        let matchingConfigurations = configurations.filter {
            $0.indexType == .vector
        }
        guard matchingConfigurations.count <= 1 else {
            throw IndexRuntimeConfigurationError.duplicateConfiguration(
                indexName: matchingConfigurations[0].indexName
            )
        }
        guard let configuration = matchingConfigurations.first else {
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
            try requireExactOptionKeys(
                ["algorithm"],
                in: options
            )
            resolvedAlgorithm = .flat
        case "hnsw":
            try requireExactOptionKeys(
                ["algorithm", "efConstruction", "efSearch", "m"],
                in: options
            )
            resolvedAlgorithm = .hnsw(VectorHNSWParameters(
                m: try positiveInteger("m", in: options),
                efConstruction: try positiveInteger(
                    "efConstruction",
                    in: options
                ),
                efSearch: try positiveInteger("efSearch", in: options)
            ))
        case "ivf":
            try requireExactOptionKeys(
                [
                    "algorithm",
                    "kmeansIterations",
                    "nlist",
                    "nprobe",
                ],
                in: options
            )
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
            try requireExactOptionKeys(
                ["algorithm", "m", "niter"],
                in: options
            )
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
            algorithm: resolvedAlgorithm)
    }

    /// Resolves the effective policy for every configured vector index.
    ///
    /// Grouping and selection intentionally match the production maintainer
    /// and reader path. A vector index has exactly one effective algorithm;
    /// duplicate configurations fail instead of depending on array order.
    public static func resolveConfiguredIndexes(
        in configurations: [any IndexRuntimeConfiguration]
    ) throws -> [String: VectorRuntimePolicy] {
        var configurationsByIndex: [
            String: [any IndexRuntimeConfiguration]
        ] = [:]
        for configuration in configurations where
            configuration.indexType == .vector
        {
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

    private static func requireExactOptionKeys(
        _ expected: Set<String>,
        in options: FieldObject
    ) throws {
        let actual = Set(options.fields.map { $0.key })
        guard actual == expected else {
            throw VectorIndexError.invalidArgument(
                "Vector execution policy contains missing or unknown options"
            )
        }
    }
}
