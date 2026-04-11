import DatabaseEngine
import VectorIndex
import FullTextIndex
import RankIndex
import BitmapIndex
import VersionIndex
import PermutedIndex
import GraphIndex

/// Runtime composition point for built-in read executors.
///
/// `DatabaseEngine` owns only neutral contracts and registries. Concrete executor
/// wiring lives here so built-in features can be composed without teaching the
/// engine about individual index families.
public enum BuiltinReadRuntime {
    private static let registration: Void = {
        VectorReadBridge.registerReadExecutors()
        FullTextReadBridge.registerReadExecutors()
        RankReadBridge.registerReadExecutors()
        BitmapReadBridge.registerReadExecutors()
        VersionReadBridge.registerReadExecutors()
        PermutedReadBridge.registerReadExecutors()
        GraphTableReadBridge.registerReadExecutors()
        SPARQLReadBridge.registerReadExecutors()
    }()

    public static func registerBuiltins() {
        _ = registration
    }
}
