import DatabaseEngine

public enum SPARQLReadExecutors {
    public static func sourceExecutor(
        functionRegistry: SPARQLFunctionRegistry
    ) -> any SPARQLSourceExecutor {
        RuntimeSPARQLSourceExecutor(
            functionRegistry: functionRegistry
        )
    }
}
