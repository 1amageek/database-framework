#if os(macOS) && SQLITE
import AccessPerformanceProbe
import Database
import DatabaseAccess
import DatabaseClient
import DatabaseEmbedded
import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import DatabaseWire
import Foundation
import SQLiteStorage
import StorageKit
import StorageKitSystemClock

@main
struct AccessPerformanceContract {
    enum Failure: Error { case invalidOutput, observationUnavailable, invalidClock, authorizationBypassed }
    enum Output: Sendable {
        case local(QueryRowPage)
        case remote(QueryRowStream<PerformanceTransport>)
        case named(RowStream)
        func consume() async throws -> (Int, Int64, Bool) {
            var count = 0, sum: Int64 = 0, valid = true
            func accumulate(_ row: QueryRow) {
                count += 1
                let number = value(row)
                valid = valid && row.values.count == 1 && number == Int64(count)
                sum += number
            }
            switch self {
            case .local(let page):
                var rows = page.makeRowIterator()
                while let row = try rows.next() { accumulate(row) }
            case .remote(let rows):
                for try await row in rows { accumulate(row) }
            case .named(let rows):
                for try await row in rows { accumulate(row) }
            }
            return (count, sum, valid)
        }
        private func value(_ row: QueryRow) -> Int64 {
            if let value = row.values.first, case .primitive(.int32(let number)) = value { return Int64(number) }
            return 0
        }
    }
    struct BenchmarkWallClock: WallClock {
        var now: Timestamp { Timestamp(secondsSinceUnixEpoch: Int64(Date().timeIntervalSince1970)) }
    }
    typealias Producer = @Sendable () async throws -> Output

    static func main() async {
        do {
            let observing = CommandLine.arguments.contains("--observe")
            let quick = CommandLine.arguments.contains("--quick")
            let rounds = quick ? 1 : 5
            let initial = access_performance_observe()
            guard initial.valid == 1 else { throw Failure.observationUnavailable }
            if observing {
                let calibration = access_performance_calibrate()
                guard initial.observer_present == 1, calibration.valid == 1 else { throw Failure.observationUnavailable }
                emit(["kind": "calibration", "iterations": calibration.iterations,
                    "enabledNS": calibration.enabled_ns, "disabledNS": calibration.disabled_ns])
            }
            let schema = try Schema(entities: [])
            let runtime = try DatabaseFrameworkRuntime.configuration(executionIdentity: .init(identifier: "access-performance", revision: 1), schema: schema,
                schemaDrivenAuthorizationPolicyFactory: { entity in AuthorizationPolicyHandler(schemaDriven: entity) { _, _ in true } },
                sqlAuthorizationPolicyFactory: { entity, namespace, name in
                    guard namespace.name == "public", name == "access_items" else { return nil }
                    return AuthorizationPolicyHandler(schemaDriven: entity) { _, context in context.principal?.roles.contains("reader") == true }
                })
            let container = try await DBContainer.open(for: schema,
                configuration: DBConfiguration(storageEngine: SQLiteStorageEngine(configuration: .inMemory),
                    monotonicClock: SystemStorageClock(), wallClock: BenchmarkWallClock()),
                runtimeConfiguration: runtime, security: .enabled())
            defer { await container.shutdown() }
            let context = container.newContext(authorization: .authenticated(.init(identifier: "benchmark", roles: ["admin", "reader"])))
            let setup = try await context.openConnection(maximumConcurrentExecutions: 8)
            defer { await setup.close() }
            _ = try await setup.execute("CREATE TABLE access_items(n integer)")
            for start in stride(from: 1, through: 10000, by: 100) {
                let values = (start..<(start + 100)).map { "(\($0))" }.joined(separator: ",")
                _ = try await setup.execute(SQL(text: "INSERT INTO access_items VALUES " + values))
            }
            let denied = try await container.newContext(authorization: .authenticated(.init(identifier: "denied", roles: []))).openConnection()
            defer { await denied.close() }
            var wasDenied = false
            do { _ = try await denied.execute("SELECT n FROM access_items LIMIT 1") }
            catch let failure as SQLExecutionFailure {
                guard failure.diagnostic.sqlState == "42501" else { throw failure }
                wasDenied = true
            }
            guard wasDenied else { throw Failure.authorizationBypassed }
            var records = 0
            for count in quick ? [100] : [1, 100, 10000] {
                let query = TableRef("access_items").select("n").orderBy("n").limit(UInt64(count))
                let budget = ExecutionBudget(maximumRows: UInt32(count), maximumWorkUnits: 10_000_000,
                    maximumIntermediateRows: 100000, maximumIntermediateBytes: 128 * 1024 * 1024, timeoutMilliseconds: 60000)
                let client = DatabaseClient(transport: try PerformanceTransport(count: count, input: .statement(.select(query)), budget: budget))
                let remote = try await client.openConnection(maximumConcurrentExecutions: 8)
                let local = try await context.openConnection(maximumConcurrentExecutions: 8)
                defer { await remote.close(); await local.close() }
                let directory = DatabaseDirectory()
                try await directory.configure(databases: [
                    "remote": .borrowingRemote(session: client.database, maximumConcurrentExecutions: 8),
                    "local": .borrowingEmbedded(context: context, maximumConcurrentExecutions: 8)])
                defer { await directory.shutdown() }
                let options = QueryExecutionOptions(pageSize: 256, budget: budget)
                for backend in ["remote", "local"] {
                    let named = try directory.database(backend)
                    let direct: Producer = {
                        if backend == "remote" {
                            guard case .rows(let rows) = try await remote.execute(.select(query), options: options) else { throw Failure.invalidOutput }
                            return .remote(rows)
                        }
                        guard case .rows(let rows) = try await local.execute(.select(query), options: options) else { throw Failure.invalidOutput }
                        return .local(rows)
                    }
                    let facade: Producer = { .named(try await named.execute(query, options: options).rows) }
                    for concurrency in quick ? [1] : [1, 4, 8] {
                        if count == (quick ? 100 : 1) {
                            try await construction(directory: directory, database: named, alias: backend,
                                count: 0, concurrency: concurrency, phase: "lookup", observing: observing, rounds: rounds)
                            records += 2
                        }
                        try await construction(directory: directory, database: named, alias: backend,
                            count: count, concurrency: concurrency, phase: "construction", observing: observing, rounds: rounds)
                        records += 2
                        for round in 0...rounds {
                            let order = round.isMultiple(of: 2) ? [false, true] : [true, false]
                            for useFacade in order {
                                let result = try await execute(useFacade ? facade : direct, count: count,
                                    concurrency: concurrency, observing: observing)
                                if round > 0 {
                                    emit(result.merging(["kind": "execution", "backend": backend,
                                        "path": useFacade ? "named" : "direct", "rows": count,
                                        "concurrency": concurrency, "round": round, "observing": observing]) { _, new in new })
                                }
                            }
                        }
                        records += 2
                    }
                }
            }
            emit(["kind": "complete", "scenarios": records, "rounds": rounds, "observing": observing, "quick": quick])
            print("ACCESS_PERFORMANCE_PASS")
        } catch {
            print("ACCESS_PERFORMANCE_FAILURE \(error)")
            exit(1)
        }
    }

    static func execute(_ produce: @escaping Producer, count: Int, concurrency: Int, observing: Bool) async throws -> [String: Any] {
        let before = access_performance_observe()
        var perCall: [UInt64] = []
        var total = 0, checksum: Int64 = 0, valid = true
        let start = access_performance_now()
        var retained = before
        if observing {
            let outputs = try await withThrowingTaskGroup(of: Output.self) { group in
                for _ in 0..<concurrency { group.addTask { try await produce() } }
                var result: [Output] = []
                for try await output in group { result.append(output) }
                return result
            }
            retained = access_performance_observe()
            for output in outputs {
                let value = try await output.consume(); total += value.0; checksum += value.1; valid = valid && value.2
            }
            withExtendedLifetime(outputs) {}
        } else {
            let values = try await withThrowingTaskGroup(of: (UInt64, Int, Int64, Bool).self) { group in
                for _ in 0..<concurrency {
                    group.addTask {
                        let callStart = access_performance_now()
                        let value = try await produce().consume()
                        return (access_performance_now() - callStart, value.0, value.1, value.2)
                    }
                }
                var result: [(UInt64, Int, Int64, Bool)] = []
                for try await value in group { result.append(value) }
                return result
            }
            for value in values { perCall.append(value.0); total += value.1; checksum += value.2; valid = valid && value.3 }
        }
        let end = access_performance_now()
        let after = access_performance_observe()
        guard end > start, before.valid == 1, after.valid == 1, retained.valid == 1,
            valid, total == count * concurrency, checksum == Int64(count * (count + 1) / 2 * concurrency) else { throw Failure.invalidOutput }
        var result: [String: Any] = ["wallNS": end - start, "callNS": perCall,
            "rowsConsumed": total, "checksum": checksum,
            "queriesPerSecond": Double(concurrency) * 1e9 / Double(end - start),
            "rowsPerSecond": Double(total) * 1e9 / Double(end - start)]
        if observing {
            guard before.observer_present == 1, after.observer_present == 1 else { throw Failure.observationUnavailable }
            result.merge(["allocationCalls": after.allocation_calls - before.allocation_calls,
                "requestedBytes": after.requested_bytes - before.requested_bytes,
                "baselineHeapBytes": before.retained_bytes, "retainedHeapBytes": retained.retained_bytes,
                "retainedHeapDelta": Int64(retained.retained_bytes) - Int64(before.retained_bytes),
                "baselineBlocks": before.retained_blocks, "retainedBlocks": retained.retained_blocks,
                "heapHighWaterBytes": after.peak_retained_bytes == 0 ? NSNull() : NSNumber(value: after.peak_retained_bytes),
                "allocatorReservedBytes": after.reserved_bytes,
                "processPeakRSSBytes": after.peak_resident_bytes]) { _, new in new }
        }
        return result
    }

    enum Built: Sendable {
        case handles([DatabaseAccess.Database], UInt64)
        case queries([SelectQuery], UInt64)
        case failure
    }

    static func construction(directory: DatabaseDirectory, database: DatabaseAccess.Database, alias: String,
        count: Int, concurrency: Int, phase: String, observing: Bool, rounds: Int) async throws {
        let iterations = 1000
        for round in 0...rounds {
            for named in round.isMultiple(of: 2) ? [false, true] : [true, false] {
                let before = access_performance_observe(), start = access_performance_now()
                let constructed = await withTaskGroup(of: Built.self) { group in
                    for _ in 0..<concurrency {
                        group.addTask {
                            let workerStart = access_performance_now()
                            if phase == "lookup" {
                                var handles: [DatabaseAccess.Database] = []
                                handles.reserveCapacity(iterations)
                                for _ in 0..<iterations {
                                    // Lookup failure remains a real measurement failure.
                                    do { handles.append(named ? try directory.database(alias) : database) }
                                    catch { return .failure }
                                }
                                return .handles(handles, access_performance_now() - workerStart)
                            }
                            var queries: [SelectQuery] = []
                            queries.reserveCapacity(iterations)
                            for index in 0..<iterations {
                                queries.append(named
                                    ? database.entity("access_items").select("n").limit(UInt64(count)).offset(UInt64(index)).definition
                                    : TableRef("access_items").select("n").limit(UInt64(count)).offset(UInt64(index)))
                            }
                            return .queries(queries, access_performance_now() - workerStart)
                        }
                    }
                    var result: [Built] = []
                    for await value in group { result.append(value) }
                    return result
                }
                let end = access_performance_now(), after = access_performance_observe()
                guard end > start, before.valid == 1, after.valid == 1 else { throw Failure.invalidClock }
                var workerNS: [UInt64] = []
                for value in constructed {
                    switch value {
                    case .handles(let handles, let elapsed):
                        workerNS.append(elapsed)
                        guard handles.count == iterations, handles.allSatisfy({ $0 === database }) else { throw Failure.invalidOutput }
                    case .queries(let queries, let elapsed):
                        workerNS.append(elapsed)
                        guard queries.count == iterations else { throw Failure.invalidOutput }
                        for (index, query) in queries.enumerated() {
                            guard query == TableRef("access_items").select("n").limit(UInt64(count)).offset(UInt64(index)) else { throw Failure.invalidOutput }
                        }
                    case .failure: throw Failure.invalidOutput
                    }
                }
                withExtendedLifetime(constructed) {}
                if round > 0 {
                    var result: [String: Any] = ["kind": phase, "backend": alias, "path": named ? "named" : "direct",
                        "rows": count, "concurrency": concurrency, "round": round, "observing": observing,
                        "operations": iterations * concurrency, "workerNS": workerNS, "iterationsPerWorker": iterations, "wallNS": end - start,
                        "operationsPerSecond": Double(iterations * concurrency) * 1e9 / Double(end - start)]
                    if observing {
                        result["allocationCalls"] = after.allocation_calls - before.allocation_calls
                        result["requestedBytes"] = after.requested_bytes - before.requested_bytes
                        result["retainedHeapDelta"] = Int64(after.retained_bytes) - Int64(before.retained_bytes)
                        result["processPeakRSSBytes"] = after.peak_resident_bytes
                    }
                    emit(result)
                }
            }
        }
    }
    static func emit(_ value: [String: Any]) {
        do {
            let data = try JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])
            guard let line = String(data: data, encoding: .utf8) else { print("ACCESS_PERFORMANCE_FAILURE JSON"); exit(1) }
            print("ACCESS_PERFORMANCE \(line)")
        } catch { print("ACCESS_PERFORMANCE_FAILURE JSON"); exit(1) }
    }
}
#else
@main
struct UnavailableAccessPerformanceContract {
    enum Failure: Error { case requiresMacOSAndSQLite }
    static func main() throws { throw Failure.requiresMacOSAndSQLite }
}
#endif
