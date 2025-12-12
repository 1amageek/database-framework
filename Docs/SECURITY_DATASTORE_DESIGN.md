# DataStore セキュリティ設計

## 概要

全てのデータアクセスを DataStore に集約し、DataStore 層でセキュリティ評価を行う設計。

## 現状の問題

```
現状のデータアクセス経路:

経路1: FDBContext → FDBDataStore → FoundationDB
経路2: FDBContext → TransactionContext → FoundationDB  ← DataStore をバイパス
```

- TransactionContext が DataStore を経由せずに直接 FoundationDB にアクセス
- セキュリティ評価が分散し、漏れのリスクがある
- DataStore プロトコルに withTransaction がない

## 新設計

```
┌─────────────────────────────────────────────────────────────────┐
│ FDBContext (Public API)                                          │
│                                                                  │
│  fetch().execute() ─┐                                            │
│  cursor().next()   ─┼─→ store.fetch(query, security)            │
│  enumerate()       ─┘                                            │
│  model(for:)       ───→ store.fetch(type, id, security)         │
│  save()            ───→ store.executeBatch(..., security)       │
│  withTransaction() ───→ store.withTransaction(security) { }     │
│  fetchPolymorphic()───→ store.fetchPolymorphic(..., security)   │
│  clearAll()        ───→ store.clearAll(type, security)          │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ DataStore (セキュリティ評価層)                                   │
│                                                                  │
│  fetch(query, security)         ← [LIST 評価]                   │
│  fetch(type, id, security)      ← [GET 評価]                    │
│  fetchAll(type, security)       ← [LIST 評価]                   │
│  fetchCount(query, security)    ← [LIST 評価]                   │
│  executeBatch(..., security)    ← [CREATE/UPDATE/DELETE 評価]   │
│  clearAll(type, security)       ← [ADMIN 専用]                  │
│                                                                  │
│  withTransaction(security) { context in                         │
│      context.get()    ← [GET 評価]                              │
│      context.set()    ← [CREATE/UPDATE 評価]                    │
│      context.delete() ← [DELETE 評価]                           │
│  }                                                               │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ FoundationDB                                                     │
└─────────────────────────────────────────────────────────────────┘
```

**全てのデータアクセスが DataStore を通過する。DataStore でセキュリティ評価すれば漏れがない。**

## 型定義

### SecurityContext

```swift
/// セキュリティ評価に必要な情報
///
/// DataStore の各メソッドに渡され、評価の要否と auth 情報を提供する。
public struct SecurityContext: Sendable {
    /// 認証コンテキスト（nil = 未認証）
    public let auth: (any AuthContext)?

    /// セキュリティ設定
    public let configuration: SecurityConfiguration

    /// セキュリティ評価が必要か
    ///
    /// false を返すケース:
    /// - セキュリティが無効
    /// - Admin ロールを持つ
    public var shouldEvaluate: Bool {
        guard configuration.isEnabled else { return false }
        guard let auth else { return true }  // 未認証は評価対象
        return auth.roles.isDisjoint(with: configuration.adminRoles)
    }

    /// セキュリティ無効のコンテキスト（テスト用）
    public static let disabled = SecurityContext(
        auth: nil,
        configuration: .disabled
    )

    public init(auth: (any AuthContext)?, configuration: SecurityConfiguration) {
        self.auth = auth
        self.configuration = configuration
    }
}
```

### TransactionContextProtocol

```swift
/// トランザクション操作のプロトコル
///
/// DataStore.withTransaction() から返されるコンテキストが準拠する。
/// セキュリティ評価は DataStore/TransactionContext の実装内で行われる。
public protocol TransactionContextProtocol: Sendable {

    /// 単一モデルを取得
    ///
    /// - Parameters:
    ///   - type: 取得する型
    ///   - id: ID
    ///   - snapshot: true = スナップショット読み取り（コンフリクトなし）
    /// - Returns: モデル（存在しない場合は nil）
    func get<T: Persistable>(
        _ type: T.Type,
        id: any TupleElement,
        snapshot: Bool
    ) async throws -> T?

    /// 複数モデルを取得
    func getMany<T: Persistable>(
        _ type: T.Type,
        ids: [any TupleElement],
        snapshot: Bool
    ) async throws -> [T]

    /// モデルを保存（insert or update）
    func set<T: Persistable>(_ model: T) async throws

    /// モデルを削除
    func delete<T: Persistable>(_ model: T) async throws

    /// ID 指定でモデルを削除
    func delete<T: Persistable>(_ type: T.Type, id: any TupleElement) async throws

    /// 生のトランザクションへのアクセス（上級者向け）
    var rawTransaction: any TransactionProtocol { get }
}
```

### DataStore プロトコル（拡張）

```swift
public protocol DataStore: AnyObject, Sendable {
    associatedtype Configuration: DataStoreConfiguration

    // MARK: - Fetch Operations

    /// クエリでモデルを取得
    func fetch<T: Persistable>(
        _ query: Query<T>,
        security: SecurityContext
    ) async throws -> [T]

    /// ID でモデルを取得
    func fetch<T: Persistable>(
        _ type: T.Type,
        id: any TupleElement,
        security: SecurityContext
    ) async throws -> T?

    /// 全モデルを取得
    func fetchAll<T: Persistable>(
        _ type: T.Type,
        security: SecurityContext
    ) async throws -> [T]

    /// クエリのカウント
    func fetchCount<T: Persistable>(
        _ query: Query<T>,
        security: SecurityContext
    ) async throws -> Int

    // MARK: - Write Operations

    /// バッチ実行（insert/delete）
    func executeBatch(
        inserts: [any Persistable],
        deletes: [any Persistable],
        security: SecurityContext
    ) async throws

    /// 全削除（Admin 専用）
    func clearAll<T: Persistable>(
        _ type: T.Type,
        security: SecurityContext
    ) async throws

    // MARK: - Transaction Operations

    /// トランザクション実行
    ///
    /// - Parameters:
    ///   - configuration: トランザクション設定
    ///   - security: セキュリティコンテキスト
    ///   - operation: トランザクション内で実行する操作
    /// - Returns: 操作の戻り値
    func withTransaction<T: Sendable>(
        configuration: TransactionConfiguration,
        security: SecurityContext,
        _ operation: @Sendable (any TransactionContextProtocol) async throws -> T
    ) async throws -> T
}
```

## FDBDataStore 実装

### セキュリティ評価ヘルパー

```swift
extension FDBDataStore {

    /// LIST セキュリティ評価
    private func evaluateListSecurity<T: Persistable>(
        type: T.Type,
        limit: Int?,
        offset: Int?,
        orderBy: [String]?,
        security: SecurityContext
    ) throws {
        guard security.shouldEvaluate else { return }
        guard let secureType = T.self as? any SecurityPolicy.Type else { return }

        let allowed = secureType._evaluateList(
            limit: limit,
            offset: offset,
            orderBy: orderBy,
            auth: security.auth
        )

        guard allowed else {
            throw SecurityError(
                operation: .list,
                targetType: T.persistableType,
                reason: "Access denied: list operation not allowed"
            )
        }
    }

    /// GET セキュリティ評価
    private func evaluateGetSecurity(
        _ resource: any Persistable,
        security: SecurityContext
    ) throws {
        guard security.shouldEvaluate else { return }
        let modelType = type(of: resource)
        guard let secureType = modelType as? any SecurityPolicy.Type else { return }

        let allowed = secureType._evaluateGet(resource: resource, auth: security.auth)

        guard allowed else {
            throw SecurityError(
                operation: .get,
                targetType: modelType.persistableType,
                reason: "Access denied: get operation not allowed"
            )
        }
    }

    /// CREATE セキュリティ評価
    private func evaluateCreateSecurity(
        _ resource: any Persistable,
        security: SecurityContext
    ) throws {
        guard security.shouldEvaluate else { return }
        let modelType = type(of: resource)
        guard let secureType = modelType as? any SecurityPolicy.Type else { return }

        let allowed = secureType._evaluateCreate(newResource: resource, auth: security.auth)

        guard allowed else {
            throw SecurityError(
                operation: .create,
                targetType: modelType.persistableType,
                reason: "Access denied: create operation not allowed"
            )
        }
    }

    /// UPDATE セキュリティ評価
    private func evaluateUpdateSecurity(
        _ resource: any Persistable,
        newResource: any Persistable,
        security: SecurityContext
    ) throws {
        guard security.shouldEvaluate else { return }
        let modelType = type(of: newResource)
        guard let secureType = modelType as? any SecurityPolicy.Type else { return }

        let allowed = secureType._evaluateUpdate(
            resource: resource,
            newResource: newResource,
            auth: security.auth
        )

        guard allowed else {
            throw SecurityError(
                operation: .update,
                targetType: modelType.persistableType,
                reason: "Access denied: update operation not allowed"
            )
        }
    }

    /// DELETE セキュリティ評価
    private func evaluateDeleteSecurity(
        _ resource: any Persistable,
        security: SecurityContext
    ) throws {
        guard security.shouldEvaluate else { return }
        let modelType = type(of: resource)
        guard let secureType = modelType as? any SecurityPolicy.Type else { return }

        let allowed = secureType._evaluateDelete(resource: resource, auth: security.auth)

        guard allowed else {
            throw SecurityError(
                operation: .delete,
                targetType: modelType.persistableType,
                reason: "Access denied: delete operation not allowed"
            )
        }
    }

    /// ADMIN 専用チェック
    private func requireAdmin(
        operation: String,
        targetType: String,
        security: SecurityContext
    ) throws {
        guard !security.shouldEvaluate else {
            throw SecurityError(
                operation: .delete,  // 最も近い操作
                targetType: targetType,
                reason: "\(operation) requires admin privileges"
            )
        }
    }
}
```

### fetch(query, security)

```swift
func fetch<T: Persistable>(
    _ query: Query<T>,
    security: SecurityContext
) async throws -> [T] {
    // セキュリティ評価
    let orderByFields = query.sortDescriptors.map { $0.fieldName }
    try evaluateListSecurity(
        type: T.self,
        limit: query.fetchLimit,
        offset: query.fetchOffset,
        orderBy: orderByFields.isEmpty ? nil : orderByFields,
        security: security
    )

    // 既存の実装...
    var results: [T]
    // ... インデックス使用 or フルスキャン
    return results
}
```

### fetch(type, id, security)

```swift
func fetch<T: Persistable>(
    _ type: T.Type,
    id: any TupleElement,
    security: SecurityContext
) async throws -> T? {
    // 既存のフェッチ
    let typeSubspace = itemSubspace.subspace(T.persistableType)
    let keyTuple = (id as? Tuple) ?? Tuple([id])
    let key = typeSubspace.pack(keyTuple)

    let result: T? = try await database.withTransaction { transaction in
        guard let bytes = try await transaction.getValue(for: key, snapshot: false) else {
            return nil
        }
        return try DataAccess.deserialize(bytes)
    }

    // セキュリティ評価（取得後）
    if let r = result {
        try evaluateGetSecurity(r, security: security)
    }

    return result
}
```

### executeBatch(inserts, deletes, security)

```swift
func executeBatch(
    inserts: [any Persistable],
    deletes: [any Persistable],
    security: SecurityContext
) async throws {
    let encoder = ProtobufEncoder()

    try await database.withTransaction { transaction in
        // Delete のセキュリティ評価
        for model in deletes {
            try self.evaluateDeleteSecurity(model, security: security)
        }

        // Insert/Update のセキュリティ評価
        for model in inserts {
            let modelType = type(of: model)
            let persistableType = modelType.persistableType
            let validatedID = try self.validateID(model.id, for: persistableType)
            let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])

            let typeSubspace = self.itemSubspace.subspace(persistableType)
            let key = typeSubspace.pack(idTuple)

            let oldData = try await transaction.getValue(for: key, snapshot: false)

            if let oldData = oldData {
                // Update
                let oldModel = try DataAccess.deserializeAny(Array(oldData), as: modelType)
                try self.evaluateUpdateSecurity(oldModel, newResource: model, security: security)
            } else {
                // Create
                try self.evaluateCreateSecurity(model, security: security)
            }
        }

        // 既存の保存/削除処理...
        for model in inserts {
            try await self.saveModelUntyped(model, transaction: transaction, encoder: encoder)
        }
        for model in deletes {
            try await self.deleteModelUntyped(model, transaction: transaction)
        }
    }
}
```

### clearAll(type, security)

```swift
func clearAll<T: Persistable>(
    _ type: T.Type,
    security: SecurityContext
) async throws {
    // Admin 専用
    try requireAdmin(
        operation: "clearAll",
        targetType: T.persistableType,
        security: security
    )

    // 既存の実装...
    try await database.withTransaction { transaction in
        try transaction.setOption(forOption: .priorityBatch)
        let typeSubspace = self.itemSubspace.subspace(T.persistableType)
        let (begin, end) = typeSubspace.range()
        transaction.clearRange(beginKey: begin, endKey: end)

        for descriptor in T.indexDescriptors {
            let indexRange = self.indexSubspace.subspace(descriptor.name).range()
            transaction.clearRange(beginKey: indexRange.0, endKey: indexRange.1)
        }
    }
}
```

### withTransaction(configuration, security, operation)

```swift
func withTransaction<T: Sendable>(
    configuration: TransactionConfiguration,
    security: SecurityContext,
    _ operation: @Sendable (any TransactionContextProtocol) async throws -> T
) async throws -> T {
    let runner = TransactionRunner(database: database)

    return try await runner.run(configuration: configuration) { transaction in
        // セキュリティコンテキスト付きの TransactionContext を作成
        let context = SecureTransactionContext(
            transaction: transaction,
            itemSubspace: self.itemSubspace,
            indexSubspace: self.indexSubspace,
            indexMaintenanceService: self.indexMaintenanceService,
            security: security
        )
        return try await operation(context)
    }
}
```

## SecureTransactionContext 実装

```swift
/// セキュリティ評価付きトランザクションコンテキスト
internal final class SecureTransactionContext: TransactionContextProtocol, @unchecked Sendable {

    private let transaction: any TransactionProtocol
    private let itemSubspace: Subspace
    private let indexSubspace: Subspace
    private let indexMaintenanceService: IndexMaintenanceService
    private let security: SecurityContext

    /// サブスペースキャッシュ
    private var subspaceCache: [String: Subspace] = [:]

    init(
        transaction: any TransactionProtocol,
        itemSubspace: Subspace,
        indexSubspace: Subspace,
        indexMaintenanceService: IndexMaintenanceService,
        security: SecurityContext
    ) {
        self.transaction = transaction
        self.itemSubspace = itemSubspace
        self.indexSubspace = indexSubspace
        self.indexMaintenanceService = indexMaintenanceService
        self.security = security
    }

    // MARK: - TransactionContextProtocol

    public func get<T: Persistable>(
        _ type: T.Type,
        id: any TupleElement,
        snapshot: Bool = false
    ) async throws -> T? {
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let keyTuple = (id as? Tuple) ?? Tuple([id])
        let key = typeSubspace.pack(keyTuple)

        guard let bytes = try await transaction.getValue(for: key, snapshot: snapshot) else {
            return nil
        }

        let result: T = try DataAccess.deserialize(bytes)

        // GET セキュリティ評価
        try evaluateGetSecurity(result)

        return result
    }

    public func getMany<T: Persistable>(
        _ type: T.Type,
        ids: [any TupleElement],
        snapshot: Bool = false
    ) async throws -> [T] {
        var results: [T] = []
        for id in ids {
            if let model: T = try await get(type, id: id, snapshot: snapshot) {
                results.append(model)
            }
        }
        return results
    }

    public func set<T: Persistable>(_ model: T) async throws {
        let validatedID = try model.validateIDForStorage()
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])

        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let key = typeSubspace.pack(idTuple)

        // 既存データを取得（Create/Update 判定 + インデックス更新用）
        let oldData = try await transaction.getValue(for: key, snapshot: false)
        let oldModel: T? = oldData.flatMap { try? DataAccess.deserialize($0) }

        // セキュリティ評価
        if let old = oldModel {
            try evaluateUpdateSecurity(old, newResource: model)
        } else {
            try evaluateCreateSecurity(model)
        }

        // 保存
        let data = try DataAccess.serialize(model)
        transaction.setValue(data, for: key)

        // インデックス更新
        try await updateScalarIndexes(oldModel: oldModel, newModel: model, id: idTuple)
    }

    public func delete<T: Persistable>(_ model: T) async throws {
        // DELETE セキュリティ評価
        try evaluateDeleteSecurity(model)

        let validatedID = try model.validateIDForStorage()
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])

        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let key = typeSubspace.pack(idTuple)

        // インデックス削除
        try await updateScalarIndexes(oldModel: model, newModel: nil as T?, id: idTuple)

        // 削除
        transaction.clear(key: key)
    }

    public func delete<T: Persistable>(_ type: T.Type, id: any TupleElement) async throws {
        guard let model: T = try await get(type, id: id, snapshot: false) else {
            return
        }
        try await delete(model)
    }

    public var rawTransaction: any TransactionProtocol {
        transaction
    }

    // MARK: - Private Security Helpers

    private func evaluateGetSecurity(_ resource: any Persistable) throws {
        guard security.shouldEvaluate else { return }
        let modelType = type(of: resource)
        guard let secureType = modelType as? any SecurityPolicy.Type else { return }

        let allowed = secureType._evaluateGet(resource: resource, auth: security.auth)
        guard allowed else {
            throw SecurityError(
                operation: .get,
                targetType: modelType.persistableType,
                reason: "Access denied: get operation not allowed"
            )
        }
    }

    private func evaluateCreateSecurity(_ resource: any Persistable) throws {
        guard security.shouldEvaluate else { return }
        let modelType = type(of: resource)
        guard let secureType = modelType as? any SecurityPolicy.Type else { return }

        let allowed = secureType._evaluateCreate(newResource: resource, auth: security.auth)
        guard allowed else {
            throw SecurityError(
                operation: .create,
                targetType: modelType.persistableType,
                reason: "Access denied: create operation not allowed"
            )
        }
    }

    private func evaluateUpdateSecurity(_ resource: any Persistable, newResource: any Persistable) throws {
        guard security.shouldEvaluate else { return }
        let modelType = type(of: newResource)
        guard let secureType = modelType as? any SecurityPolicy.Type else { return }

        let allowed = secureType._evaluateUpdate(
            resource: resource,
            newResource: newResource,
            auth: security.auth
        )
        guard allowed else {
            throw SecurityError(
                operation: .update,
                targetType: modelType.persistableType,
                reason: "Access denied: update operation not allowed"
            )
        }
    }

    private func evaluateDeleteSecurity(_ resource: any Persistable) throws {
        guard security.shouldEvaluate else { return }
        let modelType = type(of: resource)
        guard let secureType = modelType as? any SecurityPolicy.Type else { return }

        let allowed = secureType._evaluateDelete(resource: resource, auth: security.auth)
        guard allowed else {
            throw SecurityError(
                operation: .delete,
                targetType: modelType.persistableType,
                reason: "Access denied: delete operation not allowed"
            )
        }
    }

    // MARK: - Index Maintenance (簡略版)

    private func updateScalarIndexes<T: Persistable>(
        oldModel: T?,
        newModel: T?,
        id: Tuple
    ) async throws {
        // 既存の TransactionContext と同様のインデックス更新ロジック
        // IndexMaintenanceService を使用
    }
}
```

## FDBContext の変更

```swift
extension FDBContext {

    // MARK: - Security Context 生成

    /// 現在のコンテキストからセキュリティコンテキストを生成
    private var securityContext: SecurityContext {
        SecurityContext(
            auth: auth,
            configuration: container.securityConfiguration
        )
    }

    // MARK: - Fetch (internal)

    /// クエリ実行（internal - QueryExecutor から呼ばれる）
    internal func fetch<T: Persistable>(_ query: Query<T>) async throws -> [T] {
        let store = try await container.store(for: T.self)
        return try await store.fetch(query, security: securityContext)
    }

    internal func fetchCount<T: Persistable>(_ query: Query<T>) async throws -> Int {
        let store = try await container.store(for: T.self)
        return try await store.fetchCount(query, security: securityContext)
    }

    // MARK: - Model (public)

    public func model<T: Persistable>(
        for id: any TupleElement,
        as type: T.Type
    ) async throws -> T? {
        // pending チェック（既存のまま）
        // ...

        let store = try await container.store(for: type)
        return try await store.fetch(type, id: id, security: securityContext)
    }

    // MARK: - Save (internal)

    /// 変更を保存
    public func save() async throws {
        let (inserts, deletes) = // 既存の pending 取得

        let store = try await container.store(for: /* type */)
        try await store.executeBatch(
            inserts: inserts,
            deletes: deletes,
            security: securityContext
        )
    }

    // MARK: - Transaction (public)

    public func withTransaction<T: Sendable>(
        configuration: TransactionConfiguration = .default,
        _ operation: @Sendable (any TransactionContextProtocol) async throws -> T
    ) async throws -> T {
        let store = try await container.defaultStore()
        return try await store.withTransaction(
            configuration: configuration,
            security: securityContext,
            operation
        )
    }

    // MARK: - Enumerate

    public func enumerate<T: Persistable>(
        _ type: T.Type,
        block: (T) throws -> Void
    ) async throws {
        let store = try await container.store(for: type)
        let models = try await store.fetchAll(type, security: securityContext)
        for model in models {
            try block(model)
        }
    }

    // MARK: - Clear All

    public func clearAll<T: Persistable>(_ type: T.Type) async throws {
        let store = try await container.store(for: type)
        try await store.clearAll(type, security: securityContext)
    }
}
```

## QueryExecutor の変更

```swift
public struct QueryExecutor<T: Persistable>: Sendable {

    public func execute() async throws -> [T] {
        // セキュリティ評価コード削除（DataStore で評価される）
        return try await context.fetch(query)
    }

    public func count() async throws -> Int {
        // セキュリティ評価コード削除（DataStore で評価される）
        return try await context.fetchCount(query)
    }
}
```

## Polymorphic API

Polymorphic API は複数の型を横断するため、Admin 専用とする。

```swift
extension FDBContext {

    public func fetchPolymorphic<P: Polymorphable>(
        _ protocolType: P.Type
    ) async throws -> [any Persistable] {
        // Admin 専用チェック
        guard !securityContext.shouldEvaluate else {
            throw SecurityError(
                operation: .list,
                targetType: P.polymorphableType,
                reason: "Polymorphic operations require admin privileges"
            )
        }

        // 既存の実装...
    }
}
```

## 削除するファイル/コード

- `FDBContext+Security.swift` の評価メソッドを DataStore に移動
- `QueryExecutor` のセキュリティ評価コード削除
- 既存の `TransactionContext` を `SecureTransactionContext` に置き換え

## 評価

| 観点 | 値 |
|------|-----|
| セキュリティ評価位置 | DataStore（単一の低レイヤー） |
| データアクセス経路 | 1つ（全て DataStore 経由） |
| 漏れのリスク | なし |
| 責務 | DataStore = データアクセス + セキュリティ |
| 変更範囲 | DataStore プロトコル、FDBDataStore、FDBContext |
| TransactionContext | DataStore.withTransaction() 経由で統合 |

## マイグレーション手順

1. `SecurityContext` を追加
2. `TransactionContextProtocol` を追加
3. `DataStore` プロトコルを拡張
4. `FDBDataStore` にセキュリティ評価を実装
5. `SecureTransactionContext` を実装
6. `FDBContext` を DataStore 経由に変更
7. `QueryExecutor` のセキュリティコード削除
8. `FDBContext+Security.swift` の評価メソッドを削除
9. テスト更新
