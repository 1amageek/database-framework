# Security Rule 設計仕様

## 概要

Protocol ベースの宣言的アクセス制御システム。

### 設計原則

1. **Protocol ベース**: 型ごとに `SecurityPolicy` を実装
2. **宣言的**: 各操作の許可条件を関数として定義
3. **責務分離**: テナント分離は Directory + FDB Partition、ドキュメントレベルは SecurityPolicy

### テナント分離との関係

| 責務 | 担当 | 備考 |
|------|------|------|
| テナント分離 | Directory + FDB Partition | 物理的分離、JWT認証 |
| 認証チェック | SecurityPolicy | ログイン必須か |
| 所有者チェック | SecurityPolicy | 自分のデータか |
| 公開/非公開 | SecurityPolicy | 誰でも見れるか |
| クエリ制限 | SecurityPolicy | limit等 |

```swift
// テナント分離は Directory で設定（SecurityPolicy の責務外）
#Directory<Order>(
    "tenants",
    Field(\.tenantID),
    "orders",
    layer: .partition  // FDB パーティション（物理分離）
)
```

## アーキテクチャ

```
┌─────────────────────────────────────────────────────────────────┐
│                      Application Server                          │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                    DatabaseEngine                           │ │
│  │                                                             │ │
│  │  AuthContextKey.$current.withValue(auth) {                  │ │
│  │      // リクエストスコープで認証情報を設定                     │ │
│  │  }                                                          │ │
│  │                                                             │ │
│  │  ┌───────────────────────────────────────────────────────┐ │ │
│  │  │ FDBContainer (Resource Manager)                        │ │ │
│  │  │  - securityDelegate: DataStoreSecurityDelegate?        │ │ │
│  │  │  - schema, database                                    │ │ │
│  │  └───────────────────────────────────────────────────────┘ │ │
│  │           ↓                                                 │ │
│  │  ┌───────────────────────────────────────────────────────┐ │ │
│  │  │ FDBContext (User-Facing API)                           │ │ │
│  │  │  - insert(), delete(), fetch(), save()                 │ │ │
│  │  │  ❌ Does NOT handle security directly                   │ │ │
│  │  └───────────────────────────────────────────────────────┘ │ │
│  │           ↓                                                 │ │
│  │  ┌───────────────────────────────────────────────────────┐ │ │
│  │  │ FDBDataStore (Data Operations + Security Evaluation)   │ │ │
│  │  │  - securityDelegate?.evaluateGet(resource)             │ │ │
│  │  │  - securityDelegate?.evaluateCreate(resource)          │ │ │
│  │  │  - securityDelegate?.evaluateUpdate(old, new)          │ │ │
│  │  │  - securityDelegate?.evaluateDelete(resource)          │ │ │
│  │  │  - securityDelegate?.evaluateList(type, limit, ...)    │ │ │
│  │  └───────────────────────────────────────────────────────┘ │ │
│  │           ↓                                                 │ │
│  │  ┌───────────────────────────────────────────────────────┐ │ │
│  │  │ DataStoreSecurityDelegate                              │ │ │
│  │  │  - AuthContextKey.current で認証情報を取得             │ │ │
│  │  │  - SecurityPolicy に委譲して評価                       │ │ │
│  │  └───────────────────────────────────────────────────────┘ │ │
│  │                                                             │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                        FoundationDB                              │
│                                                                  │
│  Partition: tenants/[tenantID]/  ← JWT でアクセス制御           │
│  └── posts/[id]                                                 │
│  └── users/[id]                                                 │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 責務分離

| コンポーネント | 責務 |
|--------------|------|
| `FDBContainer` | リソース管理（securityDelegate 保持） |
| `FDBContext` | ユーザー向け API（セキュリティ評価なし） |
| `FDBDataStore` | データ操作 + セキュリティ評価の呼び出し |
| `DataStoreSecurityDelegate` | セキュリティ評価（SecurityPolicy に委譲） |
| `AuthContextKey` | TaskLocal で認証情報を管理 |

## モジュール配置

```
database-kit (クライアント共有)
└── Core/
    └── Security/
        ├── SecurityPolicy.swift       # Protocol 定義
        ├── SecurityQuery.swift        # クエリ情報
        ├── AuthContext.swift          # 認証コンテキスト
        └── SecurityError.swift        # エラー型

database-framework (サーバー専用)
└── DatabaseEngine/
    └── Security/
        ├── SecurityConfiguration.swift
        └── DataStoreSecurityDelegate.swift  # Delegate + TaskLocal + 実装
```

## Core Types（database-kit）

### SecurityPolicy（プロトコル）

```swift
/// セキュリティポリシープロトコル
///
/// 型ごとに実装し、各操作の許可条件を定義する。
///
/// **Usage**:
/// ```swift
/// extension Post: SecurityPolicy {
///     static func allowGet(resource: Post, auth: (any AuthContext)?) -> Bool {
///         resource.isPublic || resource.authorID == auth?.userID
///     }
///     // ...
/// }
/// ```
public protocol SecurityPolicy: Persistable {

    /// 単一ドキュメント取得の許可判定
    ///
    /// - Parameters:
    ///   - resource: 取得対象のドキュメント
    ///   - auth: 認証コンテキスト（nil = 未認証）
    /// - Returns: 許可する場合 true
    static func allowGet(resource: Self, auth: (any AuthContext)?) -> Bool

    /// クエリ（リスト取得）の許可判定
    ///
    /// **重要**: セキュリティルールはフィルタではない。
    /// クエリ結果をフィルタするのではなく、クエリ自体の許可を判定。
    ///
    /// - Parameters:
    ///   - query: クエリ情報（limit, offset 等）
    ///   - auth: 認証コンテキスト（nil = 未認証）
    /// - Returns: 許可する場合 true
    static func allowList(query: SecurityQuery<Self>, auth: (any AuthContext)?) -> Bool

    /// ドキュメント作成の許可判定
    ///
    /// - Parameters:
    ///   - newResource: 作成しようとしているドキュメント
    ///   - auth: 認証コンテキスト（nil = 未認証）
    /// - Returns: 許可する場合 true
    static func allowCreate(newResource: Self, auth: (any AuthContext)?) -> Bool

    /// ドキュメント更新の許可判定
    ///
    /// - Parameters:
    ///   - resource: 更新前のドキュメント
    ///   - newResource: 更新後のドキュメント
    ///   - auth: 認証コンテキスト（nil = 未認証）
    /// - Returns: 許可する場合 true
    static func allowUpdate(resource: Self, newResource: Self, auth: (any AuthContext)?) -> Bool

    /// ドキュメント削除の許可判定
    ///
    /// - Parameters:
    ///   - resource: 削除対象のドキュメント
    ///   - auth: 認証コンテキスト（nil = 未認証）
    /// - Returns: 許可する場合 true
    static func allowDelete(resource: Self, auth: (any AuthContext)?) -> Bool

    // MARK: - 型消去用メソッド（内部用）

    /// 型消去された List 評価
    ///
    /// プロトコル要件として定義することで `any SecurityPolicy.Type` から呼び出し可能。
    /// 通常は `allowList` を実装すればよく、このメソッドを直接実装する必要はない。
    static func _evaluateList(
        limit: Int?,
        offset: Int?,
        orderBy: [String]?,
        auth: (any AuthContext)?
    ) -> Bool
}
```

### デフォルト実装

```swift
public extension SecurityPolicy {
    /// デフォルト: 全拒否（セキュアバイデフォルト）
    static func allowGet(resource: Self, auth: (any AuthContext)?) -> Bool { false }
    static func allowList(query: SecurityQuery<Self>, auth: (any AuthContext)?) -> Bool { false }
    static func allowCreate(newResource: Self, auth: (any AuthContext)?) -> Bool { false }
    static func allowUpdate(resource: Self, newResource: Self, auth: (any AuthContext)?) -> Bool { false }
    static func allowDelete(resource: Self, auth: (any AuthContext)?) -> Bool { false }

    /// 型消去用メソッドのデフォルト実装
    ///
    /// 内部で SecurityQuery を構築し allowList に委譲する。
    static func _evaluateList(
        limit: Int?,
        offset: Int?,
        orderBy: [String]?,
        auth: (any AuthContext)?
    ) -> Bool {
        let query = SecurityQuery<Self>(limit: limit, offset: offset, orderBy: orderBy)
        return allowList(query: query, auth: auth)
    }
}
```

### SecurityQuery（クエリ情報）

```swift
/// クエリ情報
///
/// list 操作時のクエリ制約を検証するために使用。
public struct SecurityQuery<T: Persistable>: Sendable {
    /// クエリの最大取得件数
    public let limit: Int?

    /// オフセット
    public let offset: Int?

    /// ソート順
    public let orderBy: [String]?

    public init(limit: Int? = nil, offset: Int? = nil, orderBy: [String]? = nil) {
        self.limit = limit
        self.offset = offset
        self.orderBy = orderBy
    }
}
```

### AuthContext（認証コンテキスト）

```swift
/// 認証コンテキストプロトコル
///
/// アプリケーションが独自の認証情報型を定義する。
///
/// **信頼モデル**:
/// DatabaseEngine は Auth の検証を行わない。
/// Auth はアプリケーション層で検証済みトークンから構築される前提。
///
/// **最低要件**:
/// - `userID`: 必須。ユーザー識別子
/// - `roles`: オプション。デフォルトは空集合（Admin 判定に影響）
///
/// **Usage**:
/// ```swift
/// // 最小実装（roles はデフォルト空集合）
/// struct SimpleAuth: AuthContext {
///     let userID: String
/// }
///
/// // ロールベース実装
/// struct MyAuth: AuthContext {
///     let userID: String
///     let roles: Set<String>
///     let teamIDs: [String]
/// }
/// ```
public protocol AuthContext: Sendable {
    /// ユーザー識別子（必須）
    var userID: String { get }

    /// ロール一覧（Admin 判定に使用）
    ///
    /// デフォルト実装は空集合を返す。
    /// Admin 判定が必要な場合は明示的に実装すること。
    var roles: Set<String> { get }
}

public extension AuthContext {
    /// デフォルト: 空のロール（Admin 判定されない）
    var roles: Set<String> { [] }
}
```

## Server Types（database-framework）

### SecurityConfiguration

```swift
/// セキュリティ設定
///
/// セキュリティはデフォルトで有効（secure by default）
public struct SecurityConfiguration: Sendable {
    /// セキュリティ機能の有効/無効
    public let isEnabled: Bool

    /// Admin として扱うロール（評価スキップ）
    public let adminRoles: Set<String>

    public init(isEnabled: Bool = true, adminRoles: Set<String> = ["admin"]) {
        self.isEnabled = isEnabled
        self.adminRoles = adminRoles
    }

    /// セキュリティ無効（テスト専用）
    ///
    /// **Warning**: 本番環境では使用禁止
    public static let disabled = SecurityConfiguration(isEnabled: false, adminRoles: [])

    /// セキュリティ有効（デフォルト）
    public static func enabled(adminRoles: Set<String> = ["admin"]) -> SecurityConfiguration {
        SecurityConfiguration(isEnabled: true, adminRoles: adminRoles)
    }
}
```

### SecurityError

```swift
/// セキュリティエラー
public struct SecurityError: Error, Sendable {
    public enum Operation: String, Sendable {
        case get, list, create, update, delete
    }

    public let operation: Operation
    public let targetType: String
    public let reason: String
}
```

### DataStoreSecurityDelegate

```swift
/// DataStore のセキュリティ評価を担当するデリゲート
///
/// **設計ポイント**:
/// - DataStore が保持し、各操作前に呼び出す
/// - 認証情報は TaskLocal (AuthContextKey.current) から取得
/// - SecurityPolicy に委譲して評価
public protocol DataStoreSecurityDelegate: Sendable {
    func evaluateList<T: Persistable>(type: T.Type, limit: Int?, offset: Int?, orderBy: [String]?) throws
    func evaluateGet(_ resource: any Persistable) throws
    func evaluateCreate(_ resource: any Persistable) throws
    func evaluateUpdate(_ resource: any Persistable, newResource: any Persistable) throws
    func evaluateDelete(_ resource: any Persistable) throws
    func requireAdmin(operation: String, targetType: String) throws
}
```

### AuthContextKey（TaskLocal）

```swift
/// リクエストスコープで認証情報を管理
///
/// **Usage**:
/// ```swift
/// // リクエストハンドラで設定
/// try await AuthContextKey.$current.withValue(userAuth) {
///     let context = container.newContext()
///     try await context.save()  // DataStore がセキュリティ評価時に参照
/// }
/// ```
public enum AuthContextKey {
    @TaskLocal public static var current: (any AuthContext)?
}
```

### DefaultSecurityDelegate

```swift
/// デフォルトのセキュリティ評価実装
public final class DefaultSecurityDelegate: DataStoreSecurityDelegate, Sendable {
    private let configuration: SecurityConfiguration

    public init(configuration: SecurityConfiguration) {
        self.configuration = configuration
    }

    /// TaskLocal から認証情報を取得
    private var auth: (any AuthContext)? {
        AuthContextKey.current
    }

    private var shouldEvaluate: Bool {
        guard configuration.isEnabled else { return false }
        guard let auth else { return true }
        return auth.roles.isDisjoint(with: configuration.adminRoles)
    }

    public func evaluateGet(_ resource: any Persistable) throws {
        guard shouldEvaluate else { return }
        let modelType = type(of: resource)
        guard let secureType = modelType as? any SecurityPolicy.Type else { return }

        let allowed = secureType._evaluateGet(resource: resource, auth: auth)
        guard allowed else {
            throw SecurityError(
                operation: .get,
                targetType: modelType.persistableType,
                reason: "Access denied: get operation not allowed"
            )
        }
    }

    // 他のメソッドも同様のパターン...
}
```

### DisabledSecurityDelegate

```swift
/// セキュリティ無効（テスト専用）
///
/// **Warning**: 本番環境では使用禁止
public final class DisabledSecurityDelegate: DataStoreSecurityDelegate, Sendable {
    public init() {}

    public func evaluateList<T: Persistable>(type: T.Type, limit: Int?, offset: Int?, orderBy: [String]?) throws {}
    public func evaluateGet(_ resource: any Persistable) throws {}
    public func evaluateCreate(_ resource: any Persistable) throws {}
    public func evaluateUpdate(_ resource: any Persistable, newResource: any Persistable) throws {}
    public func evaluateDelete(_ resource: any Persistable) throws {}
    public func requireAdmin(operation: String, targetType: String) throws {}
}
```

### FDBDataStore でのセキュリティ呼び出し

```swift
// FDBDataStore 内部
internal final class FDBDataStore: DataStore {
    let securityDelegate: (any DataStoreSecurityDelegate)?

    func fetch<T: Persistable>(_ type: T.Type, id: any TupleElement) async throws -> T? {
        // ... データ取得 ...

        // 取得後にセキュリティ評価
        if let result = result {
            try securityDelegate?.evaluateGet(result)
        }
        return result
    }

    func executeBatchInTransaction(
        inserts: [any Persistable],
        deletes: [any Persistable],
        transaction: any TransactionProtocol
    ) async throws -> [SerializedModel] {
        // Delete のセキュリティ評価
        for model in deletes {
            try securityDelegate?.evaluateDelete(model)
        }

        // Insert/Update のセキュリティ評価
        for model in inserts {
            // 既存レコードがあれば UPDATE、なければ CREATE
            if let oldData = try await transaction.getValue(for: key, snapshot: false) {
                let oldModel = try DataAccess.deserializeAny(oldData, as: modelType)
                try securityDelegate?.evaluateUpdate(oldModel, newResource: model)
            } else {
                try securityDelegate?.evaluateCreate(model)
            }
        }

        // ... データ保存 ...
    }
}
```

## 使用例

### 1. 基本的なアクセス制御

```swift
@Persistable
struct Post {
    var id: String = ULID().ulidString
    var authorID: String
    var title: String
    var content: String
    var isPublic: Bool

    #Directory<Post>("posts")
}

extension Post: SecurityPolicy {

    static func allowGet(resource: Post, auth: (any AuthContext)?) -> Bool {
        // 公開記事は誰でも読める、非公開は作者のみ
        resource.isPublic || resource.authorID == auth?.userID
    }

    static func allowList(query: SecurityQuery<Post>, auth: (any AuthContext)?) -> Bool {
        // 認証済みユーザーのみ、100件まで
        auth != nil && (query.limit ?? 0) <= 100
    }

    static func allowCreate(newResource: Post, auth: (any AuthContext)?) -> Bool {
        // 認証済み、かつ authorID が自分
        auth != nil && newResource.authorID == auth?.userID
    }

    static func allowUpdate(resource: Post, newResource: Post, auth: (any AuthContext)?) -> Bool {
        // 作者のみ更新可能、authorID 変更不可
        resource.authorID == auth?.userID
            && newResource.authorID == resource.authorID
    }

    static func allowDelete(resource: Post, auth: (any AuthContext)?) -> Bool {
        // 作者のみ削除可能
        resource.authorID == auth?.userID
    }
}
```

### 2. チームベースのアクセス制御

```swift
struct TeamAuth: AuthContext {
    let userID: String
    let teamIDs: [String]
    // roles はデフォルト空集合を使用（Admin 判定不要）
}

@Persistable
struct Document {
    var id: String = ULID().ulidString
    var teamID: String
    var title: String
    var content: String
}

extension Document: SecurityPolicy {

    static func allowGet(resource: Document, auth: (any AuthContext)?) -> Bool {
        guard let auth = auth as? TeamAuth else { return false }
        return auth.teamIDs.contains(resource.teamID)
    }

    static func allowList(query: SecurityQuery<Document>, auth: (any AuthContext)?) -> Bool {
        auth != nil
    }

    static func allowCreate(newResource: Document, auth: (any AuthContext)?) -> Bool {
        guard let auth = auth as? TeamAuth else { return false }
        return auth.teamIDs.contains(newResource.teamID)
    }

    static func allowUpdate(resource: Document, newResource: Document, auth: (any AuthContext)?) -> Bool {
        guard let auth = auth as? TeamAuth else { return false }
        // 元のチームに所属 & チーム変更不可
        return auth.teamIDs.contains(resource.teamID)
            && newResource.teamID == resource.teamID
    }

    static func allowDelete(resource: Document, auth: (any AuthContext)?) -> Bool {
        guard let auth = auth as? TeamAuth else { return false }
        return auth.teamIDs.contains(resource.teamID)
    }
}
```

### 3. マルチテナント + ドキュメントレベル制御

```swift
@Persistable
struct Order {
    var id: String = ULID().ulidString
    var tenantID: String
    var customerID: String
    var total: Int
    var status: String

    // テナント分離は Directory で設定（物理分離）
    #Directory<Order>("tenants", Field(\.tenantID), "orders", layer: .partition)
}

extension Order: SecurityPolicy {
    // テナント分離は Directory + FDB Partition で既に実現
    // ここでは テナント内の アクセス制御のみ

    static func allowGet(resource: Order, auth: (any AuthContext)?) -> Bool {
        // 自分の注文のみ
        resource.customerID == auth?.userID
    }

    static func allowList(query: SecurityQuery<Order>, auth: (any AuthContext)?) -> Bool {
        auth != nil && (query.limit ?? 0) <= 50
    }

    static func allowCreate(newResource: Order, auth: (any AuthContext)?) -> Bool {
        auth != nil && newResource.customerID == auth?.userID
    }

    static func allowUpdate(resource: Order, newResource: Order, auth: (any AuthContext)?) -> Bool {
        // 注文の更新は不可（別途 Admin API で対応）
        false
    }

    static func allowDelete(resource: Order, auth: (any AuthContext)?) -> Bool {
        // 削除は不可
        false
    }
}
```

### 4. ロールベースのアクセス制御

```swift
struct RoleAuth: AuthContext {
    let userID: String
    let roles: Set<String>

    var isAdmin: Bool { roles.contains("admin") }
    var isModerator: Bool { roles.contains("moderator") }
}

extension Post: SecurityPolicy {

    static func allowGet(resource: Post, auth: (any AuthContext)?) -> Bool {
        resource.isPublic || resource.authorID == auth?.userID
    }

    static func allowDelete(resource: Post, auth: (any AuthContext)?) -> Bool {
        guard let auth = auth as? RoleAuth else { return false }
        // 作者、または Moderator/Admin
        return resource.authorID == auth.userID
            || auth.isModerator
            || auth.isAdmin
    }

    // ...
}
```

## 評価フロー

```
リクエストハンドラ: AuthContextKey.$current.withValue(userAuth) { ... }
         ↓
context.fetch(Post.self, id: "xxx")
         ↓
┌────────────────────────────────────────────────────────────────┐
│  FDBContext                                                     │
│  → store.fetch(Post.self, id: "xxx") を呼び出し                │
└────────────────────────────────────────────────────────────────┘
         ↓
┌────────────────────────────────────────────────────────────────┐
│  FDBDataStore                                                   │
│                                                                │
│  1. FoundationDB からデータ取得                                 │
│  2. securityDelegate?.evaluateGet(result) を呼び出し           │
└────────────────────────────────────────────────────────────────┘
         ↓
┌────────────────────────────────────────────────────────────────┐
│  DefaultSecurityDelegate                                        │
│                                                                │
│  1. shouldEvaluate チェック                                     │
│     - configuration.isEnabled？ → 無効ならスキップ             │
│     - isAdmin(auth)？ → Admin ならスキップ                      │
│  2. auth = AuthContextKey.current                              │
│  3. Post.allowGet(resource: post, auth: auth)                  │
│     → true: 処理続行                                            │
│     → false: SecurityError を throw                            │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### save() の評価フロー

```
context.insert(newPost)
context.save()
         ↓
┌────────────────────────────────────────────────────────────────┐
│  FDBContext.save()                                              │
│  → anyStore.withRawTransaction { transaction in                │
│        store.executeBatchInTransaction(inserts, deletes, tx)   │
│    }                                                           │
└────────────────────────────────────────────────────────────────┘
         ↓
┌────────────────────────────────────────────────────────────────┐
│  FDBDataStore.executeBatchInTransaction()                       │
│                                                                │
│  1. DELETE: securityDelegate?.evaluateDelete(model)            │
│  2. INSERT/UPDATE:                                              │
│     - 既存データあり → securityDelegate?.evaluateUpdate(old,new)│
│     - 新規 → securityDelegate?.evaluateCreate(model)           │
│  3. データ保存、インデックス更新                                │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

## 参考資料

- [FoundationDB Authorization](https://apple.github.io/foundationdb/authorization.html)
- [PostgreSQL Row Level Security](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)
