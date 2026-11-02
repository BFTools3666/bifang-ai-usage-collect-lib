//! 按调用方相对路径保存各 Agent 的统计数据库、checkpoint 和事务快照。
//!
//! 不存储 GUI 设置、账号凭据或其他产品业务表；宿主拥有调度与运行时。

mod discovery_candidates;
pub use discovery_candidates::ConfirmedRootCandidate;
mod file_import;
pub use file_import::PreparedFileIndex;
mod binding;
mod call_store;
mod checkpoint;
mod error;
mod path_codec;
mod private_sqlite;
mod root_registry;
mod scan_connection;
mod schema;
mod snapshot;
mod snapshot_ops;

use std::path::{Path, PathBuf};

use sea_orm::{ConnectionTrait, DatabaseConnection, DatabaseTransaction, DbBackend, Statement};

pub use crate::{
    ClaudeBatchOutcome, DiscoveryMethod, LocalError, LocalErrorKind, RegisteredRoot, RootRecord,
    SourceParseCheckpoint, StoredSourceFile, UsageSnapshot, coverage_state_from_label,
    coverage_state_label, stable_id,
};

#[cfg(any(test, feature = "test-util"))]
const USAGE_INDEX_FILE_NAME: &str = "test-index.db";
use error::IntoLocalResult;
use error::map_database_error;
use scan_connection::ScanConnection;

use self::private_sqlite::{PrivateSqliteError, open_private_sqlite};

/// 管理调用方指定 app-data 内的单一物理数据库连接。
// 池上限固定为 1（SeaORM 对 SQLite 后端在未显式设置时的默认值）：
// 效果等价于旧实现里“每次都新开一个 `rusqlite::Connection`”——不同的
// `LocalIndex` 实例（不同 Tauri command 调用）各自持有独立的单连接
// 连接池，靠 `busy_timeout` PRAGMA 而不是应用层显式串行化来处理短暂的
// 跨连接写锁竞争，与本模块一直以来“同一时刻只有一个扫描 writer”的
// 假设一致。
pub struct LocalIndex {
    /// 索引 SQLite 连接。
    pub(crate) connection: DatabaseConnection,
    /// 当前扫描尚未发布的单一写事务。文件解析继续在数据库外完成，来源、
    /// checkpoint、覆盖状态与调用只有在扫描终态提交时才一起对其他连接可见。
    pub(crate) active_scan_transaction: Option<DatabaseTransaction>,
    /// 当前事务内的扫描运行记录。工作流可先建立事务并完成清理/根对账，
    /// 直到真正开始解析时才补上这份终态所需的脱敏元数据。
    pub(crate) active_scan_record: Option<ActiveScanRecord>,
    /// `true` 表示事务由完整工作流拥有，provider 只暂存终态，待根激活状态
    /// 一并收口后再发布。
    pub(crate) defer_scan_publication: bool,
    /// provider 已把 completed/partial 终态写入事务，但工作流尚未发布。
    pub(crate) staged_scan_status: Option<String>,
    /// 暂存终态对应的真实完成时间，供 commit 失败后的可靠终态修复复用。
    pub(crate) staged_scan_finished_at_epoch_ms: Option<i64>,
    /// 当前客户端数据库只接受的 parser generation。
    pub(crate) parser_version: u32,
    /// 索引数据库文件路径。
    #[cfg_attr(not(test), allow(dead_code))]
    database_path: PathBuf,
    /// 此连接绑定的物理 Agent；仅测试便利入口不绑定。
    client: Option<crate::SourceClientKind>,
}

impl std::fmt::Debug for LocalIndex {
    /// 隐藏连接、数据库路径与扫描原始元数据，只保留安全运行状态。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalIndex")
            .field("client", &self.client)
            .field("parser_version", &self.parser_version)
            .field("scan_active", &self.active_scan_transaction.is_some())
            .finish_non_exhaustive()
    }
}

impl LocalIndex {
    /// 在基准目录内按相对路径打开单个 Agent 的统计数据库。
    ///
    /// 例如分别传入 `codex.db`、`claude.db`、`grok.db`。目录可自动创建，
    /// 绝对路径、上跳、链接、WorkBuddy 和已绑定其他 Agent 的库均拒绝。
    /// 调用者负责提供 Tokio 运行时，不得用于 GUI 设置或账号业务数据。
    ///
    /// # Errors
    /// 路径、权限、Agent 身份、schema 或 SQLite 操作失败时返回脱敏错误。
    pub async fn open(
        base_dir: &Path,
        relative_path: &Path,
        client: crate::SourceClientKind,
    ) -> Result<Self, LocalError> {
        binding::agent_label(client)?;
        Self::open_internal(
            base_dir,
            relative_path,
            client.parser_version(),
            Some(client),
        )
        .await
    }

    /// 为当前格式的索引回归提供显式夹具入口，不作为生产默认路径。
    #[cfg(any(test, feature = "test-util"))]
    pub async fn open_in_app_data(
        base_dir: &Path,
        parser_version: u32,
    ) -> Result<Self, LocalError> {
        Self::open_internal(
            base_dir,
            Path::new(USAGE_INDEX_FILE_NAME),
            parser_version,
            None,
        )
        .await
    }

    /// 通过显式相对路径和解析代次建立当前格式的回归夹具。
    #[cfg(any(test, feature = "test-util"))]
    pub async fn open_test_at(
        base_dir: &Path,
        relative_path: &Path,
        parser_version: u32,
    ) -> Result<Self, LocalError> {
        Self::open_internal(base_dir, relative_path, parser_version, None).await
    }

    /// 使用经过校验的数据库位置构造连接及扫描发布状态。
    async fn open_internal(
        base_dir: &Path,
        relative_path: &Path,
        parser_version: u32,
        client: Option<crate::SourceClientKind>,
    ) -> Result<Self, LocalError> {
        let (connection, database_path) = open_database(base_dir, relative_path, client).await?;
        Ok(Self {
            connection,
            active_scan_transaction: None,
            active_scan_record: None,
            defer_scan_publication: false,
            staged_scan_status: None,
            staged_scan_finished_at_epoch_ms: None,
            database_path,
            parser_version,
            client,
        })
    }

    /// 测试夹具可注入 SQLite 故障，不向生产消费者暴露数据库连接。
    #[cfg(feature = "test-util")]
    pub fn test_connection(&self) -> &DatabaseConnection {
        &self.connection
    }

    /// 测试夹具检查失败或取消后是否已经释放写事务。
    #[cfg(feature = "test-util")]
    pub fn test_scan_transaction_active(&self) -> bool {
        self.active_scan_transaction.is_some()
    }

    /// 返回扫描内部应使用的连接；扫描外仍直接使用数据库连接。
    ///
    /// 这只用于单条读写。需要在扫描外自建短事务的多语句操作必须显式分支，
    /// 避免在扫描事务里另开一条独立连接而提前发布局部结果。
    pub(crate) fn scan_connection(&self) -> ScanConnection<'_> {
        match self.active_scan_transaction.as_ref() {
            Some(transaction) => ScanConnection::Transaction(transaction),
            None => ScanConnection::Connection(&self.connection),
        }
    }

    /// 返回数据库固定文件名所在位置。目前只有测试用它独立开一个连接核对
    /// 落盘内容；尚无隐私页/清理 UI 消费者，等真的接入那类界面时再放开
    /// 下面的 `allow(dead_code)`。
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn database_path(&self) -> &Path {
        &self.database_path
    }
}

/// 回滚扫描事务后重建失败/取消终态所需的最小脱敏事实。
#[derive(Debug, Clone)]
pub(crate) struct ActiveScanRecord {
    /// 扫描 ID。
    pub(crate) scan_id: String,
    /// 扫描范围标识（如快速或全设备）。
    pub(crate) mode: String,
    /// 扫描开始时间（毫秒 Unix 时间戳）。
    pub(crate) started_at_epoch_ms: i64,
}

/// 在调用方指定位置初始化新格式统计库，或只读验证已有库的格式与 Agent。
async fn open_database(
    app_data_dir: &Path,
    relative_path: &Path,
    client: Option<crate::SourceClientKind>,
) -> Result<(DatabaseConnection, PathBuf), LocalError> {
    let (connection, database_path) = open_private_sqlite(app_data_dir, relative_path)
        .await
        .map_err(map_private_sqlite_error)?;

    if let Err(error) = schema::ensure_schema(&connection, client).await {
        let _ = connection.close().await;
        return Err(error);
    }
    private_sqlite::set_private_database_permissions(&database_path)
        .await
        .map_err(map_private_sqlite_error)?;
    enable_concurrent_snapshot_reads(&connection).await?;

    Ok((connection, database_path))
}

/// 让长时间扫描提交不阻塞新概览读取；模式持久保存在本机索引数据库中。
async fn enable_concurrent_snapshot_reads(
    connection: &DatabaseConnection,
) -> Result<(), LocalError> {
    let mode = connection
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "PRAGMA journal_mode = WAL".to_owned(),
        ))
        .await
        .map_err(map_database_error)?
        .ok_or_else(|| {
            LocalError::new(
                LocalErrorKind::Database,
                "local index journal is unreadable",
            )
        })?
        .try_get_by_index::<String>(0)
        .map_err(map_database_error)?;
    if mode != "wal" {
        return Err(LocalError::new(
            LocalErrorKind::Database,
            "local index journal does not support concurrent reads",
        ));
    }
    Ok(())
}

/// 把共用 SQLite 打开错误映射为本机索引稳定错误。
fn map_private_sqlite_error(error: PrivateSqliteError) -> LocalError {
    match error {
        PrivateSqliteError::InvalidPath => {
            LocalError::new(LocalErrorKind::InvalidPath, "app data directory is invalid")
        }
        PrivateSqliteError::PermissionDenied => {
            LocalError::new(LocalErrorKind::PermissionDenied, "local access was denied")
        }
        PrivateSqliteError::StorageUnavailable => LocalError::new(
            LocalErrorKind::SourceUnavailable,
            "local source operation failed",
        ),
        PrivateSqliteError::Database => {
            LocalError::new(LocalErrorKind::Database, "local index operation failed")
        }
    }
}

#[cfg(test)]
mod read_snapshot_tests;
#[cfg(test)]
mod scan_publication_tests;
#[cfg(test)]
mod tests;
