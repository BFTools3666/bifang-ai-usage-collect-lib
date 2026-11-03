//! canonical 调用聚合、与扫描运行记录的持久化。

use sea_orm::{
    ConnectionTrait, DbBackend, SqliteTransactionMode, Statement, TransactionOptions,
    TransactionTrait,
};
use uuid::Uuid;

use crate::{
    CanonicalUsageSet, CoverageState, LocalIndexState, LocalUsageAggregate, ProviderKind,
    ScanCancellation,
};

use super::call_store::to_sql_u64;
use super::snapshot::{
    load_canonical_calls_since_with_cancellation, load_canonical_snapshot_with_cancellation,
    load_usage_snapshot, load_usage_snapshot_since,
};
use super::snapshot_cpu::{aggregate_owned_canonical, check_cancelled};
use super::{
    ActiveScanRecord, IntoLocalResult, LocalError, LocalErrorKind, LocalIndex, UsageSnapshot,
    map_database_error, stable_id,
};

/// 为快照维护操作构造带绑定值的 SQLite 语句。
fn statement(sql: &str, values: Vec<sea_orm::Value>) -> Statement {
    Statement::from_sql_and_values(DbBackend::Sqlite, sql, values)
}

/// 在调用方选择的事务边界内删除窗口外派生用量。
async fn prune_usage_before_on<C: ConnectionTrait + ?Sized>(
    connection: &C,
    cutoff_epoch_ms: i64,
) -> Result<(), LocalError> {
    connection
        .execute_raw(statement(
            "DELETE FROM usage_calls WHERE occurred_at_epoch_ms < ?1",
            vec![cutoff_epoch_ms.into()],
        ))
        .await
        .into_local_result()?;
    connection
        .execute_raw(statement(
            "DELETE FROM usage_token_snapshots WHERE occurred_at_epoch_ms < ?1",
            vec![cutoff_epoch_ms.into()],
        ))
        .await
        .into_local_result()?;
    Ok(())
}

/// 在扫描事务内创建尚未发布的运行记录。
async fn insert_running_scan_run<C: ConnectionTrait + ?Sized>(
    connection: &C,
    record: &ActiveScanRecord,
) -> Result<(), LocalError> {
    connection
        .execute_raw(statement(
            "INSERT INTO scan_runs
             (scan_id, mode, started_at_epoch_ms, status, cancelled,
              files_scanned, calls_added, warning_count)
             VALUES (?1, ?2, ?3, 'running', 0, 0, 0, 0)",
            vec![
                record.scan_id.clone().into(),
                record.mode.clone().into(),
                record.started_at_epoch_ms.into(),
            ],
        ))
        .await
        .into_local_result()?;
    Ok(())
}

/// 更新扫描事务内已有的运行记录。
#[allow(clippy::too_many_arguments)]
async fn update_scan_run_terminal<C: ConnectionTrait + ?Sized>(
    connection: &C,
    scan_id: &str,
    finished_at_epoch_ms: i64,
    status: &str,
    cancelled: bool,
    files_scanned: i64,
    calls_added: i64,
    warning_count: i64,
) -> Result<(), LocalError> {
    let result = connection
        .execute_raw(statement(
            "UPDATE scan_runs SET
               finished_at_epoch_ms = ?2, status = ?3, cancelled = ?4,
               files_scanned = ?5, calls_added = ?6, warning_count = ?7
             WHERE scan_id = ?1",
            vec![
                scan_id.into(),
                finished_at_epoch_ms.into(),
                status.into(),
                cancelled.into(),
                files_scanned.into(),
                calls_added.into(),
                warning_count.into(),
            ],
        ))
        .await
        .into_local_result()?;
    if result.rows_affected() != 1 {
        return Err(LocalError::new(
            LocalErrorKind::Database,
            "scan publication record is missing",
        ));
    }
    Ok(())
}

/// 回滚后在已发布连接上持久化失败或取消终态；upsert 也能修复极少数
/// SQLite commit 返回不确定结果时遗留的非终态行。
#[allow(clippy::too_many_arguments)]
async fn write_terminal_scan_run<C: ConnectionTrait + ?Sized>(
    connection: &C,
    record: &ActiveScanRecord,
    finished_at_epoch_ms: i64,
    status: &str,
    cancelled: bool,
    files_scanned: i64,
    calls_added: i64,
    warning_count: i64,
) -> Result<(), LocalError> {
    connection
        .execute_raw(statement(
            "INSERT INTO scan_runs
             (scan_id, mode, started_at_epoch_ms, finished_at_epoch_ms, status, cancelled,
              files_scanned, calls_added, warning_count)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(scan_id) DO UPDATE SET
               mode = excluded.mode,
               started_at_epoch_ms = excluded.started_at_epoch_ms,
               finished_at_epoch_ms = excluded.finished_at_epoch_ms,
               status = excluded.status,
               cancelled = excluded.cancelled,
               files_scanned = excluded.files_scanned,
               calls_added = excluded.calls_added,
               warning_count = excluded.warning_count",
            vec![
                record.scan_id.clone().into(),
                record.mode.clone().into(),
                record.started_at_epoch_ms.into(),
                finished_at_epoch_ms.into(),
                status.into(),
                cancelled.into(),
                files_scanned.into(),
                calls_added.into(),
                warning_count.into(),
            ],
        ))
        .await
        .into_local_result()?;
    Ok(())
}

/// 从已发布连接读取扫描终态，用于消解 commit 返回错误时的未知结果。
async fn load_scan_status<C: ConnectionTrait + ?Sized>(
    connection: &C,
    scan_id: &str,
) -> Result<Option<String>, LocalError> {
    connection
        .query_one_raw(statement(
            "SELECT status FROM scan_runs WHERE scan_id = ?1",
            vec![scan_id.into()],
        ))
        .await
        .into_local_result()?
        .map(|row| row.try_get_by_index(0).into_local_result())
        .transpose()
}

/// 消解 commit 返回错误后的可见终态。只有完全不存在运行记录时才能断定
/// 事务没有发布并补写 Failed；任意已存在但不符合预期的终态都可能来自一次
/// 实际成功的提交，必须保持原样并把结果标记为不确定。
pub(super) async fn resolve_scan_commit_error<C: ConnectionTrait + ?Sized>(
    connection: &C,
    record: &ActiveScanRecord,
    finished_at_epoch_ms: i64,
    expected_status: &str,
    commit_error: LocalError,
) -> Result<(), LocalError> {
    match load_scan_status(connection, &record.scan_id).await {
        Ok(Some(status)) if status == expected_status => Ok(()),
        Ok(Some(_)) => Err(LocalError::new(
            LocalErrorKind::Database,
            "scan publication commit outcome is uncertain",
        )),
        Ok(None) => {
            let _ = write_terminal_scan_run(
                connection,
                record,
                finished_at_epoch_ms,
                "failed",
                false,
                0,
                0,
                0,
            )
            .await;
            Err(commit_error)
        }
        // commit 结果不确定且复读本身失败时不能反向覆盖：事务可能
        // 已经成功发布 completed/partial，只向调用方保留原始失败。
        Err(_) => Err(commit_error),
    }
}

impl LocalIndex {
    /// 返回当前 parser generation 是否已经存在可展示的派生用量。
    pub async fn has_current_parser_usage(&self) -> Result<bool, LocalError> {
        match self.active_scan_transaction.as_ref() {
            Some(transaction) => {
                super::snapshot::has_current_parser_usage(transaction, self.parser_version).await
            }
            None => {
                super::snapshot::has_current_parser_usage(&self.connection, self.parser_version)
                    .await
            }
        }
    }

    /// 只用同一快照内的有界计数读取已发布索引四态，不装载完整调用。
    /// # Errors
    /// 同一实例持有未发布扫描时立即返回 `ScanBusy`，避免等待自己的唯一连接。
    pub async fn index_state(&self) -> Result<LocalIndexState, LocalError> {
        self.ensure_published_read()?;
        super::snapshot::load_index_state(&self.connection, self.parser_version).await
    }

    /// 已发布视图不得与同一实例的扫描写事务争用唯一连接。
    fn ensure_published_read(&self) -> Result<(), LocalError> {
        if self.active_scan_transaction.is_some() {
            return Err(LocalError::new(
                LocalErrorKind::ScanBusy,
                "published snapshot is unavailable during an active scan",
            ));
        }
        Ok(())
    }

    /// 删除保留窗口以前的派生调用与累计快照，保留根和来源 checkpoint。
    pub async fn prune_usage_before(&mut self, cutoff_epoch_ms: i64) -> Result<(), LocalError> {
        if let Some(transaction) = &self.active_scan_transaction {
            prune_usage_before_on(transaction, cutoff_epoch_ms).await
        } else {
            let transaction = self.connection.begin().await.into_local_result()?;
            prune_usage_before_on(&transaction, cutoff_epoch_ms).await?;
            transaction.commit().await.into_local_result()
        }
    }

    /// 为完整 core 工作流预先建立唯一发布事务；清理、根对账、认领、扫描与
    /// 根终态都复用它，直到显式发布或失败/取消回滚。
    pub async fn begin_scan_attempt(&mut self) -> Result<(), LocalError> {
        if self.active_scan_transaction.is_some() {
            return Err(LocalError::new(
                LocalErrorKind::ScanBusy,
                "scan publication transaction is already active",
            ));
        }
        // 策略读取之后仍会写入；先取得 writer，避免 WAL 旧快照升级时报
        // SQLITE_BUSY_SNAPSHOT。其他连接仍能读取最近一次已发布快照。
        // SQLite 隔离约定：https://www.sqlite.org/isolation.html
        self.active_scan_transaction = Some(
            self.connection
                .begin_with_options(TransactionOptions {
                    sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                    ..TransactionOptions::default()
                })
                .await
                .into_local_result()?,
        );
        self.active_scan_record = None;
        self.defer_scan_publication = true;
        self.staged_scan_status = None;
        self.staged_scan_finished_at_epoch_ms = None;
        Ok(())
    }

    /// 放弃尚未进入 provider 扫描的工作流尝试。已经建立扫描记录的失败由
    /// `fail_active_scan` 收口，避免把无 scan id 的发现错误伪造成一次运行。
    pub async fn abort_scan_attempt(&mut self) -> Result<(), LocalError> {
        let Some(transaction) = self.active_scan_transaction.take() else {
            return Ok(());
        };
        self.active_scan_record = None;
        self.defer_scan_publication = false;
        self.staged_scan_status = None;
        self.staged_scan_finished_at_epoch_ms = None;
        transaction.rollback().await.into_local_result()
    }

    /// 发布由完整工作流暂存的扫描、清理、根对账与激活终态。
    pub async fn publish_scan_attempt(&mut self) -> Result<(), LocalError> {
        let Some(transaction) = self.active_scan_transaction.take() else {
            return Ok(());
        };
        let Some(record) = self.active_scan_record.take() else {
            self.defer_scan_publication = false;
            self.staged_scan_status = None;
            self.staged_scan_finished_at_epoch_ms = None;
            transaction.rollback().await.into_local_result()?;
            return Err(LocalError::new(
                LocalErrorKind::Database,
                "scan publication record is missing",
            ));
        };
        let Some(expected_status) = self.staged_scan_status.take() else {
            self.defer_scan_publication = false;
            self.staged_scan_finished_at_epoch_ms = None;
            transaction.rollback().await.into_local_result()?;
            write_terminal_scan_run(
                &self.connection,
                &record,
                record.started_at_epoch_ms,
                "failed",
                false,
                0,
                0,
                0,
            )
            .await?;
            return Err(LocalError::new(
                LocalErrorKind::Database,
                "scan publication terminal is missing",
            ));
        };
        let finished_at_epoch_ms = self
            .staged_scan_finished_at_epoch_ms
            .take()
            .unwrap_or(record.started_at_epoch_ms);
        self.defer_scan_publication = false;
        match transaction.commit().await {
            Ok(()) => Ok(()),
            Err(error) => {
                resolve_scan_commit_error(
                    &self.connection,
                    &record,
                    finished_at_epoch_ms,
                    &expected_status,
                    map_database_error(error),
                )
                .await
            }
        }
    }

    /// 把工作流合并后的 Partial 覆盖反映到同一事务内的 scan terminal，再发布。
    pub async fn publish_scan_attempt_for_coverage(
        &mut self,
        coverage_state: CoverageState,
    ) -> Result<(), LocalError> {
        if coverage_state == CoverageState::Partial
            && self.staged_scan_status.as_deref() == Some("completed")
        {
            let record = self.active_scan_record.as_ref().ok_or_else(|| {
                LocalError::new(
                    LocalErrorKind::Database,
                    "scan publication record is missing",
                )
            })?;
            let result = self
                .active_scan_transaction
                .as_ref()
                .ok_or_else(|| {
                    LocalError::new(
                        LocalErrorKind::Database,
                        "scan publication transaction is not active",
                    )
                })?
                .execute_raw(statement(
                    "UPDATE scan_runs SET status = 'partial'
                     WHERE scan_id = ?1 AND status = 'completed'",
                    vec![record.scan_id.clone().into()],
                ))
                .await
                .into_local_result()?;
            if result.rows_affected() != 1 {
                return Err(LocalError::new(
                    LocalErrorKind::Database,
                    "scan publication record is missing",
                ));
            }
            self.staged_scan_status = Some("partial".to_owned());
        }
        self.publish_scan_attempt().await
    }

    /// 当前事务内的聚合只供扫描摘要使用，确保聚合失败发生在发布之前。
    pub async fn aggregate_active_scan_for_provider(
        &self,
        provider: ProviderKind,
    ) -> Result<LocalUsageAggregate, LocalError> {
        self.aggregate_active_scan_for_provider_with_cancellation(
            provider,
            &ScanCancellation::default(),
        )
        .await
    }

    /// 扫描摘要在 SQL 页与 CPU 调用边界响应取消，不返回或发布半套聚合。
    /// # Errors
    /// 取消或读取失败返回脱敏错误，调用方仍须回滚当前扫描事务。
    pub async fn aggregate_active_scan_for_provider_with_cancellation(
        &self,
        provider: ProviderKind,
        cancellation: &ScanCancellation,
    ) -> Result<LocalUsageAggregate, LocalError> {
        check_cancelled(cancellation)?;
        let canonical = if let Some(transaction) = &self.active_scan_transaction {
            load_canonical_calls_since_with_cancellation(
                transaction,
                self.parser_version,
                i64::MIN,
                cancellation,
            )
            .await?
        } else {
            self.ensure_published_read()?;
            load_canonical_snapshot_with_cancellation(
                &self.connection,
                self.parser_version,
                cancellation,
            )
            .await?
        };
        aggregate_owned_canonical(canonical, provider, cancellation).await
    }

    /// 在同一只读事务内读取已发布调用与累计快照，并执行 canonical 去重。
    /// # Errors
    /// 同一实例持有未发布扫描时立即返回 `ScanBusy`，存储错误保持原稳定类别。
    pub async fn canonical_calls(&self) -> Result<CanonicalUsageSet, LocalError> {
        self.ensure_published_read()?;
        super::snapshot::load_canonical_snapshot(&self.connection, self.parser_version).await
    }

    /// 在一个只读快照事务中取得已发布调用与根记录，不触发扫描或写入。
    /// # Errors
    /// 同一实例持有未发布扫描时立即返回 `ScanBusy`，其他存储失败返回稳定错误。
    pub async fn usage_snapshot(&mut self) -> Result<UsageSnapshot, LocalError> {
        self.ensure_published_read()?;
        load_usage_snapshot(&self.connection, self.parser_version).await
    }

    /// 在一个只读事务中只装载给定时间下界后的已发布调用。
    /// # Errors
    /// 同一实例持有未发布扫描时立即返回 `ScanBusy`，其他存储失败返回稳定错误。
    pub async fn usage_snapshot_since(
        &mut self,
        cutoff_epoch_ms: i64,
    ) -> Result<UsageSnapshot, LocalError> {
        self.ensure_published_read()?;
        load_usage_snapshot_since(&self.connection, self.parser_version, cutoff_epoch_ms).await
    }

    /// 返回全部 canonical 本机调用的聚合。
    #[cfg(any(test, feature = "test-util"))]
    pub async fn aggregate(&self) -> Result<LocalUsageAggregate, LocalError> {
        let canonical = self.canonical_calls().await.into_local_result()?;
        aggregate_owned_canonical(
            canonical,
            ProviderKind::RolloutJsonl,
            &ScanCancellation::default(),
        )
        .await
    }

    /// 按明确本机 provider 的业务口径聚合当前 generation，供扫描摘要复用。
    pub async fn aggregate_for_provider(
        &self,
        provider: ProviderKind,
    ) -> Result<LocalUsageAggregate, LocalError> {
        self.aggregate_for_provider_with_cancellation(provider, &ScanCancellation::default())
            .await
    }

    /// 从同一已发布快照生成可取消摘要，保持当前 parser 与 provider 总量口径。
    /// # Errors
    /// 活动写事务仍返回 ScanBusy，取消和存储错误返回稳定脱敏类别。
    pub async fn aggregate_for_provider_with_cancellation(
        &self,
        provider: ProviderKind,
        cancellation: &ScanCancellation,
    ) -> Result<LocalUsageAggregate, LocalError> {
        self.ensure_published_read()?;
        let canonical = load_canonical_snapshot_with_cancellation(
            &self.connection,
            self.parser_version,
            cancellation,
        )
        .await?;
        aggregate_owned_canonical(canonical, provider, cancellation).await
    }

    /// 清空本产品派生调用、checkpoint 与扫描记录，保留用户根登记。
    // “清空索引”只删除本产品从原始文件派生出来的数据（调用记录、文件
    // checkpoint、扫描历史），`source_roots` 表本身
    // （用户登记了哪些数据根、启用状态、别名）完全不动——对应产品语义
    // “清空索引不等于清空数据源设置”，用户下次扫描会重新从头解析原始
    // 文件，但不需要重新登记数据根。
    pub async fn clear_index(&mut self) -> Result<(), LocalError> {
        let transaction = self.connection.begin().await.into_local_result()?;
        transaction
            .execute_raw(statement("DELETE FROM usage_calls", vec![]))
            .await
            .into_local_result()?;
        transaction
            .execute_raw(statement("DELETE FROM source_files", vec![]))
            .await
            .into_local_result()?;
        transaction
            .execute_raw(statement("DELETE FROM scan_runs", vec![]))
            .await
            .into_local_result()?;
        transaction
            .execute_raw(statement(
                "UPDATE source_roots SET last_coverage_state = NULL",
                vec![],
            ))
            .await
            .into_local_result()?;
        transaction.commit().await.into_local_result()?;
        Ok(())
    }

    /// 在当前发布事务内创建扫描运行记录并返回不含路径的稳定扫描 ID。
    pub async fn begin_scan(
        &mut self,
        mode: &str,
        started_at_epoch_ms: i64,
    ) -> Result<String, LocalError> {
        if self.active_scan_record.is_some() {
            return Err(LocalError::new(
                LocalErrorKind::ScanBusy,
                "scan run is already active",
            ));
        }
        if self.active_scan_transaction.is_none() {
            self.active_scan_transaction = Some(self.connection.begin().await.into_local_result()?);
            self.defer_scan_publication = false;
            self.staged_scan_status = None;
            self.staged_scan_finished_at_epoch_ms = None;
        }
        let nonce = Uuid::new_v4();
        let record = ActiveScanRecord {
            scan_id: stable_id(
                "scan",
                &format!("{started_at_epoch_ms}\u{0}{}", nonce.simple()),
            ),
            mode: mode.to_owned(),
            started_at_epoch_ms,
        };
        let insert_result = insert_running_scan_run(
            self.active_scan_transaction
                .as_ref()
                .expect("transaction was established above"),
            &record,
        )
        .await;
        if let Err(error) = insert_result {
            if let Some(transaction) = self.active_scan_transaction.take() {
                let _ = transaction.rollback().await;
            }
            self.defer_scan_publication = false;
            self.staged_scan_status = None;
            self.staged_scan_finished_at_epoch_ms = None;
            let _ = write_terminal_scan_run(
                &self.connection,
                &record,
                started_at_epoch_ms,
                "failed",
                false,
                0,
                0,
                0,
            )
            .await;
            return Err(error);
        }
        let scan_id = record.scan_id.clone();
        self.active_scan_record = Some(record);
        self.staged_scan_finished_at_epoch_ms = None;
        Ok(scan_id)
    }

    /// 完成扫描运行记录，所有计数都来自脱敏摘要。
    #[allow(clippy::too_many_arguments)]
    pub async fn finish_scan(
        &mut self,
        scan_id: &str,
        finished_at_epoch_ms: i64,
        status: &str,
        cancelled: bool,
        files_scanned: u64,
        calls_added: u64,
        warning_count: u64,
    ) -> Result<(), LocalError> {
        let files_scanned = to_sql_u64(files_scanned)?;
        let calls_added = to_sql_u64(calls_added)?;
        let warning_count = to_sql_u64(warning_count)?;
        let Some(record) = self.active_scan_record.as_ref() else {
            return Err(LocalError::new(
                LocalErrorKind::Database,
                "scan publication record is not active",
            ));
        };
        if record.scan_id != scan_id {
            return Err(LocalError::new(
                LocalErrorKind::Database,
                "scan publication record does not match",
            ));
        }
        let publish = !cancelled && matches!(status, "completed" | "partial");
        if publish {
            let update_result = update_scan_run_terminal(
                self.active_scan_transaction.as_ref().ok_or_else(|| {
                    LocalError::new(
                        LocalErrorKind::Database,
                        "scan publication transaction is not active",
                    )
                })?,
                scan_id,
                finished_at_epoch_ms,
                status,
                false,
                files_scanned,
                calls_added,
                warning_count,
            )
            .await;
            if let Err(error) = update_result {
                let _ = self
                    .rollback_active_scan_with_terminal(
                        finished_at_epoch_ms,
                        "failed",
                        false,
                        0,
                        0,
                        0,
                    )
                    .await;
                return Err(error);
            }
            self.staged_scan_status = Some(status.to_owned());
            self.staged_scan_finished_at_epoch_ms = Some(finished_at_epoch_ms);
            if self.defer_scan_publication {
                Ok(())
            } else {
                self.publish_scan_attempt().await
            }
        } else {
            self.rollback_active_scan_with_terminal(
                finished_at_epoch_ms,
                status,
                cancelled,
                files_scanned,
                calls_added,
                warning_count,
            )
            .await
        }
    }

    /// 回滚全部暂存变化后在独立短语句中记录终态，避免任何失败遗留 Running。
    #[allow(clippy::too_many_arguments)]
    async fn rollback_active_scan_with_terminal(
        &mut self,
        finished_at_epoch_ms: i64,
        status: &str,
        cancelled: bool,
        files_scanned: i64,
        calls_added: i64,
        warning_count: i64,
    ) -> Result<(), LocalError> {
        let transaction = self.active_scan_transaction.take();
        let record = self.active_scan_record.take();
        self.defer_scan_publication = false;
        self.staged_scan_status = None;
        self.staged_scan_finished_at_epoch_ms = None;
        let rollback_result = match transaction {
            Some(transaction) => transaction.rollback().await.into_local_result(),
            None => Ok(()),
        };
        let terminal_result = match record {
            Some(record) => {
                write_terminal_scan_run(
                    &self.connection,
                    &record,
                    finished_at_epoch_ms,
                    status,
                    cancelled,
                    files_scanned,
                    calls_added,
                    warning_count,
                )
                .await
            }
            None => Ok(()),
        };
        terminal_result?;
        rollback_result
    }

    /// 若扫描体仍持有未发布事务，则整体回滚并把运行记录收口为失败。
    ///
    /// 已经完成正常终态或已在其他路径收口时是无操作；失败计数固定为零，
    /// 因为该尝试的来源、调用与覆盖事实均未发布。
    pub async fn fail_active_scan(
        &mut self,
        scan_id: &str,
        finished_at_epoch_ms: i64,
    ) -> Result<(), LocalError> {
        if self.active_scan_transaction.is_none() {
            return Ok(());
        }
        if self
            .active_scan_record
            .as_ref()
            .is_some_and(|record| record.scan_id != scan_id)
        {
            return Err(LocalError::new(
                LocalErrorKind::Database,
                "scan publication record does not match",
            ));
        }
        self.finish_scan(scan_id, finished_at_epoch_ms, "failed", false, 0, 0, 0)
            .await
    }

    /// 工作流后处理失败时使用当前记录统一回滚并收口失败终态。
    pub async fn fail_scan_attempt(&mut self, finished_at_epoch_ms: i64) -> Result<(), LocalError> {
        if self.active_scan_transaction.is_none() {
            return Ok(());
        }
        if self.active_scan_record.is_none() {
            return self.abort_scan_attempt().await;
        }
        self.rollback_active_scan_with_terminal(finished_at_epoch_ms, "failed", false, 0, 0, 0)
            .await
    }

    /// 工作流级取消可能来自发现覆盖而非 provider；两种来源都整体回滚。
    pub async fn cancel_scan_attempt(
        &mut self,
        finished_at_epoch_ms: i64,
    ) -> Result<(), LocalError> {
        if self.active_scan_transaction.is_none() {
            return Ok(());
        }
        if self.active_scan_record.is_none() {
            return self.abort_scan_attempt().await;
        }
        self.rollback_active_scan_with_terminal(finished_at_epoch_ms, "cancelled", true, 0, 0, 0)
            .await
    }
}
