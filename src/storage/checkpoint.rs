//! 单个来源文件的安全 checkpoint 读写原语。
//!
//! 增量解析（判断追加/重建、流式读取 JSONL、按行分派进 core 调用模型）
//! 是各 adapter 自己的格式特定逻辑，留在 adapter 内；这里只暴露它们
//! 编排增量索引所需的最小存储原语，任何原始文件正文都不会经过这一层。

use std::collections::HashMap;

use sea_orm::{ConnectionTrait, TransactionTrait};

use super::call_store::{from_sql_u64, to_sql_u64};
use super::{IntoLocalResult, LocalError, LocalIndex, SourceParseCheckpoint, StoredSourceFile};
use crate::SourceFileObservation;

use crate::storage::sql::statement;

/// 单个来源 checkpoint 的完整读取结果（同一 `ready = 1` 行的三类列）。
#[derive(Debug, Clone)]
pub struct SourceCheckpointBundle {
    /// 与 [`LocalIndex::stored_source_file`] 相同的 checkpoint 元数据。
    pub stored: StoredSourceFile,
    /// 与 [`LocalIndex::load_parse_context`] 相同的增量解析上下文。
    pub parse_context: SourceParseCheckpoint,
    /// 与 [`LocalIndex::load_source_warning_counts_json`] 相同的原始警告快照。
    pub warning_counts_json: Option<String>,
}

impl LocalIndex {
    /// 返回一个来源当前生效的安全 checkpoint。
    pub async fn stored_source_file(
        &self,
        source_id: &str,
    ) -> Result<Option<StoredSourceFile>, LocalError> {
        let Some(row) = self
            .scan_connection()
            .query_one_raw(statement(
                "SELECT source_id, root_id, relative_label, file_identity, archived,
                        observed_size, modified_at_epoch_ms, parsed_offset, trailing_bytes,
                        parser_version, generation, token_snapshots_ready
                 FROM source_files WHERE source_id = ?1 AND ready = 1",
                vec![source_id.into()],
            ))
            .await
            .into_local_result()?
        else {
            return Ok(None);
        };
        Ok(Some(decode_stored_source_file(&row)?))
    }

    /// 以单次查询读取一个来源的 checkpoint、警告快照与解析上下文，等价于依次调用
    /// [`Self::stored_source_file`]、[`Self::load_source_warning_counts_json`] 与
    /// [`Self::load_parse_context`]（同一 `ready = 1` 行），把每个文件三次往返压成一次。
    pub async fn load_source_checkpoint_bundle(
        &self,
        source_id: &str,
    ) -> Result<Option<SourceCheckpointBundle>, LocalError> {
        let Some(row) = self
            .scan_connection()
            .query_one_raw(statement(
                "SELECT source_id, root_id, relative_label, file_identity, archived,
                        observed_size, modified_at_epoch_ms, parsed_offset, trailing_bytes,
                        parser_version, generation, token_snapshots_ready,
                        thread_key, project_key, project_label, thread_label,
                        model, reasoning_effort, call_sequence, adapter_state,
                        warning_counts_json
                 FROM source_files WHERE source_id = ?1 AND ready = 1",
                vec![source_id.into()],
            ))
            .await
            .into_local_result()?
        else {
            return Ok(None);
        };
        Ok(Some(SourceCheckpointBundle {
            stored: decode_stored_source_file(&row)?,
            parse_context: decode_parse_context(&row, 12)?,
            warning_counts_json: row.try_get_by_index(20).into_local_result()?,
        }))
    }

    /// 返回某根某区域下全部来源上次记录的大小与修改时间，按相对标签索引，
    /// 供枚举阶段判断窗口外文件是否在两次扫描之间被追加或替换。
    /// 未完成提交的占位行也会返回（大小与时间均为 0），从而必然被再次访问。
    pub async fn source_file_observations_for_root(
        &self,
        root_id: &str,
        archived: bool,
        warning_counts_compatible: impl Fn(&str) -> bool + Send,
    ) -> Result<HashMap<String, SourceFileObservation>, LocalError> {
        let rows = self
            .scan_connection()
            .query_all_raw(statement(
                "SELECT relative_label, observed_size, modified_at_epoch_ms,
                        warning_counts_json
                 FROM source_files WHERE root_id = ?1 AND archived = ?2",
                vec![root_id.into(), archived.into()],
            ))
            .await
            .into_local_result()?;
        let mut observations = HashMap::with_capacity(rows.len());
        for row in rows {
            let relative_label: String = row.try_get_by_index(0).into_local_result()?;
            let warning_counts_json: Option<String> =
                row.try_get_by_index(3).into_local_result()?;
            let warning_counts_ready = warning_counts_json
                .as_deref()
                .is_some_and(&warning_counts_compatible);
            observations.insert(
                relative_label,
                SourceFileObservation {
                    // 旧 schema、损坏或 adapter 不兼容的质量快照都不能证明
                    // 来源未变；用大小哨兵确保窗口外来源也会在下次扫描重建。
                    observed_size: if warning_counts_ready {
                        from_sql_u64(row.try_get_by_index(1).into_local_result()?)?
                    } else {
                        u64::MAX
                    },
                    modified_at_epoch_ms: row.try_get_by_index(2).into_local_result()?,
                },
            );
        }
        Ok(observations)
    }

    /// 恢复增量解析所需的安全上下文；除受控的 project_label/thread_label
    /// 外，不恢复任何原始 session 内容或完整路径。
    pub async fn load_parse_context(
        &self,
        source_id: &str,
    ) -> Result<Option<SourceParseCheckpoint>, LocalError> {
        let Some(row) = self
            .scan_connection()
            .query_one_raw(statement(
                "SELECT thread_key, project_key, project_label, thread_label,
                        model, reasoning_effort, call_sequence, adapter_state
                 FROM source_files WHERE source_id = ?1 AND ready = 1",
                vec![source_id.into()],
            ))
            .await
            .into_local_result()?
        else {
            return Ok(None);
        };
        Ok(Some(decode_parse_context(&row, 0)?))
    }

    /// 读取来源最近一次完整 checkpoint 的脱敏警告分类快照。
    ///
    /// 旧 schema 行和无法解码的历史值都会返回 `None`，adapter 必须据此
    /// 重建来源，而不能把缺失历史质量事实解释成零警告。
    pub async fn load_source_warning_counts_json(
        &self,
        source_id: &str,
    ) -> Result<Option<String>, LocalError> {
        let value = self
            .scan_connection()
            .query_one_raw(statement(
                "SELECT warning_counts_json FROM source_files
                 WHERE source_id = ?1 AND ready = 1",
                vec![source_id.into()],
            ))
            .await
            .into_local_result()?
            .map(|row| row.try_get_by_index(0).into_local_result())
            .transpose()?;
        Ok(value.flatten())
    }

    /// 为一个此前从未见过的来源占位建立 checkpoint 行；已存在时是无操作。
    /// 调用方随后必须以 [`Self::commit_source_file_checkpoint`] 完成写入，
    /// 否则该行会一直停留在 `ready = 0` 的占位状态。
    #[allow(clippy::too_many_arguments)]
    pub async fn begin_source_file_checkpoint(
        &mut self,
        source_id: &str,
        root_id: &str,
        relative_label: &str,
        file_identity: &str,
        archived: bool,
        parser_version: u32,
        thread_key: &str,
        project_key: Option<&str>,
        project_label: Option<&str>,
        thread_label: Option<&str>,
    ) -> Result<(), LocalError> {
        self.validate_root_agent(root_id)?;
        self.scan_connection()
            .execute_raw(statement(
                "INSERT INTO source_files
                 (source_id, root_id, relative_label, file_identity, archived,
                  observed_size, modified_at_epoch_ms, parsed_offset, trailing_bytes,
                  oversized_tail, parser_version, generation, ready,
                  thread_key, project_key, project_label, thread_label,
                  model, reasoning_effort, call_sequence)
                 VALUES (?1, ?2, ?3, ?4, ?5, 0, 0, 0, 0, 0, ?6, 0, 0,
                         ?7, ?8, ?9, ?10, NULL, NULL, 0)
                 ON CONFLICT(source_id) DO NOTHING",
                vec![
                    source_id.into(),
                    root_id.into(),
                    relative_label.into(),
                    file_identity.into(),
                    archived.into(),
                    i64::from(parser_version).into(),
                    thread_key.into(),
                    project_key.map(str::to_owned).into(),
                    project_label.map(str::to_owned).into(),
                    thread_label.map(str::to_owned).into(),
                ],
            ))
            .await
            .into_local_result()?;
        Ok(())
    }

    /// 丢弃某来源超出 `keep_generation` 的调用观察；重建前用于清理未来 generation
    /// 的残留（例如上一次重建被取消，留下比当前 checkpoint 更新的部分数据）。
    pub async fn discard_calls_beyond_generation(
        &mut self,
        source_id: &str,
        keep_generation: u64,
    ) -> Result<(), LocalError> {
        let keep = to_sql_u64(keep_generation)?;
        self.scan_connection()
            .execute_raw(statement(
                "DELETE FROM usage_calls WHERE source_id = ?1 AND generation > ?2",
                vec![source_id.into(), keep.into()],
            ))
            .await
            .into_local_result()?;
        self.scan_connection()
            .execute_raw(statement(
                "DELETE FROM usage_token_snapshots WHERE source_id = ?1 AND generation > ?2",
                vec![source_id.into(), keep.into()],
            ))
            .await
            .into_local_result()?;
        Ok(())
    }

    /// 丢弃某来源指定 generation 的全部调用观察；用于重建被取消时撤销本次
    /// 已经提交的部分写入，让下次扫描从旧 checkpoint 重新开始完整重建。
    pub async fn discard_generation(
        &mut self,
        source_id: &str,
        generation: u64,
    ) -> Result<(), LocalError> {
        let generation = to_sql_u64(generation)?;
        self.scan_connection()
            .execute_raw(statement(
                "DELETE FROM usage_calls WHERE source_id = ?1 AND generation = ?2",
                vec![source_id.into(), generation.into()],
            ))
            .await
            .into_local_result()?;
        self.scan_connection()
            .execute_raw(statement(
                "DELETE FROM usage_token_snapshots WHERE source_id = ?1 AND generation = ?2",
                vec![source_id.into(), generation.into()],
            ))
            .await
            .into_local_result()?;
        Ok(())
    }

    /// 在一个事务中把一次增量索引的最终 checkpoint 写回，并清理该来源其余
    /// generation 的调用（新一代完整写完后才删除旧一代，避免中途状态可见）。
    #[allow(clippy::too_many_arguments)]
    pub async fn commit_source_file_checkpoint(
        &mut self,
        source_id: &str,
        root_id: &str,
        relative_label: &str,
        file_identity: &str,
        archived: bool,
        observed_size: u64,
        modified_at_epoch_ms: i64,
        parsed_offset: u64,
        trailing_bytes: u64,
        oversized_tail: bool,
        parser_version: u32,
        generation: u64,
        checkpoint: &SourceParseCheckpoint,
        warning_counts_json: Option<&str>,
    ) -> Result<(), LocalError> {
        self.validate_root_agent(root_id)?;
        let generation_sql = to_sql_u64(generation)?;
        if let Some(transaction) = &self.active_scan_transaction {
            commit_checkpoint(
                transaction,
                source_id,
                root_id,
                relative_label,
                file_identity,
                archived,
                observed_size,
                modified_at_epoch_ms,
                parsed_offset,
                trailing_bytes,
                oversized_tail,
                parser_version,
                generation_sql,
                checkpoint,
                warning_counts_json,
            )
            .await
        } else {
            let transaction = self.connection.begin().await.into_local_result()?;
            commit_checkpoint(
                &transaction,
                source_id,
                root_id,
                relative_label,
                file_identity,
                archived,
                observed_size,
                modified_at_epoch_ms,
                parsed_offset,
                trailing_bytes,
                oversized_tail,
                parser_version,
                generation_sql,
                checkpoint,
                warning_counts_json,
            )
            .await?;
            transaction.commit().await.into_local_result()
        }
    }
}

/// 在调用方提供的事务边界内提交来源 checkpoint 与 generation 清理。
#[allow(clippy::too_many_arguments)]
async fn commit_checkpoint<C: ConnectionTrait + ?Sized>(
    connection: &C,
    source_id: &str,
    root_id: &str,
    relative_label: &str,
    file_identity: &str,
    archived: bool,
    observed_size: u64,
    modified_at_epoch_ms: i64,
    parsed_offset: u64,
    trailing_bytes: u64,
    oversized_tail: bool,
    parser_version: u32,
    generation_sql: i64,
    checkpoint: &SourceParseCheckpoint,
    warning_counts_json: Option<&str>,
) -> Result<(), LocalError> {
    connection
        .execute_raw(statement(
            "INSERT INTO source_files
                 (source_id, root_id, relative_label, file_identity, archived,
                  observed_size, modified_at_epoch_ms, parsed_offset, trailing_bytes,
                  oversized_tail, parser_version, generation, ready,
                  thread_key, project_key, project_label, thread_label,
                  model, reasoning_effort, call_sequence, token_snapshots_ready,
                  adapter_state, warning_counts_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 1,
                         ?13, ?14, ?15, ?16, ?17, ?18, ?19, 1, ?20, ?21)
                 ON CONFLICT(source_id) DO UPDATE SET
                   root_id = excluded.root_id,
                   relative_label = excluded.relative_label,
                   file_identity = excluded.file_identity,
                   archived = excluded.archived,
                   observed_size = excluded.observed_size,
                   modified_at_epoch_ms = excluded.modified_at_epoch_ms,
                   parsed_offset = excluded.parsed_offset,
                   trailing_bytes = excluded.trailing_bytes,
                   oversized_tail = excluded.oversized_tail,
                   parser_version = excluded.parser_version,
                   generation = excluded.generation,
                   ready = 1,
                   thread_key = excluded.thread_key,
                   project_key = excluded.project_key,
                   project_label = excluded.project_label,
                   thread_label = excluded.thread_label,
                   model = excluded.model,
                   reasoning_effort = excluded.reasoning_effort,
                   call_sequence = excluded.call_sequence,
                   token_snapshots_ready = 1,
                   adapter_state = excluded.adapter_state,
                   warning_counts_json = excluded.warning_counts_json",
            vec![
                source_id.into(),
                root_id.into(),
                relative_label.into(),
                file_identity.into(),
                archived.into(),
                to_sql_u64(observed_size)?.into(),
                modified_at_epoch_ms.into(),
                to_sql_u64(parsed_offset)?.into(),
                to_sql_u64(trailing_bytes)?.into(),
                oversized_tail.into(),
                i64::from(parser_version).into(),
                generation_sql.into(),
                (&checkpoint.thread_key).into(),
                checkpoint.project_key.clone().into(),
                checkpoint.project_label.clone().into(),
                checkpoint.thread_label.clone().into(),
                checkpoint.model.clone().into(),
                checkpoint.reasoning_effort.clone().into(),
                to_sql_u64(checkpoint.call_sequence)?.into(),
                checkpoint.adapter_state.clone().into(),
                warning_counts_json.map(str::to_owned).into(),
            ],
        ))
        .await
        .into_local_result()?;
    connection
        .execute_raw(statement(
            "DELETE FROM usage_calls WHERE source_id = ?1 AND generation <> ?2",
            vec![source_id.into(), generation_sql.into()],
        ))
        .await
        .into_local_result()?;
    connection
        .execute_raw(statement(
            "DELETE FROM usage_token_snapshots WHERE source_id = ?1 AND generation <> ?2",
            vec![source_id.into(), generation_sql.into()],
        ))
        .await
        .into_local_result()?;
    Ok(())
}

/// 从按 `stored_source_file` 列序选取的行解码 checkpoint 元数据。
fn decode_stored_source_file(row: &sea_orm::QueryResult) -> Result<StoredSourceFile, LocalError> {
    Ok(StoredSourceFile {
        source_id: row.try_get_by_index(0).into_local_result()?,
        root_id: row.try_get_by_index(1).into_local_result()?,
        relative_label: row.try_get_by_index(2).into_local_result()?,
        file_identity: row.try_get_by_index(3).into_local_result()?,
        archived: row.try_get_by_index(4).into_local_result()?,
        observed_size: from_sql_u64(row.try_get_by_index(5).into_local_result()?)?,
        modified_at_epoch_ms: row.try_get_by_index(6).into_local_result()?,
        parsed_offset: from_sql_u64(row.try_get_by_index(7).into_local_result()?)?,
        trailing_bytes: from_sql_u64(row.try_get_by_index(8).into_local_result()?)?,
        parser_version: u32::try_from(row.try_get_by_index::<i64>(9).into_local_result()?)
            .map_err(|_| {
                LocalError::new(
                    super::LocalErrorKind::Database,
                    "stored parser version is invalid",
                )
            })?,
        generation: from_sql_u64(row.try_get_by_index(10).into_local_result()?)?,
        token_snapshots_ready: row.try_get_by_index(11).into_local_result()?,
    })
}

/// 从自 `base` 起依次为解析上下文八列的行解码。
fn decode_parse_context(
    row: &sea_orm::QueryResult,
    base: usize,
) -> Result<SourceParseCheckpoint, LocalError> {
    Ok(SourceParseCheckpoint {
        thread_key: row.try_get_by_index(base).into_local_result()?,
        project_key: row.try_get_by_index(base + 1).into_local_result()?,
        project_label: row.try_get_by_index(base + 2).into_local_result()?,
        thread_label: row.try_get_by_index(base + 3).into_local_result()?,
        model: row.try_get_by_index(base + 4).into_local_result()?,
        reasoning_effort: row.try_get_by_index(base + 5).into_local_result()?,
        call_sequence: from_sql_u64(row.try_get_by_index(base + 6).into_local_result()?)?,
        adapter_state: row.try_get_by_index(base + 7).into_local_result()?,
    })
}
