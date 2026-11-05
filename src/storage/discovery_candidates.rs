//! 在各 Agent 统计库保存发现位置，并原子转为用户确认的数据根。

use super::{
    IntoLocalResult, LocalIndex,
    path_codec::{decode_path, encode_path},
    root_registry::{queue_confirmed_root_on, register_confirmed_root_on},
};
use crate::{
    DiscoveryMethod, LocalError, LocalErrorKind, RootActivationState, RootCandidate,
    RootCandidateEvidence, RootDiscoveryStrategy, SourceClientKind, path_key, stable_id,
};
use sea_orm::{
    ConnectionTrait, DatabaseTransaction, DbBackend, QueryResult, SqliteTransactionMode, Statement,
    TransactionOptions, TransactionTrait,
};
use std::path::{Component, Path};

/// 原子确认结果，不包含原生访问路径。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmedRootCandidate {
    /// 已登记的稳定数据根标识。
    pub root_id: String,
    /// 此次是否首次登记该根。
    pub added: bool,
    /// 确认后真实的激活状态，不自动扫描来源。
    pub activation_state: RootActivationState,
}

/// 构造只带绑定值的候选存储语句。
fn statement(sql: &str, values: Vec<sea_orm::Value>) -> Statement {
    Statement::from_sql_and_values(DbBackend::Sqlite, sql, values)
}

/// 非法或已发生变化的候选只返回固定安全说明。
fn invalid() -> LocalError {
    LocalError::new(
        LocalErrorKind::InvalidUsage,
        "root discovery candidate is invalid or changed",
    )
}

/// 校验候选属于绑定 Agent，身份、路径和证据一致且不读取任何来源内容。
fn validate(candidate: &RootCandidate, client: SourceClientKind) -> Result<(), LocalError> {
    let path = Path::new(&candidate.absolute_path);
    let evidence_matches = matches!(
        (client, candidate.evidence),
        (SourceClientKind::Codex, RootCandidateEvidence::CodexRollout)
            | (
                SourceClientKind::ClaudeCode,
                RootCandidateEvidence::ClaudeTranscript | RootCandidateEvidence::ClaudeSubagent
            )
            | (
                SourceClientKind::GrokBuildCli,
                RootCandidateEvidence::GrokSessionUpdates
            )
            | (
                SourceClientKind::Glm,
                RootCandidateEvidence::GlmModelIoUpdates
            )
    );
    if candidate.client != client
        || !evidence_matches
        || !path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir))
        || candidate.id
            != stable_id(
                client.candidate_namespace(),
                &path_key(&candidate.absolute_path),
            )
    {
        return Err(invalid());
    }
    Ok(())
}

/// 将策略编码为数据库固定标签。
fn strategy_label(value: RootDiscoveryStrategy) -> &'static str {
    match value {
        RootDiscoveryStrategy::WindowsSearch => "windows_search",
        RootDiscoveryStrategy::MacOsSpotlight => "macos_spotlight",
        RootDiscoveryStrategy::MetadataTraversal => "metadata_traversal",
    }
}

/// 将证据编码为数据库固定标签。
fn evidence_label(value: RootCandidateEvidence) -> &'static str {
    match value {
        RootCandidateEvidence::CodexRollout => "codex_rollout",
        RootCandidateEvidence::ClaudeTranscript => "claude_transcript",
        RootCandidateEvidence::ClaudeSubagent => "claude_subagent",
        RootCandidateEvidence::GrokSessionUpdates => "grok_updates",
        RootCandidateEvidence::GlmModelIoUpdates => "glm_model_io",
    }
}

/// 恢复本机候选并拒绝损坏的路径、身份或标签。
fn decode(row: QueryResult, client: SourceClientKind) -> Result<RootCandidate, LocalError> {
    let bytes: Vec<u8> = row.try_get_by_index(1).into_local_result()?;
    let path = decode_path(&bytes).ok_or_else(invalid)?;
    let strategy: String = row.try_get_by_index(2).into_local_result()?;
    let evidence: String = row.try_get_by_index(3).into_local_result()?;
    let candidate = RootCandidate {
        id: row.try_get_by_index(0).into_local_result()?,
        client,
        absolute_path: path,
        strategy: match strategy.as_str() {
            "windows_search" => RootDiscoveryStrategy::WindowsSearch,
            "macos_spotlight" => RootDiscoveryStrategy::MacOsSpotlight,
            "metadata_traversal" => RootDiscoveryStrategy::MetadataTraversal,
            _ => return Err(invalid()),
        },
        evidence: match evidence.as_str() {
            "codex_rollout" => RootCandidateEvidence::CodexRollout,
            "claude_transcript" => RootCandidateEvidence::ClaudeTranscript,
            "claude_subagent" => RootCandidateEvidence::ClaudeSubagent,
            "grok_updates" => RootCandidateEvidence::GrokSessionUpdates,
            "glm_model_io" => RootCandidateEvidence::GlmModelIoUpdates,
            _ => return Err(invalid()),
        },
    };
    validate(&candidate, client)?;
    Ok(candidate)
}

impl LocalIndex {
    /// 候选必须绑定真实 Agent，不能写入未绑定的测试夹具连接。
    fn discovery_client(&self) -> Result<SourceClientKind, LocalError> {
        self.client.ok_or_else(invalid)
    }

    /// 保存或更新发现命中的位置；已登记根不重新成为待确认候选。
    /// # Errors
    /// 路径身份、证据或 Agent 不一致、扫描忙和数据库失败返回稳定错误。
    pub async fn save_discovery_candidate(
        &mut self,
        candidate: &RootCandidate,
    ) -> Result<bool, LocalError> {
        validate(candidate, self.discovery_client()?)?;
        let transaction = self.candidate_transaction().await?;
        let root_id = stable_id(
            candidate.client.root_id_namespace(),
            &path_key(Path::new(&candidate.absolute_path)),
        );
        let registered = transaction
            .query_one_raw(statement(
                "SELECT root_id FROM source_roots WHERE root_id = ?1",
                vec![root_id.into()],
            ))
            .await
            .into_local_result()?
            .is_some();
        if registered {
            transaction
                .execute_raw(statement(
                    "DELETE FROM discovery_candidates WHERE candidate_id = ?1",
                    vec![candidate.id.clone().into()],
                ))
                .await
                .into_local_result()?;
            transaction.commit().await.into_local_result()?;
            return Ok(false);
        }
        transaction.execute_raw(statement("INSERT INTO discovery_candidates(candidate_id, access_path, strategy, evidence) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(candidate_id) DO UPDATE SET strategy=excluded.strategy, evidence=excluded.evidence", vec![candidate.id.clone().into(), encode_path(Path::new(&candidate.absolute_path)).into(), strategy_label(candidate.strategy).into(), evidence_label(candidate.evidence).into()])).await.into_local_result()?;
        transaction.commit().await.into_local_result()?;
        Ok(true)
    }

    /// 读取该 Agent 已落盘的候选位置，绝对路径只供本机原生调用者使用。
    /// # Errors
    /// Agent 未绑定、候选损坏或 SQLite 读取失败时返回稳定错误。
    pub async fn discovery_candidates(&self) -> Result<Vec<RootCandidate>, LocalError> {
        let client = self.discovery_client()?;
        self.scan_connection().query_all_raw(statement("SELECT candidate_id, access_path, strategy, evidence FROM discovery_candidates ORDER BY candidate_id", vec![])).await.into_local_result()?.into_iter().map(|row| decode(row, client)).collect()
    }

    /// 清空该 Agent 尚未确认的全部发现候选，不触碰已登记根和统计；返回清除数量。
    ///
    /// 用于用户明确放弃一轮发现（如跳过初始化扫描）后，避免残留候选被之后的页面自动登记。
    /// # Errors
    /// Agent 未绑定、扫描忙或 SQLite 失败时返回稳定错误，不部分提交。
    pub async fn clear_discovery_candidates(&mut self) -> Result<u64, LocalError> {
        self.discovery_client()?;
        let transaction = self.candidate_transaction().await?;
        let cleared = transaction
            .execute_raw(statement("DELETE FROM discovery_candidates", vec![]))
            .await
            .into_local_result()?
            .rows_affected();
        transaction.commit().await.into_local_result()?;
        Ok(cleared)
    }

    /// 在同一事务中确认当前候选、登记根并移除候选，不触发 Token 扫描。
    ///
    /// 宿主在调用前必须重新验证候选位置仍符合本机来源访问边界。
    /// # Errors
    /// 候选消失或发生变化、Agent 不一致、扫描忙及数据库失败均不部分提交。
    pub async fn confirm_discovery_candidate(
        &mut self,
        candidate: &RootCandidate,
        alias: &str,
    ) -> Result<ConfirmedRootCandidate, LocalError> {
        let client = self.discovery_client()?;
        validate(candidate, client)?;
        let transaction = self.candidate_transaction().await?;
        let row = transaction.query_one_raw(statement("SELECT candidate_id, access_path, strategy, evidence FROM discovery_candidates WHERE candidate_id = ?1", vec![candidate.id.clone().into()])).await.into_local_result()?.ok_or_else(invalid)?;
        if decode(row, client)? != *candidate {
            return Err(invalid());
        }
        let path = Path::new(&candidate.absolute_path);
        let root_id = stable_id(client.root_id_namespace(), &path_key(path));
        let added = register_confirmed_root_on(
            &transaction,
            &root_id,
            path,
            alias,
            DiscoveryMethod::MetadataDiscovery,
        )
        .await?;
        let activation_state = queue_confirmed_root_on(&transaction, &root_id).await?;
        transaction.commit().await.into_local_result()?;
        Ok(ConfirmedRootCandidate {
            root_id,
            added,
            activation_state,
        })
    }

    /// 候选写入先取得 SQLite writer，避免读后写升级，并拒绝嵌套扫描事务。
    async fn candidate_transaction(&self) -> Result<DatabaseTransaction, LocalError> {
        if self.active_scan_transaction.is_some() {
            return Err(LocalError::new(
                LocalErrorKind::ScanBusy,
                "candidate update is unavailable during a scan",
            ));
        }
        self.connection
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..TransactionOptions::default()
            })
            .await
            .into_local_result()
    }
}

#[cfg(test)]
mod glm_tests {
    use super::*;

    /// GLM 发现位置在独占库中真实保存、恢复并确认，拒绝其他 Agent 接管。
    #[tokio::test]
    async fn glm_candidate_persists_and_confirms_in_its_own_database() {
        let temp = tempfile::tempdir().expect("isolated app-data exists");
        let root = temp.path().join("rollout");
        let candidate = RootCandidate {
            id: stable_id(
                SourceClientKind::Glm.candidate_namespace(),
                &path_key(&root),
            ),
            client: SourceClientKind::Glm,
            absolute_path: root.clone(),
            strategy: RootDiscoveryStrategy::MetadataTraversal,
            evidence: RootCandidateEvidence::GlmModelIoUpdates,
        };
        let mut index = LocalIndex::open(temp.path(), Path::new("glm.db"), SourceClientKind::Glm)
            .await
            .expect("GLM database opens");
        assert!(
            index
                .save_discovery_candidate(&candidate)
                .await
                .expect("GLM candidate saves")
        );
        assert_eq!(
            index
                .discovery_candidates()
                .await
                .expect("GLM candidate restores"),
            vec![candidate.clone()]
        );
        let confirmed = index
            .confirm_discovery_candidate(&candidate, "GLM")
            .await
            .expect("GLM candidate confirms");
        assert_eq!(confirmed.root_id, stable_id("glm-root", &path_key(&root)));
        assert!(
            index
                .discovery_candidates()
                .await
                .expect("confirmed candidate is removed")
                .is_empty()
        );
        drop(index);
        assert!(
            LocalIndex::open(temp.path(), Path::new("glm.db"), SourceClientKind::Codex)
                .await
                .is_err()
        );
        assert!(!temp.path().join("codex.db").exists());
    }
}
