//! 定义本机索引不泄露路径或原始记录的稳定错误边界。

use thiserror::Error;

use crate::TokenUsageError;

/// 标识本机索引可以稳定映射的错误类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalErrorKind {
    /// 调用方给出的数据根或 app-data 位置不符合边界要求。
    InvalidPath,
    /// 当前用户没有读取来源或写入本产品索引的权限。
    PermissionDenied,
    /// 只读来源在扫描期间消失或无法打开。
    SourceUnavailable,
    /// 数据库 schema 比当前 adapter 新，必须停止写入。
    UnsupportedSchema,
    /// 数据库初始化、事务或查询失败。
    Database,
    /// Token 字段违反 core 的业务不变量。
    InvalidUsage,
    /// 当前已有一个写扫描，拒绝第二个 writer。
    ScanBusy,
    /// 内部计数或偏移无法安全表示。
    Overflow,
}

/// 包装稳定类别与脱敏说明，不保存绝对路径、JSONL 正文或 SQL 原始语句。
// `message` 类型是 `&'static str`（编译期固定的字符串字面量），而不是
// `String`：这是刻意的设计约束——静态字符串不可能在运行时被拼接进
// 用户目录路径、SQL 语句或文件内容等敏感信息，从类型层面杜绝了“脱敏
// 文案不小心带出隐私数据”的风险。
#[derive(Debug, Error)]
#[error("{message}")]
pub struct LocalError {
    /// 稳定错误类别，供上层映射与展示。
    kind: LocalErrorKind,
    /// 固定文案，不含路径、正文或底层错误。
    message: &'static str,
}

impl LocalError {
    /// 创建一个只含稳定类别与静态脱敏说明的错误。
    pub const fn new(kind: LocalErrorKind, message: &'static str) -> Self {
        Self { kind, message }
    }

    /// 返回供调用方映射的稳定错误类别。
    pub const fn kind(&self) -> LocalErrorKind {
        self.kind
    }
}

impl From<TokenUsageError> for LocalError {
    /// 把非法 Token 事实转换为可计数的业务错误，不保留原始 JSONL。
    fn from(_error: TokenUsageError) -> Self {
        Self::new(LocalErrorKind::InvalidUsage, "local token usage is invalid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证派生 Error 实现只显示静态脱敏说明并保留稳定类别。
    #[test]
    fn keeps_local_error_contract() {
        /// 在编译期断言本机索引错误满足标准错误契约。
        fn assert_standard_error<T: std::error::Error>() {}

        assert_standard_error::<LocalError>();
        let error = LocalError::new(LocalErrorKind::Database, "local index operation failed");
        assert_eq!(error.kind(), LocalErrorKind::Database);
        assert_eq!(error.to_string(), "local index operation failed");
    }
}
