//! 存储层共用的 SQL 构造小工具；各子模块不得再各自复制。

use sea_orm::{DbBackend, Statement};

/// 参数化语句只由各模块的固定 SQL 产生，用户输入只通过绑定值进入。
pub(crate) fn statement(sql: &str, values: Vec<sea_orm::Value>) -> Statement {
    Statement::from_sql_and_values(DbBackend::Sqlite, sql, values)
}

/// 生成 `?start, ?start+1, ..., ?start+count-1` 形式的占位符列表，供动态长度的
/// `IN (...)` 子句使用；调用方必须按同一顺序绑定对应数量的实际值。
pub(crate) fn in_placeholders(start: usize, count: usize) -> String {
    (start..start + count)
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ")
}
