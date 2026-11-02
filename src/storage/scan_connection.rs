//! 在普通连接与扫描事务之间路由已有参数化 SQL，保持扫描发布前的隔离。

use sea_orm::{
    ConnectionTrait, DatabaseConnection, DatabaseTransaction, DbErr, ExecResult, QueryResult,
    Statement,
};

/// 借用本机索引当前的数据库执行器，不持有或创建新的连接和事务。
pub(crate) enum ScanConnection<'a> {
    /// 扫描外直接使用单连接池。
    Connection(&'a DatabaseConnection),
    /// 扫描内始终使用尚未发布的外层事务。
    Transaction(&'a DatabaseTransaction),
}

impl ScanConnection<'_> {
    /// 把参数化写入送入当前执行器，保留原语句和绑定值的所有权。
    pub(super) async fn execute_raw(&self, statement: Statement) -> Result<ExecResult, DbErr> {
        match self {
            Self::Connection(connection) => connection.execute_raw(statement).await,
            Self::Transaction(transaction) => transaction.execute_raw(statement).await,
        }
    }

    /// 从当前执行器读取至多一行，扫描内能够看见本事务的未发布变更。
    pub(super) async fn query_one_raw(
        &self,
        statement: Statement,
    ) -> Result<Option<QueryResult>, DbErr> {
        match self {
            Self::Connection(connection) => connection.query_one_raw(statement).await,
            Self::Transaction(transaction) => transaction.query_one_raw(statement).await,
        }
    }

    /// 从当前执行器读取完整结果集，不在事务外另开快照连接。
    pub(super) async fn query_all_raw(
        &self,
        statement: Statement,
    ) -> Result<Vec<QueryResult>, DbErr> {
        match self {
            Self::Connection(connection) => connection.query_all_raw(statement).await,
            Self::Transaction(transaction) => transaction.query_all_raw(statement).await,
        }
    }
}
