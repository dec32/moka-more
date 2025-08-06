mod builder;
mod cache;
#[cfg(test)]
mod test;

pub use {
    builder::{QueryBuilder, RowCacheBuilder},
    cache::RowCache,
};

#[cfg(feature = "mysql")]
pub use mysql::*;
#[cfg(feature = "mysql")]
mod mysql {
    use crate::future::{QueryBuilder, RowCache, RowCacheBuilder};
    use sqlx::MySql;
    use std::{hash::RandomState, sync::Arc};

    impl QueryBuilder for MySql {
        const QUOTE: &str = "`";
        const PLACEHOLDER: &str = "?";
    }

    pub type MySqlCache<K, V, W = Arc<V>, S = RandomState> = RowCache<MySql, K, V, W, S>;
    pub type MySqlCacheBuilder<K, V, W = Arc<V>> = RowCacheBuilder<MySql, K, V, W>;
}

#[cfg(feature = "postgres")]
pub use postgres::*;
#[cfg(feature = "postgres")]
mod postgres {
    use crate::future::{QueryBuilder, RowCache, RowCacheBuilder};
    use sqlx::Postgres;
    use std::{hash::RandomState, sync::Arc};

    impl QueryBuilder for Postgres {
        const QUOTE: &str = "\"";
        const PLACEHOLDER: &str = "$1";
    }

    pub type PgCache<K, V, W = Arc<V>, S = RandomState> = RowCache<Postgres, K, V, W, S>;
    pub type PgCacheBuilder<K, V, W = Arc<V>> = RowCacheBuilder<Postgres, K, V, W>;
}

#[cfg(feature = "sqlite")]
pub use sqlite::*;
#[cfg(feature = "sqlite")]
mod sqlite {
    use crate::future::{QueryBuilder, RowCache, RowCacheBuilder};
    use sqlx::Sqlite;
    use std::{hash::RandomState, sync::Arc};

    impl QueryBuilder for Sqlite {
        const QUOTE: &str = "\"";
        const PLACEHOLDER: &str = "?";
    }

    pub type SqliteCache<K, V, W = Arc<V>, S = RandomState> = RowCache<Sqlite, K, V, W, S>;
    pub type SqliteCacheBuilder<K, V, W = Arc<V>> = RowCacheBuilder<Sqlite, K, V, W>;
}
