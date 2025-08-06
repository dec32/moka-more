use std::{
    hash::{BuildHasher, Hash},
    marker::PhantomData,
    sync::Arc,
    time::{Duration, Instant},
};

use moka::{
    Expiry,
    future::{Cache, CacheBuilder},
    notification::{ListenerFuture, RemovalCause},
    policy::EvictionPolicy,
};
use send_sync_static::SSS;
use sqlx::{Database, Pool};

use crate::future::cache::RowCache;

/// Defines the capabilities for a database to construct SQL queries.
///
/// This trait provides constants for quoting identifiers and placeholders,
/// which are essential for building database-agnostic SQL queries.
pub trait QueryBuilder {
    /// The character used to quote database identifiers (e.g., table names, column names).
    ///
    /// For example, `"` for PostgreSQL/SQLite, or ```` for MySQL.
    const QUOTE: &str;
    /// The placeholder character used in parameterized queries.
    ///
    /// For example, `?` for SQLite/MySQL, or `$1` for PostgreSQL.
    const PLACEHOLDER: &str;
}

/// A builder for creating and configuring a `RowCache`.
///
/// This struct extends `moka::future::CacheBuilder` to specifically handle
/// caching of database rows. It provides comprehensive options for customizing
/// the cache, including:
///
/// - **Integration with a Database Pool**: Manages fetching data from a `sqlx` database.
/// - **Configurable Query**: Allows specifying the SQL query used to retrieve rows.
/// - **Null Value Caching Strategy**: Offers distinct time-to-live (TTL)
///   settings for `Some(V)` (found rows) and `None` (row not found/cache miss) entries.
///   This enables fine-grained control over how long cache misses are stored.
/// - **Standard Moka Cache Features**: Exposes most other
///   `moka::future::CacheBuilder` functionalities like capacity limits and
///   eviction listeners.
pub struct RowCacheBuilder<DB: Database, K, V, W> {
    inner: CacheBuilder<K, Option<W>, Cache<K, Option<W>>>,
    query: Box<str>,
    pool: Pool<DB>,
    _0: PhantomData<(DB, V)>,
}

impl<DB: Database, K, V, W> RowCacheBuilder<DB, K, V, W>
where
    DB: Database,
    K: Clone + Hash + Eq + SSS,
    V: Unpin + SSS,
    W: From<V> + Clone + SSS,
{
    /// Creates a new `RowCacheBuilder` with a specified maximum capacity, database pool,
    /// and for a table with an "id" primary key.
    ///
    /// This method simplifies cache creation for tables where the primary key column
    /// is named "id". It internally constructs a `SELECT * FROM {table} WHERE id = {placeholder}` query.
    ///
    /// # Arguments
    /// * `max_capacity` - The maximum number of entries the cache can hold.
    /// * `pool` - The SQLx database connection pool.
    /// * `table` - The name of the database table to cache rows from.
    pub fn new(max_capacity: u64, pool: Pool<DB>, table: &str) -> Self
    where
        DB: QueryBuilder,
    {
        Self::for_table(max_capacity, pool, table, "id")
    }

    /// Creates a new `RowCacheBuilder` for a specific table and primary key column.
    ///
    /// This method constructs a default `SELECT * FROM {table} WHERE {id_column} = {placeholder}`
    /// query based on the provided table and ID column names.
    ///
    /// # Arguments
    /// * `max_capacity` - The maximum number of entries the cache can hold.
    /// * `pool` - The SQLx database connection pool.
    /// * `table` - The name of the database table to cache rows from.
    /// * `id` - The name of the primary key column for the table (e.g., "user_id", "product_uuid").
    pub fn for_table(max_capacity: u64, pool: Pool<DB>, table: &str, id: &str) -> Self
    where
        DB: QueryBuilder,
    {
        let query = format!(
            "SELECT * FROM {2}{0}{2} WHERE {2}{1}{2} = {3}",
            table,
            id,
            DB::QUOTE,
            DB::PLACEHOLDER
        );
        Self::for_query(max_capacity, pool, query)
    }

    /// Creates a new `RowCacheBuilder` with a specified maximum capacity, database pool,
    /// and a **custom SQL query**.
    ///
    /// This method allows for full control over the query used to fetch data for the cache.
    /// The query **must** contain a single placeholder for the key, which will be provided
    /// when fetching from the cache. The specific placeholder syntax (`?` or `$1`) depends
    /// on the database type used (e.g., SQLite/MySQL use `?`, PostgreSQL uses `$1`).
    ///
    /// # Arguments
    /// * `max_capacity` - The maximum number of entries the cache can hold.
    /// * `pool` - The SQLx database connection pool.
    /// * `query` - The custom SQL query string. It should contain a single placeholder
    ///   for the key (e.g., `SELECT * FROM users WHERE id = $1` for PostgreSQL,
    ///   or `SELECT * FROM users WHERE id = ?` for MySQL/SQLite).
    pub fn for_query(max_capacity: u64, pool: Pool<DB>, query: impl Into<Box<str>>) -> Self {
        RowCacheBuilder {
            inner: CacheBuilder::new(max_capacity).expire_after(DefaultExpiry::default()),
            query: query.into(),
            pool: pool,
            _0: PhantomData,
        }
    }

    /// Sets the time-to-idle (TTI) expiry for cache entries.
    ///
    /// A cached entry will be expired after the specified duration past from get or insert.
    ///
    /// This expiry applies only to `Some(V)` values.
    ///
    /// # Note on `time_to_live_for_none`
    /// Although this is merely a wrapper around [`moka::future::CacheBuilder::time_to_idle`],
    /// it does not apply to the whole cache but only the `Some(V)` values because the
    /// expiry for `None` values will be over-written by `time_to_live_for_none`.
    ///
    /// # Arguments
    /// * `duration` - The duration after which an entry will expire if idle.
    pub fn time_to_idle(self, duration: Duration) -> Self {
        let mut builder = self;
        builder.inner = builder.inner.time_to_idle(duration);
        builder
    }

    /// Sets the time-to-live (TTL) expiry for cache entries.
    ///
    /// Entries will expire `duration` after they are created.
    /// This expiry applies only to `Some(V)` values.
    ///
    /// # Note on `time_to_live_for_none`
    /// Although this is merely a wrapper around [`moka::future::CacheBuilder::time_to_live`],
    /// it does not apply to the whole cache but only the `Some(V)` values because the
    /// expiry for `None` values will be over-written by `time_to_live_for_none`.
    ///
    /// # Arguments
    /// * `duration` - The duration after which an entry will expire after creation.
    pub fn time_to_live(self, duration: Duration) -> Self {
        let mut builder = self;
        builder.inner = builder.inner.time_to_live(duration);
        builder
    }

    /// Sets the time-to-live (TTL) expiry specifically for `None` values (cache misses).
    ///
    /// When a query returns no result (a `None` value is cached), this duration
    /// specifies how long that `None` entry should remain in the cache before
    /// another attempt is made to query the database.
    ///
    /// # Arguments
    /// * `duration` - The duration for which a `None` entry will be cached.
    pub fn time_to_live_for_none(self, duration: Duration) -> Self {
        let mut builder = self;
        builder.inner = builder.inner.expire_after(DefaultExpiry::new(duration));
        builder
    }

    /// Builds the `RowCache` instance.
    ///
    /// This finalizes the configuration and constructs the `RowCache` ready for use.
    pub fn build(self) -> RowCache<DB, K, V, W> {
        RowCache {
            pool: self.pool,
            query: self.query,
            cache: self.inner.build(),
            _0: PhantomData,
        }
    }

    /// Builds the `RowCache` instance with a custom hash builder.
    ///
    /// This allows specifying a custom hasher for the underlying Moka cache.
    ///
    /// # Arguments
    /// * `hasher` - The custom hash builder to use.
    ///
    /// See [`moka::future::CacheBuilder::build_with_hasher`] for more details.
    pub fn build_with_hasher<S>(self, hasher: S) -> RowCache<DB, K, V, W, S>
    where
        S: BuildHasher + Clone + SSS,
    {
        RowCache {
            pool: self.pool,
            query: self.query,
            cache: self.inner.build_with_hasher(hasher),
            _0: PhantomData,
        }
    }
}

/// Implements the wrapper methods
macro_rules! impl_wrapper {
    (
        $(
            $visibility:vis
            fn $method_name:ident
            (self $(, $arg_name:ident: $arg_type:ty)*)
            $(-> $return_type:ty)?
            ;
        )*
    ) => {
        impl<DB, K, V, W> RowCacheBuilder<DB, K, V, W>
        where
            DB: Database,
            K: Clone + Hash + Eq + SSS,
            V: Unpin + SSS,
            W: From<V> + Clone + SSS,
        {
            $(
                #[doc = concat!(
                    "See [`moka::future::CacheBuilder::",
                    stringify!($method_name),
                    "`]."
                )]
                $visibility fn $method_name
                (self $(, $arg_name: $arg_type)*)
                $(-> $return_type)?
                {
                    let mut builder = self;
                    builder.inner = builder.inner.$method_name($($arg_name),*);
                    builder
                }
            )*
        }
    };
}

impl_wrapper! {
    pub fn name(self, name: &str) -> Self;
    pub fn max_capacity(self, max_capacity: u64) -> Self;
    pub fn initial_capacity(self, number_of_entries: usize) -> Self;
    pub fn eviction_policy(self, policy: EvictionPolicy) -> Self;
    pub fn weigher(
        self,
        weigher: impl Fn(&K, &Option<W>) -> u32 + Send + Sync + 'static
    ) -> Self;
    pub fn eviction_listener(
        self,
        listener: impl Fn(Arc<K>, Option<W>, RemovalCause) + Send + Sync + 'static
    ) -> Self;
    pub fn async_eviction_listener(
        self,
        listener: impl Fn(Arc<K>, Option<W>, RemovalCause) -> ListenerFuture + Send + Sync + 'static
    ) -> Self;
    pub fn expire_after(self, expiry: impl Expiry<K, Option<W>> + SSS) -> Self;
    pub fn support_invalidation_closures(self) -> Self;
}

/// An `Expiry` implementation that provides a general-purpose null-value caching strategy.
/// It gives different TTL values to `Some`s and `None`s (where `None`s usually have a very short TTL),
/// and TTI values only to `Some`s.
#[derive(Clone, Copy)]
struct DefaultExpiry {
    ttl_for_none: Duration,
}

impl DefaultExpiry {
    fn new(ttl_for_none: Duration) -> Self {
        Self { ttl_for_none }
    }
}

impl Default for DefaultExpiry {
    fn default() -> Self {
        Self::new(Duration::from_secs(60))
    }
}

impl<K, W> Expiry<K, Option<W>> for DefaultExpiry {
    fn expire_after_create(
        &self,
        _key: &K,
        value: &Option<W>,
        _created_at: Instant,
    ) -> Option<Duration> {
        match value {
            Some(_) => None,
            None => Some(self.ttl_for_none),
        }
    }
}
