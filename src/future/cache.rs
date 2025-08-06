use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash, RandomState},
    marker::PhantomData,
    ops::Deref,
    sync::Arc,
};

use moka::future::Cache;
use send_sync_static::SSS;
use sqlx::{Database, Encode, Executor, FromRow, IntoArguments, Pool, Type};

use crate::future::builder::{QueryBuilder, RowCacheBuilder};

/// A row-based asynchronous cache that integrates with `sqlx` database pools.
///
/// `RowCache` stores database query results (rows) in memory, backed by a `moka`
/// cache for efficient lookup and eviction. It supports caching both found rows
/// (`Some(V)`) and the absence of rows (`None`) with distinct expiry policies.
///
/// Use `RowCacheBuilder` to construct and customize `RowCache` instances.
///
/// # Type Parameters
/// * `DB`: The `sqlx::Database` type (e.g., `Postgres`, `Sqlite`, `MySql`).
/// * `K`: The type of the key used to query the database and store in the cache.
/// * `V`: The type of the row returned from the database query (must implement `sqlx::FromRow`).
/// * `W`: The type of the value stored in the cache. Defaults to `Arc<V>`.
///        Moka's design requires cached data to be both cheaply clonable and thread-safe
///        for efficient concurrent access across threads. `Arc` is the most common choice to
///        meet these requirements. Providing `W` as a generic parameter allows for further
///        optimizations for scenarios like:
///        1. Callers want to use a more performant smart pointer, such as an `Arc` variant
///           from a specialized crate like `triomphe`.
///        2. If the cached data itself (`V`) is inherently thread-safe and very cheap to clone
///           (e.g., primitive types like `i32` or `f64`), `W` can directly be `V`, avoiding
///           the overhead of a smart pointer.
/// * `S`: The type of the hash builder for the underlying `moka` cache. Defaults to `RandomState`.
pub struct RowCache<DB: Database, K, V, W = Arc<V>, S = RandomState> {
    pub(crate) pool: Pool<DB>,
    pub(crate) query: Box<str>,
    pub(crate) cache: Cache<K, Option<W>, S>,
    pub(crate) _0: PhantomData<(V, S)>,
}

impl<DB, K, V, W> RowCache<DB, K, V, W>
where
    DB: Database,
    K: Clone + Hash + Eq + SSS,
    V: Unpin + SSS,
    W: From<V> + Clone + SSS,
{
    /// Creates a new `RowCache` instance with a specified maximum capacity, database pool,
    /// and a default query for tables with an "id" primary key.
    ///
    /// This is a convenience constructor that delegates to [`RowCacheBuilder::new`].
    ///
    /// # Arguments
    /// * `max_capacity` - The maximum number of entries the cache can hold.
    /// * `pool` - The `sqlx` database connection pool.
    /// * `table` - The name of the database table.
    pub fn new(max_capacity: u64, pool: Pool<DB>, table: &str) -> Self
    where
        DB: QueryBuilder,
    {
        RowCacheBuilder::new(max_capacity, pool, table).build()
    }

    /// Creates a new `RowCache` instance for a specific table and primary key column.
    ///
    /// This is a convenience constructor that delegates to [`RowCacheBuilder::for_table`].
    /// This method constructs a default `SELECT * FROM {table} WHERE {id_column} = {placeholder}`
    /// query.
    ///
    /// # Arguments
    /// * `max_capacity` - The maximum number of entries the cache can hold.
    /// * `pool` - The `sqlx` database connection pool.
    /// * `table` - The name of the database table.
    /// * `id` - The name of the primary key column (e.g., "product_id", "uuid").
    pub fn for_table(max_capacity: u64, pool: Pool<DB>, table: &str, id: &str) -> Self
    where
        DB: QueryBuilder,
    {
        RowCacheBuilder::for_table(max_capacity, pool, table, id).build()
    }

    /// Creates a new `RowCache` instance with a specified maximum capacity, database pool,
    /// and a **custom SQL query**.
    ///
    /// This is a convenience constructor that delegates to [`RowCacheBuilder::for_query`].
    /// The query **must** contain a single placeholder for the key. The specific placeholder
    /// syntax (`?` or `$1`) depends on the database used.
    ///
    /// # Arguments
    /// * `max_capacity` - The maximum number of entries the cache can hold.
    /// * `pool` - The `sqlx` database connection pool.
    /// * `query` - The custom SQL query string. It should contain a single placeholder
    ///   for the key (e.g., `SELECT * FROM users WHERE id = $1` for PostgreSQL,
    ///   or `SELECT * FROM users WHERE id = ?` for MySQL/SQLite).
    pub fn for_query(max_capacity: u64, pool: Pool<DB>, query: impl Into<Box<str>>) -> Self {
        RowCacheBuilder::for_query(max_capacity, pool, query).build()
    }
}

impl<DB, K, V, W, S> RowCache<DB, K, V, W, S>
where
    DB: Database,
    for<'q> DB::Arguments<'q>: IntoArguments<'q, DB>,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    K: Type<DB> + for<'q> Encode<'q, DB> + Hash + Eq + Clone + SSS,
    V: for<'r> FromRow<'r, DB::Row> + Unpin + SSS,
    W: From<V> + Clone + SSS,
    S: BuildHasher + Clone + SSS,
{
    /// Attempts to retrieve a value from the cache using its key.
    ///
    /// If the value is present in the cache, it is returned. If not, the cache
    /// will attempt to fetch the row from the database using the configured query
    /// and then cache the result (either `Some(W)` or `None`) before returning it.
    ///
    /// Returns `Ok(Some(W))` if the row is found and successfully retrieved/fetched.
    /// Returns `Ok(None)` if the row is not found in the database.
    /// Returns `Err(Arc<sqlx::Error>)` if a database error occurs during fetching.
    ///
    /// # Arguments
    /// * `key` - The key to look up in the cache and bind to the database query.
    pub async fn try_get(&self, key: K) -> Result<Option<W>, Arc<sqlx::Error>> {
        self.cache
            .try_get_with(key.clone(), async move {
                sqlx::query_as::<_, V>(self.query.borrow())
                    .bind(key)
                    .fetch_optional(&self.pool)
                    .await
                    .map(|o| o.map(W::from))
            })
            .await
    }

    /// Attempts to retrieve a value from the cache using a reference to its key.
    ///
    /// This method is similar to `try_get`, but allows looking up an entry using
    /// a borrowed form of the key (`&Q`). If the value is not in the cache, the
    /// `Q` will be converted to `K` (using `ToOwned`) for the database query.
    ///
    /// Returns `Ok(Some(W))` if the row is found and successfully retrieved/fetched.
    /// Returns `Ok(None)` if the row is not found in the database.
    /// Returns `Err(Arc<sqlx::Error>)` if a database error occurs during fetching.
    ///
    /// # Arguments
    /// * `key` - A reference to the key to look up. `Q` must be hashable, equatable,
    ///           and convertible to `K`.
    pub async fn try_get_by_ref<Q>(&self, key: &Q) -> Result<Option<W>, Arc<sqlx::Error>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K>,
    {
        self.cache
            .try_get_with_by_ref(key, async move {
                sqlx::query_as::<_, V>(self.query.borrow())
                    .bind(key.to_owned()) // Use key.to_owned() for the database query
                    .fetch_optional(&self.pool)
                    .await
                    .map(|o| o.map(W::from))
            })
            .await
    }
}

impl<DB: Database, K, V, W, S> Deref for RowCache<DB, K, V, W, S> {
    /// Enables `RowCache` to be dereferenced into a `moka::future::Cache`
    /// for direct access to its underlying cache functionalities.
    ///
    /// This allows users to call `moka::future::Cache` methods directly on a
    /// `RowCache` instance (e.g., `row_cache.get(&key)`, `row_cache.invalidate(&key)`).
    type Target = Cache<K, Option<W>, S>;
    fn deref(&self) -> &Self::Target {
        &self.cache
    }
}
