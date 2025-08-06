use std::{sync::Arc, time::Duration};

use crate::future::{SqliteCache, SqliteCacheBuilder};
use sqlx::{Pool, Sqlite, prelude::FromRow};
use tokio::time::sleep;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
struct Cake {
    id: i64,
    name: String,
    fruit_id: Option<i64>,
}

impl Cake {
    fn new(id: i64) -> Self {
        Cake {
            id,
            name: "berry delight".into(),
            fruit_id: Some(42),
        }
    }
}

impl PartialEq<Arc<Self>> for Cake {
    fn eq(&self, other: &Arc<Self>) -> bool {
        self.eq(other.as_ref())
    }
}

#[tokio::test]
async fn it_works() -> Result<()> {
    // setting up the database
    let url = "sqlite::memory:";
    let pool = Pool::<Sqlite>::connect(url).await?;
    sqlx::query(
        "CREATE TABLE cakes (
            id INTEGER PRIMARY KEY,
            name VARCHAR(32),
            fruit_id BIGINT
        )",
    )
    .execute(&pool)
    .await?;

    // setting up the dataset
    let cakes = [Cake::new(0), Cake::new(1), Cake::new(2)];
    for cake in cakes.iter().cloned() {
        sqlx::query("INSERT INTO cakes(id, name, fruit_id) VALUES (?, ?, ?)")
            .bind(cake.id)
            .bind(cake.name)
            .bind(cake.fruit_id)
            .execute(&pool)
            .await?;
    }

    // build the cache
    let tti = Duration::from_millis(200);
    let ttl = Duration::from_millis(300);
    let ttl_for_none = Duration::from_millis(100);
    let cache: SqliteCache<i64, Cake> = SqliteCacheBuilder::new(512, pool.clone(), "cakes")
        .time_to_idle(tti)
        .time_to_live(ttl)
        .time_to_live_for_none(ttl_for_none)
        .build();

    // null-value works
    assert_eq!(cache.try_get(-1).await?, None);

    // present value works
    assert_eq!(
        cakes,
        [
            cache.try_get(0).await?.expect("cake[0] is missing."),
            cache.try_get(1).await?.expect("cake[1] is missing."),
            cache.try_get(2).await?.expect("cake[2] is missing.")
        ]
    );

    // null-entries can expire
    assert!(ttl_for_none < tti && ttl_for_none < ttl);
    assert_eq!(cache.get(&-1).await, Some(None));
    sleep(ttl_for_none).await;
    assert_eq!(cache.get(&-1).await, None);

    // TTL/TTI works correctly
    assert!(tti < ttl);
    cache.invalidate_all();
    cache.try_get(0).await?;
    cache.try_get(1).await?;
    sleep(tti / 2).await;
    cache.try_get(1).await?;
    sleep(tti / 2).await;
    assert!(cache.get(&0).await.is_none());
    assert!(cache.get(&1).await.is_some());
    sleep(ttl).await;
    assert!(cache.get(&1).await.is_none());
    Ok(())
}
