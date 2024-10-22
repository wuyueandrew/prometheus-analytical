mod table_builder;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::error::ArrowError;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use tokio::time::{self, sleep, Duration};

#[tokio::main]
async fn main() {
    println!("hello");

    let ctx = Arc::new(SessionContext::new());
    let ctxRef = Arc::clone(&ctx);
    let ctxRef2 = Arc::clone(&ctx);

    tokio::spawn(async move {
        println!("-1");
    });

    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(5));
        let ctx = Arc::clone(&ctxRef);
        dummy_table(ctx).await;
        loop {
            interval.tick().await; // This should go first.
            let ctx = Arc::clone(&ctxRef);
            tokio::spawn(async move {
                println!("-1");
            });
        }
    });

    for i in 0..10 {
        sleep(Duration::from_secs(1)).await;
        println!("{}", i);
    }
    ctxRef2
        .sql("select * from dummy ")
        .await
        .unwrap()
        .show()
        .await
        .unwrap();
}

async fn dummy_table(ctx: Arc<SessionContext>) -> Result<(), ArrowError> {
    let session_ctx = SessionContext::new();
    let task_ctx = session_ctx.task_ctx();
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
        Field::new("d", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![4, 5, 6])),
            Arc::new(Int32Array::from(vec![7, 8, 9])),
            Arc::new(Int32Array::from(vec![None, None, Some(9)])),
        ],
    )?;

    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("dummy", Arc::new(provider));
    Ok(())
}
