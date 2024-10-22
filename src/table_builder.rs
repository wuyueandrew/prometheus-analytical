use datafusion::arrow::datatypes::{Schema, SchemaRef, Field, DataType};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::array::{ArrayRef, StringArray, Int32Array, ArrayBuilder, Int32Builder, StringBuilder, Float64Builder};
use datafusion::prelude::*;
use std::sync::Arc;
use datafusion::datasource::MemTable;

pub struct TableBuilder {
    table_name: String,
    schema: SchemaRef,
    columns: Vec<Box<dyn ArrayBuilder>>,
}

impl TableBuilder {
    // 创建一个新的 TableBuilder 实例，可以传入初始的 Schema
    pub fn new(table_name: String, fields: Vec<Field>) -> Self {
        let schema = Arc::new(Schema::new(fields));
        let mut columns = Vec::new();

        // 根据字段类型创建相应的 ArrayBuilder
        for field in schema.fields() {
            match field.data_type() {
                DataType::Float64 => columns.push(Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>),
                DataType::Int32 => columns.push(Box::new(Int32Builder::new()) as Box<dyn ArrayBuilder>),
                DataType::Utf8 => columns.push(Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>),
                _ => panic!("Unsupported data type"),
            }
        }

        TableBuilder {
            table_name,
            schema,
            columns,
        }
    }

    // 追加一行数据
    pub fn append_value(&mut self, values: Vec<&dyn Value>) {
        if values.len() != self.columns.len() {
            panic!("Value count does not match schema columns");
        }

        for (i, value) in values.iter().enumerate() {
            value.append_to(&mut self.columns[i]);
        }
    }

    // 构建 RecordBatch 并返回
    pub fn finish(&mut self) -> RecordBatch {
        let mut arrays: Vec<ArrayRef> = Vec::new();

        // 将所有 builder 构建成 Arrow 数组
        for builder in &mut self.columns {
            arrays.push(builder.finish());
        }

        RecordBatch::try_new(self.schema.clone(), arrays).unwrap()
    }

    // 将表注册到 SessionContext
    pub fn register_table(&mut self, ctx: &mut SessionContext) {
        let table = MemTable::try_new(self.schema.clone(), vec![vec![self.finish()]]).unwrap();
        ctx.register_table(self.table_name.clone(), Arc::new(table)).unwrap();
    }
}
pub trait Value {
    fn append_to(&self, builder: &mut Box<dyn ArrayBuilder>);
}

impl Value for f64 {
    fn append_to(&self, builder: &mut Box<dyn ArrayBuilder>) {
        let builder = builder.as_any_mut().downcast_mut::<Float64Builder>().unwrap();
        builder.append_value(*self);
    }
}

impl Value for i32 {
    fn append_to(&self, builder: &mut Box<dyn ArrayBuilder>) {
        let builder = builder.as_any_mut().downcast_mut::<Int32Builder>().unwrap();
        builder.append_value(*self);
    }
}

impl Value for &str {
    fn append_to(&self, builder: &mut Box<dyn ArrayBuilder>) {
        let builder = builder.as_any_mut().downcast_mut::<StringBuilder>().unwrap();
        builder.append_value(*self);
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Float64Builder, Int32Builder};
    use super::*;

    #[tokio::test]
    async fn test_dynamic_builder() {
        // 定义 schema
        let fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("ff", DataType::Float64, false),
        ];

        // 初始化 TableBuilder
        let mut table_builder = TableBuilder::new(String::from("my_table"), fields);

        // 追加行数据
        table_builder.append_value(vec![&1 as &dyn Value, &"Alice" as &dyn Value, &1.0f64 as &dyn Value]);
        table_builder.append_value(vec![&2 as &dyn Value, &"Bob" as &dyn Value, &1.0f64 as &dyn Value]);
        table_builder.append_value(vec![&3 as &dyn Value, &"Charlie" as &dyn Value, &1.0f64 as &dyn Value]);


        // 构建 RecordBatch
        // let record_batch = table_builder.finish();

        // 创建 SessionContext 并注册表
        let mut ctx = SessionContext::new();
        table_builder.register_table(&mut ctx);

        // 查询注册的表
        let df = ctx.sql("SELECT * FROM my_table").await.unwrap();
        df.show().await.unwrap();

    }
}

