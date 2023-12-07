#[cfg(test)]
mod tests {
    use std::sync::{Arc, Once};
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use base64::Engine;
    use base64::prelude::BASE64_URL_SAFE_NO_PAD;
    use datafusion::{
        datasource::{
            listing::{FileRange, PartitionedFile},
            physical_plan::FileScanConfig,
        },
        execution::{object_store::ObjectStoreUrl, TaskContext},
        physical_plan::ExecutionPlan,
    };
    use datafusion::common::DataFusionError;
    use datafusion::physical_plan::displayable;
    use futures::{StreamExt, TryStreamExt};
    use datafusion_ext_plans::parquet_exec::ParquetExec;
    use object_store::ObjectMeta;
    use blaze_serde::from_proto::try_parse_physical_expr;
    use crate::jvm_test::init_jvm;
    use crate::sample_data::{sample_eq_filter, sample_task_definition};

    static INIT: Once = Once::new();

    pub fn initialize() {
        INIT.call_once(|| {
            init_jvm();
        });
    }

    #[tokio::test]
    async fn test_task_exec() {
        initialize();
        let task_definition = sample_task_definition();

        let task_id = &task_definition.task_id.expect("task_id is empty");
        let plan = &task_definition.plan.expect("plan is empty");

        // get execution plan
        let execution_plan: Arc<dyn ExecutionPlan> = plan.try_into().map_err(|err| {
            DataFusionError::Plan(format!("cannot create execution plan: {:?}", err))
        }).unwrap();
        let execution_plan_displayable = displayable(execution_plan.as_ref())
            .indent(true)
            .to_string();
        log::info!("Creating native execution plan succeeded");
        log::info!("  task_id={:?}", task_id);
        log::info!("  execution plan:\n{}", execution_plan_displayable);

        let stream = execution_plan.execute(
            0, Arc::new(TaskContext::default())).unwrap();
        let fut = stream.map(|x1| {
            let mbatch = x1.unwrap();
            println!("num rows: {}", mbatch.num_rows());
            mbatch
        }).collect::<Vec<_>>();

        let batches = fut.await;

        let expected_num_batches = 1;
        assert_eq!(batches.len(), expected_num_batches);
    }

    #[tokio::test]
    async fn test_parquet_exec() {
        initialize();
        let path_str = "data/sample0.parquet";
        let path = format!("{}", BASE64_URL_SAFE_NO_PAD.encode(path_str));
        let rsc_id = "fake";
        // Define schema for the data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("data", DataType::Utf8, false),
        ]));
        let schema_clone = schema.clone();
        let partition_file0 = PartitionedFile {
            object_meta: ObjectMeta {
                location: path.into(),
                last_modified: Default::default(),
                size: 817,
                e_tag: None,
            },
            partition_values: vec![],
            range: Some(FileRange { start: 4, end: 817 }),
            extensions: None,
        };

        let file_groups = vec![vec![partition_file0]];
        let scan_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_schema: schema,
            file_groups,
            statistics: Default::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: vec![],
            infinite_source: false,
        };


        let root_node = sample_eq_filter("data", 1, "bc");
        // let root_node = sample_filter();
        let expr = try_parse_physical_expr(&root_node, &schema_clone).unwrap();

        let parquet_exec = ParquetExec::new(
            scan_config, rsc_id.into(), Some(expr));
        let stream = parquet_exec.execute(
            0, Arc::new(TaskContext::default())).unwrap();

        let fut = stream.map(|x1| {
            let mbatch = x1.unwrap();
            println!("num rows: {}", mbatch.num_rows());
            mbatch
        }).collect::<Vec<_>>();

        let batches = fut.await;

        let expected_num_batches = 1;
        assert_eq!(batches.len(), expected_num_batches);

        let expected_batch = RecordBatch::try_new(
            schema_clone,
            vec![
                Arc::new(Int64Array::from(vec![2, 3])),
                Arc::new(StringArray::from(vec!["b", "c"])),
            ],
        ).unwrap();

        assert_eq!(batches[0].schema(), expected_batch.schema());
        for (i, column) in batches[0].columns().iter().enumerate() {
            assert_eq!(column.into_data(), expected_batch.column(i).into_data());
        }
    }
}
