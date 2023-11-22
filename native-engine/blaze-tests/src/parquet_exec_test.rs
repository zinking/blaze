#[cfg(test)]
mod tests {

    use arrow::datatypes::{DataType, Field, Schema};
    use blaze_jni_bridge::jni_bridge::JavaClasses;
    use datafusion::datasource::listing::{FileRange, PartitionedFile};
    use datafusion::datasource::physical_plan::FileScanConfig;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_ext_plans::parquet_exec::ParquetExec;
    use jni::{InitArgsBuilder, JNIVersion, JavaVM};
    use object_store::ObjectMeta;
    use std::sync::Arc;

    #[test]
    fn test_parquet_exec() {
        let path = "data/sample0.parquet";
        let rsc_id = "fake";
        // Define schema for the data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("data", DataType::Utf8, false),
        ]));

        let partition_file0 = PartitionedFile {
            object_meta: ObjectMeta {
                location: path.into(),
                last_modified: Default::default(),
                size: 813,
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
            table_partition_cols: vec![
                ("id".into(), DataType::Int64),
                ("data".into(), DataType::Utf8),
            ],
            output_ordering: vec![],
            infinite_source: false,
        };

        let jvm_args = InitArgsBuilder::new()
            .version(JNIVersion::V8)
            .option("-Xcheck:jni")
            .option("-Xdebug")
            .build()
            .unwrap();

        let jvm = JavaVM::new(jvm_args).unwrap();
        let env = jvm.get_env().unwrap();
        JavaClasses::init(&env);
        let parquet_exec = ParquetExec::new(scan_config, rsc_id.into(), None);
        let _ = parquet_exec.execute(0, Arc::new(TaskContext::default()));
    }
}
