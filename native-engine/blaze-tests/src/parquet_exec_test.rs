#[cfg(test)]
mod tests {
    use std::{env, sync::Arc};
    use std::path::PathBuf;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use base64::Engine;
    use base64::prelude::BASE64_URL_SAFE_NO_PAD;
    use blaze_jni_bridge::jni_bridge::JavaClasses;
    use datafusion::{
        datasource::{
            listing::{FileRange, PartitionedFile},
            physical_plan::FileScanConfig,
        },
        execution::{object_store::ObjectStoreUrl, TaskContext},
        physical_plan::ExecutionPlan,
    };
    use datafusion::common::DataFusionError;
    use datafusion::execution::SendableRecordBatchStream;
    use futures::StreamExt;
    use datafusion_ext_plans::parquet_exec::ParquetExec;
    use jni::{InitArgsBuilder, JNIVersion, JavaVM};
    use object_store::ObjectMeta;
    use tokio::runtime::Runtime;

    #[test]
    fn test_parquet_exec() {
        let pathStr = "file:/Users/zhnwang/zhenw/blaze/native-engine/blaze-tests/data/sample0.parquet";
        let path = format!("{}", BASE64_URL_SAFE_NO_PAD.encode(pathStr));
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
            table_partition_cols: vec![
                ("id".into(), DataType::Int64),
                ("data".into(), DataType::Utf8),
            ],
            output_ordering: vec![],
            infinite_source: false,
        };

        let user_home = env::var("HOME").expect("failed to get home");
        let mvn_home = PathBuf::from(&user_home).join(".m2/repository/");
        let jar_files = [
            "org/blaze/blaze-engine/2.0.7-SNAPSHOT/blaze-engine-2.0.7-SNAPSHOT.jar",
            "org/apache/hadoop/hadoop-client-api/3.3.2/hadoop-client-api-3.3.2.jar",
            "commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar",
            "org/slf4j/slf4j-api/1.7.32/slf4j-api-1.7.32.jar",
            "org/apache/hadoop/hadoop-client-runtime/3.3.2/hadoop-client-runtime-3.3.2.jar",
            "org/apache/spark/spark-core_2.12/3.3.3/spark-core_2.12-3.3.3.jar",
            "org/apache/spark/spark-sql_2.12/3.3.3/spark-sql_2.12-3.3.3.jar",
            "org/apache/logging/log4j/log4j-core/2.17.2/log4j-core-2.17.2.jar",
            "org/apache/spark/spark-catalyst_2.12/3.3.3/spark-catalyst_2.12-3.3.3.jar",
            "com/esotericsoftware/kryo-shaded/4.0.2/kryo-shaded-4.0.2.jar",
            "org/apache/spark/spark-network-common_2.12/3.3.3/spark-network-common_2.12-3.3.3.jar",
            "org/apache/spark/spark-network-shuffle_2.12/3.3.3/spark-network-shuffle_2.12-3.3.3.jar",
            "org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0.jar",
        ];

        let jar_paths: String = jar_files
            .iter()
            .map(|jar_file| mvn_home.join(jar_file).display().to_string())
            .collect::<Vec<String>>()
            .join(":");

        println!("Jar paths: {}", jar_paths);

        let jvm_args = InitArgsBuilder::new()
            .version(JNIVersion::V8)
            .option("-Xcheck:jni")
            .option(&format!("-Djava.class.path={}", jar_paths))
            .build()
            .unwrap();

        let jvm = JavaVM::new(jvm_args).unwrap();
        let _guard = jvm.attach_current_thread().unwrap();
        let env = jvm.get_env().unwrap();
        JavaClasses::init(&env);
        let parquet_exec = ParquetExec::new(scan_config, rsc_id.into(), None);
        let stream = parquet_exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        // verify_stream(stream);
        let rt = Runtime::new().unwrap();

// Run the async function using the runtime
        let result = rt.block_on(verify_stream(stream));

// Handle the result
        match result {
            Ok(_) => println!("Verification succeeded"),
            Err(e) => println!("Verification failed: {:?}", e),
        }
    }

    async fn verify_stream(mut stream: SendableRecordBatchStream) -> arrow::error::Result<()> {
        // Collect all the record batches from the stream
        let results: Vec<Result<RecordBatch, DataFusionError>> = stream.collect().await;
        // Iterate over the record batches
        for result in &results {
            // Print the number of rows in the batch
            let batch = result.as_ref().unwrap();
            println!("Number of rows: {}", batch.num_rows());

            // Iterate over the columns in the batch
            for i in 0..batch.num_columns() {
                // Get the column
                let column = batch.column(i);

                // Print the number of values in the column
                println!("Number of values in column {}: {}", i, column.len());

                // TODO: Add more checks here depending on what you want to verify
            }
        }

        Ok(())
    }
}
