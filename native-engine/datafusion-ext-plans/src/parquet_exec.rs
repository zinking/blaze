// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Execution plan for reading Parquet files

use std::{any::Any, fmt, fmt::Formatter, ops::Range, sync::Arc};

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, SchemaRef},
};
use base64::{prelude::BASE64_URL_SAFE_NO_PAD, Engine};
use blaze_jni_bridge::{
    conf, conf::BooleanConf, jni_call_static, jni_new_global_ref, jni_new_string,
};
use bytes::Bytes;
use datafusion::{
    common::DataFusionError,
    datasource::physical_plan::{
        parquet::{page_filter::PagePruningPredicate, ParquetOpener},
        FileMeta, FileScanConfig, FileStream, OnError, ParquetFileMetrics,
        ParquetFileReaderFactory,
    },
    error::Result,
    execution::context::TaskContext,
    parquet::{
        arrow::async_reader::{fetch_parquet_metadata, AsyncFileReader},
        errors::ParquetError,
        file::metadata::ParquetMetaData,
    },
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{
        expressions::PhysicalSortExpr,
        metrics::{
            BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricValue, MetricsSet, Time,
        },
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Metric, Partitioning, PhysicalExpr,
        RecordBatchStream, SendableRecordBatchStream, Statistics,
    },
};
use datafusion_ext_commons::hadoop_fs::{FsDataInputStream, FsProvider};
use fmt::Debug;
use futures::{future::BoxFuture, stream::once, FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use object_store::ObjectMeta;
use once_cell::sync::OnceCell;

use crate::common::output::TaskOutputter;

#[no_mangle]
fn schema_adapter_cast_column(
    col: &ArrayRef,
    data_type: &DataType,
) -> Result<ArrayRef, DataFusionError> {
    datafusion_ext_commons::cast::cast_scan_input_array(col.as_ref(), data_type)
}

/// Execution plan for scanning one or more Parquet partitions
#[derive(Debug, Clone)]
pub struct ParquetExec {
    fs_resource_id: String,
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    projected_output_ordering: Vec<Vec<PhysicalSortExpr>>,
    metrics: ExecutionPlanMetricsSet,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    pruning_predicate: Option<Arc<PruningPredicate>>,
    page_pruning_predicate: Option<Arc<PagePruningPredicate>>,
}

impl ParquetExec {
    /// Create a new Parquet reader execution plan provided file list and
    /// schema.
    pub fn new(
        base_config: FileScanConfig,
        fs_resource_id: String,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();
        let predicate_creation_errors =
            MetricBuilder::new(&metrics).global_counter("num_predicate_creation_errors");

        let file_schema = &base_config.file_schema;
        let pruning_predicate = predicate
            .clone()
            .and_then(|predicate_expr| {
                match PruningPredicate::try_new(predicate_expr, file_schema.clone()) {
                    Ok(pruning_predicate) => Some(Arc::new(pruning_predicate)),
                    Err(e) => {
                        log::warn!("Could not create pruning predicate: {e}");
                        predicate_creation_errors.add(1);
                        None
                    }
                }
            })
            .filter(|p| !p.allways_true());

        let page_pruning_predicate = predicate.as_ref().and_then(|predicate_expr| {
            match PagePruningPredicate::try_new(predicate_expr, file_schema.clone()) {
                Ok(pruning_predicate) => Some(Arc::new(pruning_predicate)),
                Err(e) => {
                    log::warn!("Could not create page pruning predicate: {}", e);
                    predicate_creation_errors.add(1);
                    None
                }
            }
        });

        let (projected_schema, projected_statistics, projected_output_ordering) =
            base_config.project();

        Self {
            fs_resource_id,
            base_config,
            projected_schema,
            projected_statistics,
            projected_output_ordering,
            metrics,
            predicate,
            pruning_predicate,
            page_pruning_predicate,
        }
    }
}

impl DisplayAs for ParquetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        let limit = self.base_config.limit;
        let file_group = self
            .base_config
            .file_groups
            .iter()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();

        write!(
            f,
            "ParquetExec: limit={:?}, file_group={:?}, predicate={}",
            limit,
            file_group,
            self.pruning_predicate
                .as_ref()
                .map(|pre| format!("{}", pre.predicate_expr()))
                .unwrap_or(format!("<empty>")),
        )
    }
}

impl ExecutionPlan for ParquetExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.projected_output_ordering
            .first()
            .map(|ordering| ordering.as_slice())
    }

    // in datafusion 20.0.0 ExecutionPlan trait not include relies_on_input_order
    // fn relies_on_input_order(&self) -> bool {
    //     false
    // }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition_index: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition_index);
        let _timer = baseline_metrics.elapsed_compute().timer();

        let io_time = Time::default();
        let io_time_metric = Arc::new(Metric::new(
            MetricValue::Time {
                name: "io_time".into(),
                time: io_time.clone(),
            },
            Some(partition_index),
        ));
        self.metrics.register(io_time_metric);

        // get fs object from jni bridge resource
        let resource_id = jni_new_string!(&self.fs_resource_id)?;
        let fs = jni_call_static!(JniBridge.getResource(resource_id.as_obj()) -> JObject)?;
        let fs_provider = Arc::new(FsProvider::new(jni_new_global_ref!(fs.as_obj())?, &io_time));

        let projection = match self.base_config.file_column_projection_indices() {
            Some(proj) => proj,
            None => (0..self.base_config.file_schema.fields().len()).collect(),
        };

        let opener = ParquetOpener {
            partition_index,
            projection: Arc::from(projection),
            batch_size: context.session_config().batch_size(),
            limit: self.base_config.limit,
            predicate: self.predicate.clone(),
            pruning_predicate: self.pruning_predicate.clone(),
            page_pruning_predicate: self.page_pruning_predicate.clone(),
            table_schema: self.base_config.file_schema.clone(),
            metadata_size_hint: None,
            metrics: self.metrics.clone(),
            parquet_file_reader_factory: Arc::new(FsReaderFactory::new(fs_provider)),
            pushdown_filters: false, // still buggy
            reorder_filters: false,
            enable_page_index: false,
        };

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition_index);
        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let mut file_stream =
            FileStream::new(&self.base_config, partition_index, opener, &self.metrics)?;
        if conf::IGNORE_CORRUPTED_FILES.value()? {
            file_stream = file_stream.with_on_error(OnError::Skip);
        }
        let mut stream = Box::pin(file_stream);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(async move {
                context.output_with_sender(
                    "ParquetScan",
                    stream.schema(),
                    move |sender| async move {
                        let mut timer = elapsed_compute.timer();
                        while let Some(batch) = stream.next().await.transpose()? {
                            sender.send(Ok(batch), Some(&mut timer)).await;
                        }
                        Ok(())
                    },
                )
            })
            .try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }
}

#[derive(Clone)]
pub struct FsReaderFactory {
    fs_provider: Arc<FsProvider>,
}

impl FsReaderFactory {
    pub fn new(fs_provider: Arc<FsProvider>) -> Self {
        Self { fs_provider }
    }
}

impl Debug for FsReaderFactory {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "FsReaderFactory")
    }
}

impl ParquetFileReaderFactory for FsReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        _metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        let reader = ParquetFileReaderRef(Arc::new(ParquetFileReader {
            fs_provider: self.fs_provider.clone(),
            input: OnceCell::new(),
            metrics: ParquetFileMetrics::new(
                partition_index,
                file_meta
                    .object_meta
                    .location
                    .filename()
                    .unwrap_or("__default_filename__"),
                metrics,
            ),
            meta: file_meta.object_meta,
        }));
        Ok(Box::new(reader))
    }
}

struct ParquetFileReader {
    fs_provider: Arc<FsProvider>,
    input: OnceCell<Arc<FsDataInputStream>>,
    meta: ObjectMeta,
    metrics: ParquetFileMetrics,
}

#[derive(Clone)]
struct ParquetFileReaderRef(Arc<ParquetFileReader>);

impl ParquetFileReader {
    fn get_input(&self) -> datafusion::parquet::errors::Result<Arc<FsDataInputStream>> {
        let input = self
            .input
            .get_or_try_init(|| {
                let path = BASE64_URL_SAFE_NO_PAD
                    .decode(self.meta.location.filename().expect("missing filename"))
                    .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
                    .map_err(|_| {
                        DataFusionError::Execution(format!(
                            "cannot decode filename: {:?}",
                            self.meta.location.filename()
                        ))
                    })?;
                let fs = self.fs_provider.provide_local(&path)?;
                Ok(Arc::new(fs.open(&path)?))
            })
            .map_err(|e| ParquetError::External(e))?;
        Ok(input.clone())
    }

    fn read_fully(&self, range: Range<usize>) -> Result<Bytes> {
        let mut bytes = vec![0u8; range.len()];
        self.get_input()?
            .read_fully(range.start as u64, &mut bytes)?;
        Ok(Bytes::from(bytes))
    }
}

impl AsyncFileReader for ParquetFileReaderRef {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Bytes>> {
        let inner = self.0.clone();
        inner.metrics.bytes_scanned.add(range.end - range.start);
        async move {
            inner
                .read_fully(range)
                .map_err(|e| ParquetError::External(Box::new(e)))
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Arc<ParquetMetaData>>> {
        let inner = self.0.clone();
        let meta_size = inner.meta.size;
        let size_hint = Some(2097152);
        fetch_parquet_metadata(
            move |range| {
                let inner = inner.clone();
                inner.metrics.bytes_scanned.add(range.end - range.start);
                async move {
                    inner
                        .read_fully(range)
                        .map_err(|e| ParquetError::External(Box::new(e)))
                }
            },
            meta_size,
            size_hint,
        )
        .and_then(|metadata| futures::future::ok(Arc::new(metadata)))
        .boxed()
    }
}
