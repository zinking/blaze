// Copyright 2022 The Blaze Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Serde code to convert from protocol buffers to Rust data structures.

use std::{
    convert::{TryFrom, TryInto},
    sync::Arc,
};

use arrow::datatypes::{FieldRef, SchemaRef};
use base64::{prelude::BASE64_URL_SAFE_NO_PAD, Engine};
use chrono::DateTime;
use datafusion::{
    datasource::{
        listing::{FileRange, PartitionedFile},
        object_store::ObjectStoreUrl,
        physical_plan::FileScanConfig,
    },
    error::DataFusionError,
    execution::context::ExecutionProps,
    logical_expr::{BuiltinScalarFunction, Operator},
    physical_expr::{
        expressions::{LikeExpr, SCAndExpr, SCOrExpr},
        functions, ScalarFunctionExpr,
    },
    physical_plan::{
        expressions as phys_expr,
        expressions::{
            BinaryExpr, CaseExpr, CastExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, Literal,
            NegativeExpr, NotExpr, PhysicalSortExpr,
        },
        joins::utils::{ColumnIndex, JoinFilter},
        sorts::sort::SortOptions,
        union::UnionExec,
        ColumnStatistics, ExecutionPlan, Partitioning, PhysicalExpr, Statistics,
    },
};
use datafusion_ext_commons::streams::ipc_stream::IpcReadMode;
use datafusion_ext_exprs::{
    cast::TryCastExpr, get_indexed_field::GetIndexedFieldExpr, get_map_value::GetMapValueExpr,
    named_struct::NamedStructExpr, spark_scalar_subquery_wrapper::SparkScalarSubqueryWrapperExpr,
    spark_udf_wrapper::SparkUDFWrapperExpr, string_contains::StringContainsExpr,
    string_ends_with::StringEndsWithExpr, string_starts_with::StringStartsWithExpr,
};
use datafusion_ext_plans::{
    agg::{create_agg, AggExecMode, AggExpr, AggFunction, AggMode, GroupingExpr},
    agg_exec::AggExec,
    broadcast_join_exec::BroadcastJoinExec,
    broadcast_nested_loop_join_exec::BroadcastNestedLoopJoinExec,
    debug_exec::DebugExec,
    empty_partitions_exec::EmptyPartitionsExec,
    expand_exec::ExpandExec,
    ffi_reader_exec::FFIReaderExec,
    filter_exec::FilterExec,
    generate::create_generator,
    generate_exec::GenerateExec,
    ipc_reader_exec::IpcReaderExec,
    ipc_writer_exec::IpcWriterExec,
    limit_exec::LimitExec,
    parquet_exec::ParquetExec,
    parquet_sink_exec::ParquetSinkExec,
    project_exec::ProjectExec,
    rename_columns_exec::RenameColumnsExec,
    rss_shuffle_writer_exec::RssShuffleWriterExec,
    shuffle_writer_exec::ShuffleWriterExec,
    sort_exec::SortExec,
    sort_merge_join_exec::SortMergeJoinExec,
    window::{WindowExpr, WindowFunction, WindowRankType},
    window_exec::WindowExec,
};
use object_store::{path::Path, ObjectMeta};

use crate::{
    convert_box_required, convert_required,
    error::PlanSerDeError,
    from_proto_binary_op, into_required, proto_error, protobuf,
    protobuf::{
        physical_expr_node::ExprType, physical_plan_node::PhysicalPlanType, GenerateFunction,
    },
    Schema,
};

fn bind(
    expr_in: Arc<dyn PhysicalExpr>,
    input_schema: &Arc<Schema>,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let expr = expr_in.as_any();

    if let Some(expr) = expr.downcast_ref::<Column>() {
        if expr.name() == "__bound_reference__" {
            Ok(Arc::new(expr.clone()))
        } else {
            Ok(Arc::new(Column::new_with_schema(
                expr.name(),
                input_schema,
            )?))
        }
    } else {
        let new_children = expr_in
            .children()
            .iter()
            .map(|child_expr| bind(child_expr.clone(), input_schema))
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        Ok(expr_in.with_new_children(new_children)?)
    }
}

impl TryInto<Arc<dyn ExecutionPlan>> for &protobuf::PhysicalPlanNode {
    type Error = PlanSerDeError;

    fn try_into(self) -> Result<Arc<dyn ExecutionPlan>, Self::Error> {
        let plan = self.physical_plan_type.as_ref().ok_or_else(|| {
            proto_error(format!(
                "physical_plan::from_proto() Unsupported physical plan '{:?}'",
                self
            ))
        })?;
        match plan {
            PhysicalPlanType::Projection(projection) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(projection.input)?;
                let exprs = projection
                    .expr
                    .iter()
                    .zip(projection.expr_name.iter())
                    .map(|(expr, name)| {
                        Ok((
                            bind(
                                try_parse_physical_expr(expr, &input.schema())?,
                                &input.schema(),
                            )?,
                            name.to_string(),
                        ))
                    })
                    .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, String)>, Self::Error>>()?;
                Ok(Arc::new(ProjectExec::try_new(exprs, input)?))
            }
            PhysicalPlanType::Filter(filter) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(filter.input)?;
                let predicates = filter
                    .expr
                    .iter()
                    .map(|expr| {
                        Ok(bind(
                            try_parse_physical_expr(expr, &input.schema())?,
                            &input.schema(),
                        )?)
                    })
                    .collect::<Result<_, Self::Error>>()?;
                Ok(Arc::new(FilterExec::try_new(predicates, input)?))
            }
            PhysicalPlanType::ParquetScan(scan) => {
                let conf: FileScanConfig = scan.base_conf.as_ref().unwrap().try_into()?;
                let predicate = scan
                    .pruning_predicates
                    .iter()
                    .filter_map(|predicate| {
                        try_parse_physical_expr(predicate, &conf.file_schema).ok()
                    })
                    .fold(phys_expr::lit(true), |a, b| {
                        Arc::new(BinaryExpr::new(a, Operator::And, b))
                    });
                Ok(Arc::new(ParquetExec::new(
                    conf,
                    scan.fs_resource_id.clone(),
                    Some(predicate),
                )))
            }
            PhysicalPlanType::SortMergeJoin(sort_merge_join) => {
                let left: Arc<dyn ExecutionPlan> = convert_box_required!(sort_merge_join.left)?;
                let right: Arc<dyn ExecutionPlan> = convert_box_required!(sort_merge_join.right)?;
                let on: Vec<(Column, Column)> = sort_merge_join
                    .on
                    .iter()
                    .map(|col| {
                        let left_col: Column = into_required!(col.left)?;
                        let left_col_binded: Column =
                            Column::new_with_schema(left_col.name(), &left.schema())?;
                        let right_col: Column = into_required!(col.right)?;
                        let right_col_binded: Column =
                            Column::new_with_schema(right_col.name(), &right.schema())?;
                        Ok((left_col_binded, right_col_binded))
                    })
                    .collect::<Result<_, Self::Error>>()?;

                let sort_options = sort_merge_join
                    .sort_options
                    .iter()
                    .map(|sort_options| SortOptions {
                        descending: !sort_options.asc,
                        nulls_first: sort_options.nulls_first,
                    })
                    .collect::<Vec<_>>();

                let join_type = protobuf::JoinType::from_i32(sort_merge_join.join_type)
                    .ok_or_else(|| {
                        proto_error(format!(
                            "Received a SortMergeJoinNode message with unknown JoinType {}",
                            sort_merge_join.join_type
                        ))
                    })?;

                let join_filter = sort_merge_join
                    .join_filter
                    .as_ref()
                    .map(|f| {
                        let schema = Arc::new(convert_required!(f.schema)?);
                        let expression = try_parse_physical_expr_required(&f.expression, &schema)?;
                        let column_indices = f
                            .column_indices
                            .iter()
                            .map(|i| {
                                let side =
                                    protobuf::JoinSide::from_i32(i.side).expect("invalid JoinSide");
                                Ok(ColumnIndex {
                                    index: i.index as usize,
                                    side: side.into(),
                                })
                            })
                            .collect::<Result<Vec<_>, PlanSerDeError>>()?;

                        Ok(JoinFilter::new(
                            bind(expression, &schema)?,
                            column_indices,
                            schema.as_ref().clone(),
                        ))
                    })
                    .map_or(Ok(None), |v: Result<_, PlanSerDeError>| v.map(Some))?;
                Ok(Arc::new(SortMergeJoinExec::try_new(
                    left,
                    right,
                    on,
                    join_type.into(),
                    join_filter,
                    sort_options,
                )?))
            }
            PhysicalPlanType::ShuffleWriter(shuffle_writer) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(shuffle_writer.input)?;

                let output_partitioning = parse_protobuf_hash_partitioning(
                    input.clone(),
                    shuffle_writer.output_partitioning.as_ref(),
                )?;

                Ok(Arc::new(ShuffleWriterExec::try_new(
                    input,
                    output_partitioning.unwrap(),
                    shuffle_writer.output_data_file.clone(),
                    shuffle_writer.output_index_file.clone(),
                )?))
            }
            PhysicalPlanType::RssShuffleWriter(rss_shuffle_writer) => {
                let input: Arc<dyn ExecutionPlan> =
                    convert_box_required!(rss_shuffle_writer.input)?;

                let output_partitioning = parse_protobuf_hash_partitioning(
                    input.clone(),
                    rss_shuffle_writer.output_partitioning.as_ref(),
                )?;
                Ok(Arc::new(RssShuffleWriterExec::try_new(
                    input,
                    output_partitioning.unwrap(),
                    rss_shuffle_writer.rss_partition_writer_resource_id.clone(),
                )?))
            }
            PhysicalPlanType::IpcWriter(ipc_writer) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(ipc_writer.input)?;

                Ok(Arc::new(IpcWriterExec::new(
                    input,
                    ipc_writer.ipc_consumer_resource_id.clone(),
                )))
            }
            PhysicalPlanType::IpcReader(ipc_reader) => {
                let schema = Arc::new(convert_required!(ipc_reader.schema)?);
                let mode = match protobuf::IpcReadMode::from_i32(ipc_reader.mode).unwrap() {
                    protobuf::IpcReadMode::ChannelUncompressed => IpcReadMode::ChannelUncompressed,
                    protobuf::IpcReadMode::Channel => IpcReadMode::Channel,
                    protobuf::IpcReadMode::ChannelAndFileSegment => {
                        IpcReadMode::ChannelAndFileSegment
                    }
                };
                Ok(Arc::new(IpcReaderExec::new(
                    ipc_reader.num_partitions as usize,
                    ipc_reader.ipc_provider_resource_id.clone(),
                    schema,
                    mode,
                )))
            }
            PhysicalPlanType::Debug(debug) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(debug.input)?;
                Ok(Arc::new(DebugExec::new(input, debug.debug_id.clone())))
            }
            PhysicalPlanType::Sort(sort) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(sort.input)?;
                let exprs = sort
                    .expr
                    .iter()
                    .map(|expr| {
                        let expr = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error(format!(
                                "physical_plan::from_proto() Unexpected expr {:?}",
                                self
                            ))
                        })?;
                        if let protobuf::physical_expr_node::ExprType::Sort(sort_expr) = expr {
                            let expr = sort_expr
                                .expr
                                .as_ref()
                                .ok_or_else(|| {
                                    proto_error(format!(
                                        "physical_plan::from_proto() Unexpected sort expr {:?}",
                                        self
                                    ))
                                })?
                                .as_ref();
                            Ok(PhysicalSortExpr {
                                expr: bind(
                                    try_parse_physical_expr(expr, &input.schema())?,
                                    &input.schema(),
                                )?,
                                options: SortOptions {
                                    descending: !sort_expr.asc,
                                    nulls_first: sort_expr.nulls_first,
                                },
                            })
                        } else {
                            Err(PlanSerDeError::General(format!(
                                "physical_plan::from_proto() {:?}",
                                self
                            )))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                // always preserve partitioning
                Ok(Arc::new(SortExec::new(
                    input,
                    exprs,
                    sort.fetch_limit.map(|limit| limit as usize),
                )))
            }
            PhysicalPlanType::BroadcastJoin(broadcast_join) => {
                let left: Arc<dyn ExecutionPlan> = convert_box_required!(broadcast_join.left)?;
                let right: Arc<dyn ExecutionPlan> = convert_box_required!(broadcast_join.right)?;
                let on: Vec<(Column, Column)> = broadcast_join
                    .on
                    .iter()
                    .map(|col| {
                        let left_col: Column = into_required!(col.left)?;
                        let left_col_binded: Column =
                            Column::new_with_schema(left_col.name(), &left.schema())?;
                        let right_col: Column = into_required!(col.right)?;
                        let right_col_binded: Column =
                            Column::new_with_schema(right_col.name(), &right.schema())?;
                        Ok((left_col_binded, right_col_binded))
                    })
                    .collect::<Result<_, Self::Error>>()?;

                let join_type =
                    protobuf::JoinType::from_i32(broadcast_join.join_type).ok_or_else(|| {
                        proto_error(format!(
                            "Received a BroadcastJoinNode message with unknown JoinType {}",
                            broadcast_join.join_type
                        ))
                    })?;

                let join_filter = broadcast_join
                    .join_filter
                    .as_ref()
                    .map(|f| {
                        let schema = Arc::new(convert_required!(f.schema)?);
                        let expression = try_parse_physical_expr_required(&f.expression, &schema)?;
                        let column_indices = f
                            .column_indices
                            .iter()
                            .map(|i| {
                                let side =
                                    protobuf::JoinSide::from_i32(i.side).expect("invalid JoinSide");
                                Ok(ColumnIndex {
                                    index: i.index as usize,
                                    side: side.into(),
                                })
                            })
                            .collect::<Result<Vec<_>, PlanSerDeError>>()?;

                        Ok(JoinFilter::new(
                            bind(expression, &schema)?,
                            column_indices,
                            schema.as_ref().clone(),
                        ))
                    })
                    .map_or(Ok(None), |v: Result<_, PlanSerDeError>| v.map(Some))?;

                Ok(Arc::new(BroadcastJoinExec::try_new(
                    left,
                    right,
                    on,
                    join_type.into(),
                    join_filter,
                )?))
            }
            PhysicalPlanType::BroadcastNestedLoopJoin(bnlj) => {
                let left: Arc<dyn ExecutionPlan> = convert_box_required!(bnlj.left)?;
                let right: Arc<dyn ExecutionPlan> = convert_box_required!(bnlj.right)?;
                let join_type = protobuf::JoinType::from_i32(bnlj.join_type).ok_or_else(|| {
                    proto_error(format!(
                        "Received a BroadcastNestedLoopJoinNode message with unknown JoinType {}",
                        bnlj.join_type
                    ))
                })?;
                let join_filter = bnlj
                    .join_filter
                    .as_ref()
                    .map(|f| {
                        let schema = Arc::new(convert_required!(f.schema)?);
                        let expression = try_parse_physical_expr_required(&f.expression, &schema)?;
                        let column_indices = f
                            .column_indices
                            .iter()
                            .map(|i| {
                                let side =
                                    protobuf::JoinSide::from_i32(i.side).expect("invalid JoinSide");
                                Ok(ColumnIndex {
                                    index: i.index as usize,
                                    side: side.into(),
                                })
                            })
                            .collect::<Result<Vec<_>, PlanSerDeError>>()?;

                        Ok(JoinFilter::new(
                            bind(expression, &schema)?,
                            column_indices,
                            schema.as_ref().clone(),
                        ))
                    })
                    .map_or(Ok(None), |v: Result<_, PlanSerDeError>| v.map(Some))?;

                Ok(Arc::new(BroadcastNestedLoopJoinExec::try_new(
                    left,
                    right,
                    join_type.into(),
                    join_filter,
                )?))
            }
            PhysicalPlanType::Union(union) => {
                let inputs: Vec<Arc<dyn ExecutionPlan>> = union
                    .children
                    .iter()
                    .map(|i| i.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(UnionExec::new(inputs)))
            }
            PhysicalPlanType::EmptyPartitions(empty_partitions) => {
                let schema = Arc::new(convert_required!(empty_partitions.schema)?);
                Ok(Arc::new(EmptyPartitionsExec::new(
                    schema,
                    empty_partitions.num_partitions as usize,
                )))
            }
            PhysicalPlanType::RenameColumns(rename_columns) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(rename_columns.input)?;
                Ok(Arc::new(RenameColumnsExec::try_new(
                    input,
                    rename_columns.renamed_column_names.clone(),
                )?))
            }
            PhysicalPlanType::Agg(agg) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(agg.input)?;
                let input_schema = input.schema();

                let exec_mode = protobuf::AggExecMode::from_i32(agg.exec_mode)
                    .ok_or_else(|| proto_error(format!("invalid AggExecMode {}", agg.exec_mode)))
                    .map(|exec_mode| match exec_mode {
                        protobuf::AggExecMode::HashAgg => AggExecMode::HashAgg,
                        protobuf::AggExecMode::SortAgg => AggExecMode::SortAgg,
                    })?;

                let agg_modes = agg
                    .mode
                    .iter()
                    .map(|&mode| {
                        protobuf::AggMode::from_i32(mode)
                            .ok_or_else(|| proto_error(format!("invalid AggMode {}", mode)))
                            .map(|mode| match mode {
                                protobuf::AggMode::Partial => AggMode::Partial,
                                protobuf::AggMode::PartialMerge => AggMode::PartialMerge,
                                protobuf::AggMode::Final => AggMode::Final,
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let physical_groupings: Vec<GroupingExpr> = agg
                    .grouping_expr
                    .iter()
                    .zip(agg.grouping_expr_name.iter())
                    .map(|(expr, name)| {
                        try_parse_physical_expr(expr, &input_schema).and_then(|expr| {
                            Ok(bind(expr, &input_schema).map(|expr| GroupingExpr {
                                expr,
                                field_name: name.to_owned(),
                            })?)
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let physical_aggs: Vec<AggExpr> = agg
                    .agg_expr
                    .iter()
                    .zip(&agg.agg_expr_name)
                    .zip(&agg_modes)
                    .map(|((expr, name), &mode)| {
                        let expr_type = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error("Unexpected empty aggregate physical expression")
                        })?;

                        let agg_node = match expr_type {
                            ExprType::AggExpr(agg_node) => agg_node,
                            _ => {
                                return Err(PlanSerDeError::General(
                                    "Invalid aggregate expression for AggExec".to_string(),
                                ));
                            }
                        };

                        let agg_function = protobuf::AggFunction::from_i32(agg_node.agg_function)
                            .ok_or_else(|| {
                            proto_error(format!(
                                "Received an unknown aggregate function: {}",
                                agg_node.agg_function
                            ))
                        })?;
                        let agg_children_exprs = agg_node
                            .children
                            .iter()
                            .map(|expr| {
                                try_parse_physical_expr(expr, &input_schema)
                                    .and_then(|expr| Ok(bind(expr, &input_schema)?))
                            })
                            .collect::<Result<Vec<_>, _>>()?;

                        Ok(AggExpr {
                            agg: create_agg(
                                AggFunction::from(agg_function),
                                &agg_children_exprs,
                                &input_schema,
                            )?,
                            mode,
                            field_name: name.to_owned(),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(AggExec::try_new(
                    exec_mode,
                    physical_groupings,
                    physical_aggs,
                    agg.initial_input_buffer_offset as usize,
                    agg.supports_partial_skipping,
                    input,
                )?))
            }
            PhysicalPlanType::Limit(limit) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(limit.input)?;
                Ok(Arc::new(LimitExec::new(input, limit.limit)))
            }
            PhysicalPlanType::FfiReader(ffi_reader) => {
                let schema = Arc::new(convert_required!(ffi_reader.schema)?);
                Ok(Arc::new(FFIReaderExec::new(
                    ffi_reader.num_partitions as usize,
                    ffi_reader.export_iter_provider_resource_id.clone(),
                    schema,
                )))
            }
            PhysicalPlanType::CoalesceBatches(coalesce_batches) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(coalesce_batches.input)?;
                Ok(Arc::new(LimitExec::new(input, coalesce_batches.batch_size)))
            }
            PhysicalPlanType::Expand(expand) => {
                let schema = Arc::new(convert_required!(expand.schema)?);
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(expand.input)?;
                let projections = expand
                    .projections
                    .iter()
                    .map(|projection| {
                        projection
                            .expr
                            .iter()
                            .map(|expr| {
                                Ok(bind(
                                    try_parse_physical_expr(expr, &input.schema())?,
                                    &input.schema(),
                                )?)
                            })
                            .collect::<Result<Vec<_>, Self::Error>>()
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(ExpandExec::try_new(schema, projections, input)?))
            }
            PhysicalPlanType::Window(window) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(window.input)?;
                let window_exprs = window
                    .window_expr
                    .iter()
                    .map(|w| {
                        let field: FieldRef = Arc::new(
                            w.field
                                .as_ref()
                                .ok_or_else(|| {
                                    proto_error(format!(
                                        "physical_plan::from_proto() Unexpected sort expr {:?}",
                                        self
                                    ))
                                })?
                                .try_into()?,
                        );

                        let children = w
                            .children
                            .iter()
                            .map(|expr| {
                                Ok(bind(
                                    try_parse_physical_expr(expr, &input.schema())?,
                                    &input.schema(),
                                )?)
                            })
                            .collect::<Result<Vec<_>, Self::Error>>()?;

                        let window_func = match w.func_type() {
                            protobuf::WindowFunctionType::Window => match w.window_func() {
                                protobuf::WindowFunction::RowNumber => {
                                    WindowFunction::RankLike(WindowRankType::RowNumber)
                                }
                                protobuf::WindowFunction::Rank => {
                                    WindowFunction::RankLike(WindowRankType::Rank)
                                }
                                protobuf::WindowFunction::DenseRank => {
                                    WindowFunction::RankLike(WindowRankType::DenseRank)
                                }
                            },
                            protobuf::WindowFunctionType::Agg => match w.agg_func() {
                                protobuf::AggFunction::Min => WindowFunction::Agg(AggFunction::Min),
                                protobuf::AggFunction::Max => WindowFunction::Agg(AggFunction::Max),
                                protobuf::AggFunction::Sum => WindowFunction::Agg(AggFunction::Sum),
                                protobuf::AggFunction::Avg => WindowFunction::Agg(AggFunction::Avg),
                                protobuf::AggFunction::Count => {
                                    WindowFunction::Agg(AggFunction::Count)
                                }
                                protobuf::AggFunction::CollectList => {
                                    WindowFunction::Agg(AggFunction::CollectList)
                                }
                                protobuf::AggFunction::CollectSet => {
                                    WindowFunction::Agg(AggFunction::CollectSet)
                                }
                                protobuf::AggFunction::First => {
                                    WindowFunction::Agg(AggFunction::First)
                                }
                                protobuf::AggFunction::FirstIgnoresNull => {
                                    WindowFunction::Agg(AggFunction::FirstIgnoresNull)
                                }
                            },
                        };
                        Ok::<_, Self::Error>(WindowExpr::new(window_func, children, field))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let partition_specs = window
                    .partition_spec
                    .iter()
                    .map(|expr| {
                        Ok(bind(
                            try_parse_physical_expr(expr, &input.schema())?,
                            &input.schema(),
                        )?)
                    })
                    .collect::<Result<Vec<_>, Self::Error>>()?;

                let order_specs = window
                    .order_spec
                    .iter()
                    .map(|expr| {
                        let expr = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error(format!(
                                "physical_plan::from_proto() Unexpected expr {:?}",
                                self
                            ))
                        })?;
                        if let protobuf::physical_expr_node::ExprType::Sort(sort_expr) = expr {
                            let expr = sort_expr
                                .expr
                                .as_ref()
                                .ok_or_else(|| {
                                    proto_error(format!(
                                        "physical_plan::from_proto() Unexpected sort expr {:?}",
                                        self
                                    ))
                                })?
                                .as_ref();
                            Ok(PhysicalSortExpr {
                                expr: bind(
                                    try_parse_physical_expr(expr, &input.schema())?,
                                    &input.schema(),
                                )?,
                                options: SortOptions {
                                    descending: !sort_expr.asc,
                                    nulls_first: sort_expr.nulls_first,
                                },
                            })
                        } else {
                            Err(PlanSerDeError::General(format!(
                                "physical_plan::from_proto() {:?}",
                                self
                            )))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(WindowExec::try_new(
                    input,
                    window_exprs,
                    partition_specs,
                    order_specs,
                )?))
            }
            PhysicalPlanType::Generate(generate) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(generate.input)?;
                let input_schema = input.schema();
                let pb_generator = generate.generator.as_ref().expect("missing generator");
                let pb_generator_children = &pb_generator.child;
                let pb_generate_func = GenerateFunction::from_i32(pb_generator.func)
                    .expect("unsupported generate function");

                let func = match pb_generate_func {
                    GenerateFunction::Explode => {
                        datafusion_ext_plans::generate::GenerateFunc::Explode
                    }
                    GenerateFunction::PosExplode => {
                        datafusion_ext_plans::generate::GenerateFunc::PosExplode
                    }
                };
                let children = pb_generator_children
                    .iter()
                    .map(|expr| {
                        Ok::<_, PlanSerDeError>(bind(
                            try_parse_physical_expr(expr, &input_schema)?,
                            &input_schema,
                        )?)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let generator = create_generator(&input_schema, func, children)?;
                let generator_output_schema = Arc::new(Schema::new(
                    generate
                        .generator_output
                        .iter()
                        .map(|field| Ok(Arc::new(field.try_into()?)))
                        .collect::<Result<Vec<FieldRef>, PlanSerDeError>>()?,
                ));

                let required_child_output_cols = generate
                    .required_child_output
                    .iter()
                    .map(|name| Ok(Column::new_with_schema(name, &input_schema)?))
                    .collect::<Result<_, PlanSerDeError>>()?;

                Ok(Arc::new(GenerateExec::try_new(
                    input,
                    generator,
                    required_child_output_cols,
                    generator_output_schema,
                    generate.outer,
                )?))
            }
            PhysicalPlanType::ParquetSink(parquet_sink) => {
                let mut props: Vec<(String, String)> = vec![];
                for prop in &parquet_sink.prop {
                    props.push((prop.key.clone(), prop.value.clone()));
                }
                Ok(Arc::new(ParquetSinkExec::new(
                    convert_box_required!(parquet_sink.input)?,
                    parquet_sink.fs_resource_id.clone(),
                    parquet_sink.path.clone(),
                    props,
                )))
            }
        }
    }
}

impl From<&protobuf::PhysicalColumn> for Column {
    fn from(c: &protobuf::PhysicalColumn) -> Column {
        Column::new(&c.name, c.index as usize)
    }
}

impl From<&protobuf::BoundReference> for Column {
    fn from(c: &protobuf::BoundReference) -> Column {
        Column::new("__bound_reference__", c.index as usize)
    }
}

impl From<&protobuf::ScalarFunction> for BuiltinScalarFunction {
    fn from(f: &protobuf::ScalarFunction) -> BuiltinScalarFunction {
        use protobuf::ScalarFunction;
        match f {
            ScalarFunction::Sqrt => Self::Sqrt,
            ScalarFunction::Sin => Self::Sin,
            ScalarFunction::Cos => Self::Cos,
            ScalarFunction::Tan => Self::Tan,
            ScalarFunction::Asin => Self::Asin,
            ScalarFunction::Acos => Self::Acos,
            ScalarFunction::Atan => Self::Atan,
            ScalarFunction::Exp => Self::Exp,
            ScalarFunction::Log => Self::Log,
            ScalarFunction::Ln => Self::Ln,
            ScalarFunction::Log10 => Self::Log10,
            ScalarFunction::Floor => Self::Floor,
            ScalarFunction::Ceil => Self::Ceil,
            ScalarFunction::Round => Self::Round,
            ScalarFunction::Trunc => Self::Trunc,
            ScalarFunction::Abs => Self::Abs,
            ScalarFunction::OctetLength => Self::OctetLength,
            ScalarFunction::Concat => Self::Concat,
            ScalarFunction::Lower => Self::Lower,
            ScalarFunction::Upper => Self::Upper,
            ScalarFunction::Trim => Self::Trim,
            ScalarFunction::Ltrim => Self::Ltrim,
            ScalarFunction::Rtrim => Self::Rtrim,
            ScalarFunction::ToTimestamp => Self::ToTimestamp,
            ScalarFunction::Array => Self::MakeArray,
            ScalarFunction::NullIf => Self::NullIf,
            ScalarFunction::DatePart => Self::DatePart,
            ScalarFunction::DateTrunc => Self::DateTrunc,
            ScalarFunction::Md5 => Self::MD5,
            ScalarFunction::Sha224 => Self::SHA224,
            ScalarFunction::Sha256 => Self::SHA256,
            ScalarFunction::Sha384 => Self::SHA384,
            ScalarFunction::Sha512 => Self::SHA512,
            ScalarFunction::Digest => Self::Digest,
            ScalarFunction::ToTimestampMillis => Self::ToTimestampMillis,
            ScalarFunction::Log2 => Self::Log2,
            ScalarFunction::Signum => Self::Signum,
            ScalarFunction::Ascii => Self::Ascii,
            ScalarFunction::BitLength => Self::BitLength,
            ScalarFunction::Btrim => Self::Btrim,
            ScalarFunction::CharacterLength => Self::CharacterLength,
            ScalarFunction::Chr => Self::Chr,
            ScalarFunction::ConcatWithSeparator => Self::ConcatWithSeparator,
            ScalarFunction::InitCap => Self::InitCap,
            ScalarFunction::Left => Self::Left,
            ScalarFunction::Lpad => Self::Lpad,
            ScalarFunction::Random => Self::Random,
            ScalarFunction::RegexpReplace => Self::RegexpReplace,
            ScalarFunction::Repeat => Self::Repeat,
            ScalarFunction::Replace => Self::Replace,
            ScalarFunction::Reverse => Self::Reverse,
            ScalarFunction::Right => Self::Right,
            ScalarFunction::Rpad => Self::Rpad,
            ScalarFunction::SplitPart => Self::SplitPart,
            ScalarFunction::StartsWith => Self::StartsWith,
            ScalarFunction::Strpos => Self::Strpos,
            ScalarFunction::Substr => Self::Substr,
            ScalarFunction::ToHex => Self::ToHex,
            ScalarFunction::ToTimestampMicros => Self::ToTimestampMicros,
            ScalarFunction::ToTimestampSeconds => Self::ToTimestampSeconds,
            ScalarFunction::Now => Self::Now,
            ScalarFunction::Translate => Self::Translate,
            ScalarFunction::RegexpMatch => Self::RegexpMatch,
            ScalarFunction::Coalesce => Self::Coalesce,
            ScalarFunction::SparkExtFunctions => {
                unreachable!()
            }
        }
    }
}

pub fn try_parse_physical_expr(
    expr: &protobuf::PhysicalExprNode,
    input_schema: &SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>, PlanSerDeError> {
    let expr_type = expr
        .expr_type
        .as_ref()
        .ok_or_else(|| proto_error("Unexpected empty physical expression"))?;

    let pexpr: Arc<dyn PhysicalExpr> = match expr_type {
        ExprType::Column(c) => {
            let pcol: Column = c.into();
            Arc::new(pcol)
        }
        ExprType::Literal(scalar) => Arc::new(Literal::new(convert_required!(scalar.value)?)),
        ExprType::BoundReference(bound_reference) => {
            let pcol: Column = bound_reference.into();
            Arc::new(pcol)
        }
        ExprType::BinaryExpr(binary_expr) => Arc::new(BinaryExpr::new(
            try_parse_physical_expr_box_required(&binary_expr.l.clone(), input_schema)?,
            from_proto_binary_op(&binary_expr.op)?,
            try_parse_physical_expr_box_required(&binary_expr.r.clone(), input_schema)?,
        )),
        ExprType::AggExpr(_) => {
            return Err(PlanSerDeError::General(
                "Cannot convert aggregate expr node to physical expression".to_owned(),
            ));
        }
        ExprType::Sort(_) => {
            return Err(PlanSerDeError::General(
                "Cannot convert sort expr node to physical expression".to_owned(),
            ));
        }
        ExprType::IsNullExpr(e) => Arc::new(IsNullExpr::new(try_parse_physical_expr_box_required(
            &e.expr,
            input_schema,
        )?)),
        ExprType::IsNotNullExpr(e) => Arc::new(IsNotNullExpr::new(
            try_parse_physical_expr_box_required(&e.expr, input_schema)?,
        )),
        ExprType::NotExpr(e) => Arc::new(NotExpr::new(try_parse_physical_expr_box_required(
            &e.expr,
            input_schema,
        )?)),
        ExprType::Negative(e) => Arc::new(NegativeExpr::new(try_parse_physical_expr_box_required(
            &e.expr,
            input_schema,
        )?)),
        ExprType::InList(e) => Arc::new(InListExpr::new(
            try_parse_physical_expr_box_required(&e.expr, input_schema)?,
            e.list
                .iter()
                .map(|x| try_parse_physical_expr(x, input_schema))
                .collect::<Result<Vec<_>, _>>()?,
            e.negated,
            None,
        )),
        ExprType::Case(e) => Arc::new(CaseExpr::try_new(
            e.expr
                .as_ref()
                .map(|e| try_parse_physical_expr(e.as_ref(), input_schema))
                .transpose()?,
            e.when_then_expr
                .iter()
                .map(|e| {
                    Ok((
                        try_parse_physical_expr_required(&e.when_expr, input_schema)?,
                        try_parse_physical_expr_required(&e.then_expr, input_schema)?,
                    ))
                })
                .collect::<Result<Vec<_>, PlanSerDeError>>()?,
            e.else_expr
                .as_ref()
                .map(|e| try_parse_physical_expr(e.as_ref(), input_schema))
                .transpose()?,
        )?),
        ExprType::Cast(e) => Arc::new(CastExpr::new(
            try_parse_physical_expr_box_required(&e.expr, input_schema)?,
            convert_required!(e.arrow_type)?,
            None,
        )),
        ExprType::TryCast(e) => {
            let expr = try_parse_physical_expr_box_required(&e.expr, input_schema)?;
            let cast_type = convert_required!(e.arrow_type)?;
            Arc::new(TryCastExpr::new(expr, cast_type))
        }
        ExprType::ScalarFunction(e) => {
            let scalar_function = protobuf::ScalarFunction::from_i32(e.fun).ok_or_else(|| {
                proto_error(format!("Received an unknown scalar function: {}", e.fun,))
            })?;

            let args = e
                .args
                .iter()
                .map(|x| try_parse_physical_expr(x, input_schema))
                .collect::<Result<Vec<_>, _>>()?;

            let execution_props = ExecutionProps::new();
            let fun_expr = if scalar_function == protobuf::ScalarFunction::SparkExtFunctions {
                datafusion_ext_functions::create_spark_ext_function(&e.name)?
            } else {
                functions::create_physical_fun(&(&scalar_function).into(), &execution_props)?
            };

            Arc::new(ScalarFunctionExpr::new(
                &e.name,
                fun_expr,
                args,
                &convert_required!(e.return_type)?,
            ))
        }
        ExprType::SparkUdfWrapperExpr(e) => Arc::new(SparkUDFWrapperExpr::try_new(
            e.serialized.clone(),
            convert_required!(e.return_type)?,
            e.return_nullable,
            e.params
                .iter()
                .map(|x| try_parse_physical_expr(x, input_schema))
                .collect::<Result<Vec<_>, _>>()?,
        )?),
        ExprType::SparkScalarSubqueryWrapperExpr(e) => {
            Arc::new(SparkScalarSubqueryWrapperExpr::try_new(
                e.serialized.clone(),
                convert_required!(e.return_type)?,
                e.return_nullable,
            )?)
        }
        ExprType::GetIndexedFieldExpr(e) => {
            let expr = try_parse_physical_expr_box_required(&e.expr, input_schema)?;
            let key = convert_required!(e.key)?;
            Arc::new(GetIndexedFieldExpr::new(expr, key))
        }
        ExprType::GetMapValueExpr(e) => {
            let expr = try_parse_physical_expr_box_required(&e.expr, input_schema)?;
            let key = convert_required!(e.key)?;
            Arc::new(GetMapValueExpr::new(expr, key))
        }
        ExprType::StringStartsWithExpr(e) => {
            let expr = try_parse_physical_expr_box_required(&e.expr, input_schema)?;
            Arc::new(StringStartsWithExpr::new(expr, e.prefix.clone()))
        }
        ExprType::StringEndsWithExpr(e) => {
            let expr = try_parse_physical_expr_box_required(&e.expr, input_schema)?;
            Arc::new(StringEndsWithExpr::new(expr, e.suffix.clone()))
        }
        ExprType::StringContainsExpr(e) => {
            let expr = try_parse_physical_expr_box_required(&e.expr, input_schema)?;
            Arc::new(StringContainsExpr::new(expr, e.infix.clone()))
        }
        ExprType::ScAndExpr(e) => {
            let l = try_parse_physical_expr_box_required(&e.left, input_schema)?;
            let r = try_parse_physical_expr_box_required(&e.right, input_schema)?;
            Arc::new(SCAndExpr::new(l, r))
        }
        ExprType::ScOrExpr(e) => {
            let l = try_parse_physical_expr_box_required(&e.left, input_schema)?;
            let r = try_parse_physical_expr_box_required(&e.right, input_schema)?;
            Arc::new(SCOrExpr::new(l, r))
        }
        ExprType::LikeExpr(e) => Arc::new(LikeExpr::new(
            e.negated,
            e.case_insensitive,
            try_parse_physical_expr_box_required(&e.expr, input_schema)?,
            try_parse_physical_expr_box_required(&e.pattern, input_schema)?,
        )),

        ExprType::NamedStruct(e) => {
            let data_type = convert_required!(e.return_type)?;
            Arc::new(NamedStructExpr::try_new(
                e.values
                    .iter()
                    .map(|x| try_parse_physical_expr(x, input_schema))
                    .collect::<Result<Vec<_>, _>>()?,
                data_type,
            )?)
        }
    };

    Ok(pexpr)
}

fn try_parse_physical_expr_required(
    proto: &Option<protobuf::PhysicalExprNode>,
    input_schema: &SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>, PlanSerDeError> {
    if let Some(field) = proto.as_ref() {
        try_parse_physical_expr(field, input_schema)
    } else {
        Err(proto_error("Missing required field in protobuf"))
    }
}

fn try_parse_physical_expr_box_required(
    proto: &Option<Box<protobuf::PhysicalExprNode>>,
    input_schema: &SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>, PlanSerDeError> {
    if let Some(field) = proto.as_ref() {
        try_parse_physical_expr(field, input_schema)
    } else {
        Err(proto_error("Missing required field in protobuf"))
    }
}

pub fn parse_protobuf_hash_partitioning(
    input: Arc<dyn ExecutionPlan>,
    partitioning: Option<&protobuf::PhysicalHashRepartition>,
) -> Result<Option<Partitioning>, PlanSerDeError> {
    match partitioning {
        Some(hash_part) => {
            let expr = hash_part
                .hash_expr
                .iter()
                .map(|e| {
                    try_parse_physical_expr(e, &input.schema())
                        .and_then(|e| Ok(bind(e, &input.schema())?))
                })
                .collect::<Result<Vec<Arc<dyn PhysicalExpr>>, _>>()?;

            Ok(Some(Partitioning::Hash(
                expr,
                hash_part.partition_count.try_into().unwrap(),
            )))
        }
        None => Ok(None),
    }
}

impl TryFrom<&protobuf::PartitionedFile> for PartitionedFile {
    type Error = PlanSerDeError;

    fn try_from(val: &protobuf::PartitionedFile) -> Result<Self, Self::Error> {
        Ok(PartitionedFile {
            object_meta: ObjectMeta {
                location: Path::from(format!("/{}", BASE64_URL_SAFE_NO_PAD.encode(&val.path))),
                size: val.size as usize,
                last_modified: DateTime::default(),
                e_tag: None,
            },
            partition_values: val
                .partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            range: val.range.as_ref().map(|v| v.try_into()).transpose()?,
            extensions: None,
        })
    }
}

impl TryFrom<&protobuf::FileRange> for FileRange {
    type Error = PlanSerDeError;

    fn try_from(value: &protobuf::FileRange) -> Result<Self, Self::Error> {
        Ok(FileRange {
            start: value.start,
            end: value.end,
        })
    }
}

impl TryFrom<&protobuf::FileGroup> for Vec<PartitionedFile> {
    type Error = PlanSerDeError;

    fn try_from(val: &protobuf::FileGroup) -> Result<Self, Self::Error> {
        val.files
            .iter()
            .map(|f| f.try_into())
            .collect::<Result<Vec<_>, _>>()
    }
}

impl From<&protobuf::ColumnStats> for ColumnStatistics {
    fn from(cs: &protobuf::ColumnStats) -> ColumnStatistics {
        ColumnStatistics {
            null_count: Some(cs.null_count as usize),
            max_value: cs.max_value.as_ref().map(|m| m.try_into().unwrap()),
            min_value: cs.min_value.as_ref().map(|m| m.try_into().unwrap()),
            distinct_count: Some(cs.distinct_count as usize),
        }
    }
}

impl TryInto<Statistics> for &protobuf::Statistics {
    type Error = PlanSerDeError;

    fn try_into(self) -> Result<Statistics, Self::Error> {
        let column_statistics = self
            .column_stats
            .iter()
            .map(|s| s.into())
            .collect::<Vec<_>>();
        Ok(Statistics {
            num_rows: Some(self.num_rows as usize),
            total_byte_size: Some(self.total_byte_size as usize),
            // No column statistic (None) is encoded with empty array
            column_statistics: if column_statistics.is_empty() {
                None
            } else {
                Some(column_statistics)
            },
            is_exact: self.is_exact,
        })
    }
}

impl TryInto<FileScanConfig> for &protobuf::FileScanExecConf {
    type Error = PlanSerDeError;

    fn try_into(self) -> Result<FileScanConfig, Self::Error> {
        let schema: SchemaRef = Arc::new(convert_required!(self.schema)?);
        let projection = self
            .projection
            .iter()
            .map(|i| *i as usize)
            .collect::<Vec<_>>();
        let projection = if projection.is_empty() {
            None
        } else {
            Some(projection)
        };
        let statistics = convert_required!(self.statistics)?;
        let partition_schema: SchemaRef = Arc::new(convert_required!(self.partition_schema)?);

        let file_groups = (0..self.num_partitions)
            .map(|i| {
                if i == self.partition_index {
                    Ok(self
                        .file_group
                        .as_ref()
                        .expect("missing FileScanConfig.file_group")
                        .try_into()?)
                } else {
                    Ok(vec![])
                }
            })
            .collect::<Result<Vec<_>, PlanSerDeError>>()?;

        Ok(FileScanConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(), // not used
            file_schema: schema,
            file_groups,
            statistics,
            projection,
            limit: self.limit.as_ref().map(|sl| sl.limit as usize),
            table_partition_cols: partition_schema
                .fields()
                .iter()
                .map(|field| (field.name().clone(), field.data_type().clone()))
                .collect(),
            output_ordering: vec![],
            infinite_source: false,
        })
    }
}
