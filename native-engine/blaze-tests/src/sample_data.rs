use datafusion::common::DataFusionError;
use blaze_serde::protobuf::{PhysicalBinaryExprNode, PhysicalColumn, PhysicalExprNode, PhysicalPlanNode, scalar_value, ScalarValue, TaskDefinition};
use blaze_serde::protobuf::physical_expr_node::ExprType;
use prost::Message;

pub fn sample_eq_filter(col_name: &str, col_id: u32, r_val: &str) -> PhysicalExprNode {
    let column = PhysicalColumn {
        name: col_name.to_string(),
        index: col_id,
    };

    let literal = ScalarValue {
        value: Some(scalar_value::Value::Utf8Value(r_val.to_string())),
    };

    let left_node = PhysicalExprNode {
        expr_type: Some(ExprType::Column(column)),
    };

    let right_node = PhysicalExprNode {
        expr_type: Some(ExprType::Literal(literal)),
    };

    let root_node = PhysicalExprNode {
        expr_type: Some(ExprType::BinaryExpr(Box::new(PhysicalBinaryExprNode {
            op: "Eq".to_string(),
            l: Some(Box::new(left_node)),
            r: Some(Box::new(right_node)),
        }))),
    };

    return root_node;
}

pub fn sample_filter() -> PhysicalExprNode {
    let input = r#"
        binary_expr {
          l {
            column {
              name: "data"
            }
          }
          r {
            literal {
              utf8_value: "bc"
            }
          }
          op: "Eq"
        }
    "#;

    let base64_string = "Ii0KDDoKCggKBgoEZGF0YRIYIhYKCAoGCgRkYXRhEgYSBBICYmMaAkVxGgNBbmQ=";
    let binary = base64::decode(base64_string).unwrap();
    // let proto_message = PhysicalExprNode::decode(binary.as_slice()).unwrap();

    let root_node = PhysicalExprNode::decode(binary.as_slice())
        .map_err(|err| DataFusionError::Plan(format!("cannot decode PhysicalExprNode: {:?}", err))).unwrap();
    // let mut bytes = vec![];
    // root_node.encode(&mut bytes).unwrap();
    // let string = String::from_utf8_lossy(&bytes);
    // println!("the extracted proto message: {}", string);
    return root_node;
}

pub fn sample_task_definition() -> TaskDefinition {
    let input = r#"

        filter {
      input {
        rename_columns {
          input {
            parquet_scan {
              base_conf {
                num_partitions: 1
                file_group {
                  files {
                    path: "data/sample0.parquet"
                    size: 817
                    range {
                      start: 4
                      end: 817
                    }
                  }
                }
                schema {
                  columns {
                    name: "id"
                    arrow_type {
                      INT64 {
                      }
                    }
                    nullable: true
                  }
                  columns {
                    name: "data"
                    arrow_type {
                      UTF8 {
                      }
                    }
                    nullable: true
                  }
                  columns {
                    name: "float"
                    arrow_type {
                      FLOAT32 {
                      }
                    }
                    nullable: true
                  }
                }
                projection: 1
                statistics {
                }
                partition_schema {
                }
              }
              pruning_predicates {
                binary_expr {
                  l {
                    is_not_null_expr {
                      expr {
                        column {
                          name: "data"
                        }
                      }
                    }
                  }
                  r {
                    binary_expr {
                      l {
                        column {
                          name: "data"
                        }
                      }
                      r {
                        literal {
                          utf8_value: "bc"
                        }
                      }
                      op: "Eq"
                    }
                  }
                  op: "And"
                }
              }
              fsResourceId: "NativeParquetScanExec:53284939-621a-4583-a39c-a9b7e179acc0"
            }
          }
          renamed_column_names: "\#13"
        }
      }
      expr {
        is_not_null_expr {
          expr {
            column {
              name: "\#13"
            }
          }
        }
      }
      expr {
        binary_expr {
          l {
            column {
              name: "\#13"
            }
          }
          r {
            literal {
              utf8_value: "bc"
            }
          }
          op: "Eq"
        }
      }
    }

    "#;

    let base64_string = "CgUKATAQARKFAkKCAgraAWLXAQrKASrHAQpZCAEaIgogChRkYXRhL3NhbXBsZTAucGFycXVldBCxBioFCAQQsQYiKQoKCgJpZBICUgAYAQoMCgRkYXRhEgJyABgBCg0KBWZsb2F0EgJiABgBMgIAAUIASgASLiIsCgw6CgoICgYKBGRhdGESFyIVCggKBgoEZGF0YRIFEgMSAWIaAkVxGgNBbmQaOk5hdGl2ZVBhcnF1ZXRTY2FuRXhlYzo5YmM2YzJmNy1hMzYwLTRkM2QtYThmYi0zZTQ5ZWU0MDM1ZDESAyMxMhIDIzEzEgs6CQoHCgUKAyMxMxIWIhQKBwoFCgMjMTMSBRIDEgFiGgJFcQ==";
    let binary = base64::decode(base64_string).unwrap();
    // let proto_message = PhysicalExprNode::decode(binary.as_slice()).unwrap();

    let root_node = TaskDefinition::decode(binary.as_slice())
        .map_err(|err| DataFusionError::Plan(format!("cannot decode execution plan: {:?}", err))).unwrap();
    return root_node;
}