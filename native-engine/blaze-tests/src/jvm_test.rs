use std::env;
use std::path::PathBuf;
use jni::{InitArgsBuilder, JavaVM, JNIVersion};
use blaze_jni_bridge::jni_bridge::JavaClasses;

pub fn init_jvm() {
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

    // println!("Jar paths: {}", jar_paths);

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
}