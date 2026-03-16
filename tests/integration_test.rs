use mongodb::bson::Document;
use mongodb::sync::Client;
use polars::prelude::*;
use polars_mongo::prelude::*;
use std::fs;
use testcontainers::runners::SyncRunner;
use testcontainers_modules::mongo::Mongo;
struct TestContext {
    // We hold the container so it stays alive for the duration of the test
    _container: testcontainers::Container<Mongo>,
    // pub db: Database,
    pub connection_string: String,
}

impl TestContext {
    /// Creates a fresh Mongo container and seeds it with data from a JSON file
    fn new(db_name: &str, coll_name: &str, fixture_path: &str) -> Self {
        // 1. Setup Infrastructure
        let container = Mongo::default().start().expect("Docker not running");
        let host_port = container
            .get_host_port_ipv4(27017)
            .expect("Failed to get port");
        let connection_string = format!("mongodb://127.0.0.1:{}", host_port);

        let client = Client::with_uri_str(&connection_string).expect("Failed to create client");
        let db = client.database(db_name);

        // 2. Load and Seed Data immediately
        let file_content = fs::read_to_string(fixture_path).expect("Failed to read fixture file");

        let json_array: Vec<serde_json::Value> =
            serde_json::from_str(&file_content).expect("Failed to parse JSON");

        let docs: Vec<Document> = json_array
            .into_iter()
            .map(|v| {
                let bson_val = mongodb::bson::to_bson(&v).unwrap();
                bson_val
                    .as_document()
                    .expect("JSON was not a document")
                    .clone()
            })
            .collect();

        db.collection::<Document>(coll_name)
            .insert_many(docs)
            .run()
            .expect("Failed to seed database");

        Self {
            _container: container,
            // db,
            connection_string,
        }
    }
}
fn options(ctx: &TestContext) -> MongoScanOptions {
    MongoScanOptions {
        connection_str: ctx.connection_string.clone(),
        db: "integration_db".to_string(),
        collection: "test_collection".to_string(),
        infer_schema_length: Some(100),
        n_rows: None,
        batch_size: None,
    }
}
fn context() -> TestContext {
    TestContext::new(
        "integration_db",
        "test_collection",
        "tests/fixtures/seed.json",
    )
}
#[test]
fn test_mongo_scan_integration() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = context();
    let options = options(&ctx);
    let df = LazyFrame::scan_mongo_collection(options)?.collect();
    assert!(df.is_ok());
    dbg!("{:#?}", &df?);
    Ok(())
}

#[test]
fn test_projections() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = context();
    let options = options(&ctx);
    let df = LazyFrame::scan_mongo_collection(options)?
        .select([col("cmdLine.config")])
        .collect()?;
    dbg!("{:#?}", df);
    let expected = df!("cmdLine.config"=>&["/etc/mongod.conf","/etc/mongod.conf","/etc/mongod.conf","/etc/mongod.conf","/etc/mongod.conf","/etc/mongod.conf","/etc/mongod.conf","/etc/mongod.conf","/etc/mongod.conf","/etc/mongod.conf"])?;
    assert_eq!(df, expected);
    Ok(())
}
