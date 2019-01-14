#[macro_use(stmt)]
extern crate cassandra_cpp;
extern crate futures;

mod help;

use cassandra_cpp::*;
use futures::Future;
use std::thread;
use std::sync::Arc;


static TRUNCATE_QUERY: &'static str = "TRUNCATE examples.log;";
static INSERT_QUERY: &'static str = "INSERT INTO examples.log (key, time, entry) VALUES (?, ?, ?);";
static SELECT_QUERY: &'static str = "SELECT * FROM examples.log WHERE key = ?";
static CREATE_TABLE: &'static str = "CREATE TABLE IF NOT EXISTS examples.log (key text, time timeuuid, entry text, \
                                     PRIMARY KEY (key, time));";

fn insert_into_log(session: &Session, key: &str, time: Uuid, entry: &str) -> Result<CassResult> {
    let mut statement = stmt!(INSERT_QUERY);
    statement.bind(0, key)?;
    statement.bind(1, time)?;
    statement.bind(2, entry)?;
    let future = session.execute(&statement);
    future.wait()
}

fn select_from_log(session: &Session, key: &str) -> Result<Vec<(Uuid, String)>> {
    let mut statement = stmt!(SELECT_QUERY);
    statement.bind(0, key)?;
    let future = session.execute(&statement);
    let results = future.wait();
    results.map(|r| {
        r.iter().map(|r| {
            let t: Uuid = r.get_column(1).expect("time0").get_uuid().expect("time");
            let e: String = r.get(2).expect("entry");
            (t, e)
        }).collect()
    })
}

#[test]
fn test_multi_threads() {
    let uuid_gen = UuidGen::default();

    let session = help::create_test_session();
    help::create_example_keyspace(&session);

    session.execute(&stmt!(CREATE_TABLE)).wait().unwrap();
    session.execute(&stmt!(TRUNCATE_QUERY)).wait().unwrap();

    // Make a vector to hold the children which are spawned.
    let mut children = vec![];

    let session = Arc::new(session);

    for i in 0..5 {
        // Spin up another thread
        let session_clone = session.clone();
        children.push(thread::spawn(move || {
            let uuid_gen = UuidGen::default();
            println!("this is thread number {}", i);
            for j in 0..5 {
                let key = format!("test {}", i).to_owned();
                let entry = format!("Log entry {}", j).to_owned();
                insert_into_log(session_clone.as_ref(), &key, uuid_gen.gen_time(), &entry).expect("Insert Ok");
            }
        }));
    }

    for child in children {
        // Wait for the thread to finish. Returns a result.
        let _ = child.join();
    }

    for i in 0..5 {
        let key = format!("test {}", i).to_owned();
        let results = select_from_log(session.as_ref(), &key).unwrap();
        println!("{:?}", results);

        let mut uniques = results.iter().map(|ref kv| kv.0).collect::<Vec<Uuid>>();
        uniques.dedup();
        assert_eq!(5, uniques.len());
    }
}
