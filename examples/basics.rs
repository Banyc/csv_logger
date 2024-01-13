#[derive(serde::Serialize)]
struct TestRecord<'caller> {
    pub s: &'caller str,
    pub n: usize,
}
impl<'caller> table_log::LogRecord<'caller> for TestRecord<'caller> {
    fn table_name(&self) -> &'static str {
        "test"
    }
}

fn main() {
    let dir = tempfile::tempdir().unwrap();
    csv_logger::init(
        dir.path().to_owned(),
        csv_logger::RotationPolicy {
            max_records: 2,
            max_epochs: 2,
        },
    );
    table_log::log!(&TestRecord { s: "a", n: 0 });
    table_log::log!(&TestRecord { s: "b", n: 1 });
    table_log::flush();
}
