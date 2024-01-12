use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use table::Table;

mod table;

pub fn init(output_dir: PathBuf, rotation: RotationPolicy) {
    let logger = CsvLogger::new(output_dir, rotation);
    let mut log = table_log::GLOBAL_LOG.lock().unwrap();
    log.register(Box::new(logger));
}

pub struct CsvLogger {
    output_dir: PathBuf,
    tables: HashMap<&'static str, Table>,
    rotation: RotationPolicy,
}
impl CsvLogger {
    pub fn new(output_dir: PathBuf, rotation: RotationPolicy) -> Self {
        Self {
            output_dir,
            tables: HashMap::new(),
            rotation,
        }
    }
}
impl table_log::Logger for CsvLogger {
    fn log(&mut self, record: &dyn table_log::LogRecord) {
        let table = self.tables.entry(record.table_name()).or_insert_with(|| {
            let path = log_file_path(&self.output_dir, record.table_name(), 0);
            let writer = create_clean_log_writer(path);
            Table::new(writer)
        });
        table.serialize(record).expect("Failed to serialize");

        // Rotate log file
        if self.rotation.max_records < table.records_written() {
            let new_path = log_file_path(&self.output_dir, record.table_name(), table.epoch() + 1);
            let new_writer = create_clean_log_writer(new_path);
            table.replace(new_writer);

            let del_epoch = table.epoch().checked_sub(self.rotation.max_epochs);
            if let Some(del_epoch) = del_epoch {
                let del_path = log_file_path(&self.output_dir, record.table_name(), del_epoch);
                if del_path.exists() {
                    std::fs::remove_file(del_path).expect("Failed to remove outdated log file");
                }
            }
        }
    }

    fn flush(&mut self) {
        self.tables.iter_mut().for_each(|(_, t)| {
            t.flush().expect("Failed to flush");
        });
    }
}
pub struct RotationPolicy {
    pub max_records: usize,
    pub max_epochs: usize,
}

fn create_clean_log_writer(path: impl AsRef<Path>) -> csv::Writer<std::fs::File> {
    if path.as_ref().exists() {
        std::fs::remove_file(&path).expect("Failed to remove occupied log file");
    }
    std::fs::create_dir_all(path.as_ref().parent().unwrap()).expect("Failed to create directories");
    let file = std::fs::File::options()
        .create(true)
        .write(true)
        .open(path)
        .expect("Cannot create a log file");
    csv::Writer::from_writer(file)
}

fn log_file_path(output_dir: impl AsRef<Path>, table_name: &str, epoch: usize) -> PathBuf {
    let mut path = output_dir.as_ref().join(table_name).join(epoch.to_string());
    path.set_extension("csv");
    path
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

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

    #[test]
    fn test_logger() {
        let dir = tempfile::tempdir().unwrap();
        init(
            dir.path().to_owned(),
            RotationPolicy {
                max_records: 2,
                max_epochs: 2,
            },
        );
        table_log::log(&TestRecord { s: "a", n: 0 });
        table_log::log(&TestRecord { s: "b", n: 1 });
        table_log::flush();
        let path = log_file_path(dir.path(), "test", 0);
        assert!(path.exists());
        let mut file = std::fs::File::options().read(true).open(path).unwrap();
        let mut csv = String::new();
        file.read_to_string(&mut csv).unwrap();
        assert_eq!(
            csv,
            r#"s,n
a,0
b,1
"#
        );
    }
}
