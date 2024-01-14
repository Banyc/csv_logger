use std::{
    collections::HashMap,
    io::{Read, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    time::Duration,
};

use table::Table;

mod table;

const FLUSH_INTERVAL: Duration = Duration::from_secs(30);

pub fn init(output_dir: PathBuf, rotation: RotationPolicy) {
    let logger = CsvLogger::new(output_dir, rotation);
    let mut log = table_log::GLOBAL_LOG.lock().unwrap();
    if log.has_logger() {
        panic!("Only one logger can be registered at a time");
    }
    log.register(Box::new(logger));
    drop(log);
    std::thread::Builder::new()
        .name("CsvLogger::flush()".to_string())
        .spawn(|| loop {
            std::thread::sleep(FLUSH_INTERVAL);
            let mut log = table_log::GLOBAL_LOG.lock().unwrap();
            log.flush();
        })
        .expect("Failed to spawn the flushing worker thread");
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
        let entry = self.tables.entry(record.table_name());
        let (table, new) = match entry {
            std::collections::hash_map::Entry::Occupied(entry) => (entry.into_mut(), false),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let epoch = cur_epoch(&self.output_dir, record.table_name())
                    .map(|e| e + 1)
                    .unwrap_or_default();
                let path = log_file_path(&self.output_dir, record.table_name(), epoch);
                let writer = create_clean_log_writer(path);
                let table = entry.insert(Table::new(writer, epoch));
                (table, true)
            }
        };
        if new {
            let epoch = table.epoch();
            write_epoch(&self.output_dir, record.table_name(), epoch);
            delete_old_log_file(
                epoch,
                self.rotation.max_epochs,
                &self.output_dir,
                record.table_name(),
            );
        }
        table.serialize(record).expect("Failed to serialize");

        // Rotate log file
        if self.rotation.max_records.get() <= table.records_written() {
            let new_path = log_file_path(&self.output_dir, record.table_name(), table.epoch() + 1);
            let new_writer = create_clean_log_writer(new_path);
            table.replace(new_writer);

            let epoch = table.epoch();
            write_epoch(&self.output_dir, record.table_name(), epoch);
            delete_old_log_file(
                epoch,
                self.rotation.max_epochs,
                &self.output_dir,
                record.table_name(),
            );
        }
    }

    fn flush(&mut self) {
        self.tables.iter_mut().for_each(|(_, t)| {
            t.flush().expect("Failed to flush");
        });
    }
}
pub struct RotationPolicy {
    pub max_records: NonZeroUsize,
    pub max_epochs: usize,
}

fn delete_old_log_file(
    epoch: usize,
    max_epochs: usize,
    output_dir: impl AsRef<Path>,
    table_name: &str,
) {
    let del_epoch = epoch.checked_sub(max_epochs);
    if let Some(del_epoch) = del_epoch {
        let del_path = log_file_path(output_dir, table_name, del_epoch);
        if del_path.exists() {
            std::fs::remove_file(del_path).expect("Failed to remove outdated log file");
        }
    }
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

fn write_epoch(output_dir: impl AsRef<Path>, table_name: &str, epoch: usize) {
    let path = epoch_file_path(output_dir, table_name);
    std::fs::create_dir_all(path.parent().unwrap()).expect("Failed to create directories");
    if path.exists() {
        std::fs::remove_file(&path).expect("Failed to delete old epoch file");
    }
    let mut file = std::fs::File::options()
        .create(true)
        .write(true)
        .open(path)
        .expect("Failed to create an epoch file");
    file.write_all(epoch.to_string().as_bytes())
        .expect("Failed to write epoch to the file");
}

fn cur_epoch(output_dir: impl AsRef<Path>, table_name: &str) -> Option<usize> {
    let path = epoch_file_path(output_dir, table_name);
    if !path.exists() {
        return None;
    }
    let mut file = std::fs::File::options()
        .read(true)
        .open(&path)
        .expect("Failed to open the epoch file");
    let mut epoch = String::new();
    file.read_to_string(&mut epoch)
        .expect("Failed to read the epoch file");
    let epoch: usize = match epoch.parse() {
        Ok(epoch) => epoch,
        Err(_) => {
            std::fs::remove_file(&path).expect("Failed to delete old epoch file");
            return None;
        }
    };
    Some(epoch)
}

fn epoch_file_path(output_dir: impl AsRef<Path>, table_name: &str) -> PathBuf {
    output_dir.as_ref().join(table_name).join("epoch")
}

fn log_file_path(output_dir: impl AsRef<Path>, table_name: &str, epoch: usize) -> PathBuf {
    let mut path = output_dir.as_ref().join(table_name).join(epoch.to_string());
    path.set_extension("csv");
    path
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use serial_test::serial;

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

    fn remove_logger() {
        let mut log = table_log::GLOBAL_LOG.lock().unwrap();
        log.remove_logger();
    }

    #[test]
    #[serial]
    fn test_logger() {
        let dir = tempfile::tempdir().unwrap();
        init(
            dir.path().to_owned(),
            RotationPolicy {
                max_records: NonZeroUsize::new(2).unwrap(),
                max_epochs: 2,
            },
        );
        table_log::log!(&TestRecord { s: "a", n: 0 });
        table_log::log!(&TestRecord { s: "b", n: 1 });
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

        remove_logger();
    }

    #[test]
    #[serial]
    fn test_rotation() {
        let dir = tempfile::tempdir().unwrap();
        init(
            dir.path().to_owned(),
            RotationPolicy {
                max_records: NonZeroUsize::new(2).unwrap(),
                max_epochs: 2,
            },
        );

        table_log::log!(&TestRecord { s: "a", n: 0 });
        table_log::flush();
        let path = log_file_path(dir.path(), "test", 0);
        assert!(path.exists());
        let path = log_file_path(dir.path(), "test", 1);
        assert!(!path.exists());

        table_log::log!(&TestRecord { s: "b", n: 1 });
        let path = log_file_path(dir.path(), "test", 0);
        assert!(path.exists());
        let path = log_file_path(dir.path(), "test", 1);
        assert!(path.exists());
        let path = log_file_path(dir.path(), "test", 2);
        assert!(!path.exists());

        table_log::log!(&TestRecord { s: "c", n: 2 });
        table_log::flush();
        let path = log_file_path(dir.path(), "test", 0);
        assert!(path.exists());
        let path = log_file_path(dir.path(), "test", 1);
        assert!(path.exists());
        let path = log_file_path(dir.path(), "test", 2);
        assert!(!path.exists());

        table_log::log!(&TestRecord { s: "d", n: 3 });
        let path = log_file_path(dir.path(), "test", 0);
        assert!(!path.exists());
        let path = log_file_path(dir.path(), "test", 1);
        assert!(path.exists());
        let path = log_file_path(dir.path(), "test", 2);
        assert!(path.exists());
        let path = log_file_path(dir.path(), "test", 3);
        assert!(!path.exists());

        // println!("{:?}", dir.path());
        // std::thread::sleep(std::time::Duration::from_secs(u64::MAX));

        remove_logger();
    }
}
