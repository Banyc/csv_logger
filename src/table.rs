use std::io;

pub struct Table {
    records_written: usize,
    epoch: usize,
    writer: csv::Writer<std::fs::File>,
}
impl Table {
    pub fn new(writer: csv::Writer<std::fs::File>) -> Self {
        Self {
            records_written: 0,
            epoch: 0,
            writer,
        }
    }

    pub fn replace(&mut self, writer: csv::Writer<std::fs::File>) {
        self.writer = writer;
        self.epoch += 1;
        self.records_written = 0;
    }

    pub fn serialize(&mut self, record: &dyn table_log::LogRecord) -> Result<(), csv::Error> {
        self.writer
            .serialize(record as &dyn erased_serde::Serialize)?;
        self.records_written += 1;
        Ok(())
    }

    pub fn epoch(&self) -> usize {
        self.epoch
    }

    pub fn records_written(&self) -> usize {
        self.records_written
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}