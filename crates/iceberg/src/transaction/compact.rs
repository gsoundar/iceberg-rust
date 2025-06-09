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

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::SchemaRef;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, ManifestContentType, ManifestEntry,
    ManifestFile, Operation, Struct,
};
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceAction, SnapshotProduceOperation,
};
use crate::transaction::Transaction;
use crate::{Error, ErrorKind};

/// Defines the compaction strategy to use when compacting data files
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionType {
    /// Group files into appropriately sized bins based on file size
    BinPack,
    /// Sort the data according to the table's sort order
    Sort,
    /// Z-Order optimization for multi-dimensional clustering
    ZOrder,
}

/// Options for configuring compaction
#[derive(Debug, Clone)]
pub struct CompactionOptions {
    /// The compaction strategy to use
    pub compaction_type: Option<CompactionType>,

    /// Target file size in megabytes for the output files after compaction
    /// Files will be grouped into batches that aim to reach this size
    pub target_file_size_mb: u64,

    /// Maximum file size in megabytes for input files to be considered for compaction
    /// Files larger than this will be excluded from compaction
    /// Set to 0 to disable filtering based on size
    pub input_file_size_mb: u64,

    /// Maximum number of batches to compact at once
    /// When limited (value > 0), batches with the most small files will be prioritized
    /// A value of 0 means no limit is applied
    pub max_batches: usize,

    /// Custom writer properties for the parquet files
    /// If None, default properties will be used
    pub writer_properties: Option<WriterProperties>,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        let target_file_size_mb = 512; // 512MB default target size
        CompactionOptions {
            compaction_type: Some(CompactionType::BinPack),
            target_file_size_mb,
            input_file_size_mb: (target_file_size_mb as f64 * 0.75) as u64, // Default to 75% of target size
            max_batches: 1,          // Default to just one batch
            writer_properties: None, // Use default properties
        }
    }
}

/// A batch of files from a single partition to be compacted together
struct CompactionBatch {
    /// The partition data
    partition: Struct,
    /// The files in this partition to be compacted
    files: Vec<Arc<ManifestEntry>>,
    /// The output file name for this batch of files to be compacted
    output_file_name: String,
    /// Flag indicating this is a NoOp batch (input files should be directly returned as outputs)
    is_noop: bool,
}

impl CompactionBatch {
    /// Creates a new compaction batch for the given partition with the specified files and output file name
    fn new_with_file_name(
        partition: Struct,
        files: Vec<Arc<ManifestEntry>>,
        output_file_name: String,
    ) -> Self {
        Self {
            partition,
            files,
            output_file_name,
            is_noop: false,
        }
    }

    /// Creates a new NoOp batch for files that don't meet compaction criteria
    fn new_noop(partition: Struct, files: Vec<Arc<ManifestEntry>>) -> Self {
        Self {
            partition,
            files,
            output_file_name: String::new(), // Not used for NoOp batches
            is_noop: true,
        }
    }
}

/// Results from compacting a batch of files
#[derive(Debug)]
pub struct CompactionResult {
    /// Number of rows in the source files
    pub input_row_count: u64,
    /// Number of rows in the output file
    pub output_row_count: u64,
    /// Time taken to compact the files
    pub duration: Duration,
    /// Checksum of all input rows
    pub input_checksum: u64,
    /// Checksum of all output rows
    pub output_checksum: u64,
    /// Input files that were compacted
    pub input_files: Vec<DataFile>,
}

pub struct CompactionAction<'a> {
    snapshot_produce_action: SnapshotProduceAction<'a>,
    options: CompactionOptions,
}

impl<'a> CompactionAction<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        tx: Transaction<'a>,
        snapshot_id: i64,
        commit_uuid: Uuid,
        options: CompactionOptions,
        key_metadata: Vec<u8>,
        snapshot_properties: HashMap<String, String>,
    ) -> Result<Self> {
        Ok(Self {
            snapshot_produce_action: SnapshotProduceAction::new(
                tx,
                snapshot_id,
                key_metadata,
                commit_uuid,
                snapshot_properties,
            )?,
            options,
        })
    }

    /// Apply compaction and commit the results to the table
    ///
    /// This is the main public entry point for compaction. When called, it:
    /// 1. Identifies batches of files that need compaction
    /// 2. Compacts each batch into a single file
    /// 3. Creates a transaction that commits these changes to the table
    ///
    /// # Returns
    ///
    /// A transaction that can be committed to the catalog
    pub async fn apply(self) -> Result<Transaction<'a>> {
        // Perform compaction and get results
        let compaction_results = self.compact().await?;

        // If no batches were compacted, there's nothing to commit
        if compaction_results.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "No files were compacted",
            ));
        }

        // Create DataFile objects for the output files
        // For now, this is a placeholder - we would need to extract information
        // from the compacted files to create proper DataFile objects

        // In a complete implementation, we would:
        // 1. Collect the output file paths from the compaction process
        // 2. Create DataFile objects with proper metadata (size, record count, etc.)
        // 3. Pass these to prepare()

        // For now, we'll just use a placeholder example
        let table = self.snapshot_produce_action.tx.base_table;

        // Create a placeholder DataFile for each compaction result
        let output_files = compaction_results
            .iter()
            .enumerate()
            .map(|(i, result)| {
                // In a real implementation, we would use the actual file path and stats
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(format!("compacted/output_{}.parquet", i))
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(1024 * 1024) // 1MB placeholder
                    .record_count(result.output_row_count)
                    .partition_spec_id(table.metadata().default_partition_spec_id())
                    .partition(Struct::empty())
                    .build()
                    .unwrap()
            })
            .collect::<Vec<_>>();

        // Prepare and return the transaction
        self.prepare(compaction_results, output_files).await
    }

    /// Prepares a transaction for committing after compaction
    ///
    /// This function takes the results of a compaction operation and creates a new
    /// Iceberg compaction commit that can be committed to the catalog.
    ///
    /// When called, this function:
    /// 1. Validates the compaction results
    /// 2. Creates new data file entries for the compacted output files
    /// 3. Generates a manifest with the new compacted files
    /// 4. Creates a manifest list containing all manifests
    /// 5. Creates a new snapshot with the manifest list
    /// 6. Returns a transaction that can be committed to update the table
    ///
    /// # Arguments
    ///
    /// * `results` - A vector of results from the compact operation
    /// * `output_files` - A vector of DataFile objects representing the compacted output files
    ///
    /// # Returns
    ///
    /// A Transaction object that can be committed to the catalog
    async fn prepare(
        mut self,
        results: Vec<CompactionResult>,
        output_files: Vec<DataFile>,
    ) -> Result<Transaction<'a>> {
        // If no results were returned, there's nothing to commit
        if results.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "No compaction results to commit",
            ));
        }

        // If no output files were provided, there's nothing to commit
        if output_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "No output files provided for compaction commit",
            ));
        }

        // Add the output files from compaction to the snapshot_produce_action
        self.snapshot_produce_action.add_data_files(output_files)?;

        // Apply the compaction operation to the transaction
        // This will:
        // 1. Write a manifest for the added files
        // 2. Get existing manifests through CompactionOperation.existing_manifest
        // 3. Create a manifest list with all manifests
        // 4. Create a new snapshot with the manifest list
        // 5. Apply the snapshot to the transaction

        // TODO: Handle deletion of input files properly
        // For now, we're just adding the new files without removing the old ones

        self.snapshot_produce_action
            .apply(CompactionOperation, DefaultManifestProcess)
            .await
    }

    /// Compacts all available batches
    ///
    /// This is the main entry point for compaction. It determines which batches to compact
    /// (respecting the max_batches setting), and compacts each batch in sequence.
    ///
    /// # Returns
    ///
    /// A vector of compaction results, one for each batch that was compacted.
    async fn compact(&self) -> Result<Vec<CompactionResult>> {
        // Check if the compaction type is supported
        if let Some(compaction_type) = &self.options.compaction_type {
            if *compaction_type != CompactionType::BinPack {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!(
                        "{:?} compaction is not supported, only BinPack is implemented",
                        compaction_type
                    ),
                ));
            }
        }

        // Enumerate batches to compact
        let all_batches = self.enumerate().await?;

        // If no batches to compact, return empty result
        if all_batches.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();

        // Process each batch
        for batch in all_batches {
            // Create output path - for now, use the table's location plus the batch filename
            let table = self.snapshot_produce_action.tx.base_table;
            let table_location = table.metadata().location();
            let output_path = Path::new(table_location).join(&batch.output_file_name);

            // Compact this batch and collect results
            let result = self.compact_one_batch(&batch, &output_path).await?;
            results.push(result);
        }

        Ok(results)
    }

    /// Compacts a single batch of files into a single file
    ///
    /// # Arguments
    ///
    /// * `batch` - The batch of files to compact
    /// * `output_path` - The path to write the compacted file
    ///
    /// # Returns
    ///
    /// A result containing the row counts and duration of the compaction
    async fn compact_one_batch(
        &self,
        batch: &CompactionBatch,
        output_path: &Path,
    ) -> Result<CompactionResult> {
        let start_time = Instant::now();

        // Handle NoOp batches by returning input files directly
        if batch.is_noop {
            // For NoOp batches, we just return the input files
            let input_files: Vec<DataFile> = batch
                .files
                .iter()
                .map(|entry| entry.data_file().clone())
                .collect();
            let total_rows: u64 = input_files.iter().map(|f| f.record_count()).sum();

            return Ok(CompactionResult {
                input_row_count: total_rows,
                output_row_count: total_rows,
                duration: start_time.elapsed(),
                input_checksum: 0,
                output_checksum: 0,
                input_files,
            });
        }

        let table = self.snapshot_produce_action.tx.base_table;
        let file_io = table.file_io();

        // Track row counts and collected record batches
        let mut total_input_rows = 0;
        let mut all_record_batches: Vec<RecordBatch> = Vec::new();
        let mut schema_opt: Option<SchemaRef> = None;

        // Read data from all files in the batch
        for file_entry in &batch.files {
            let file_path = file_entry.file_path();
            let file_rows = file_entry.record_count();

            // Track input row count from manifest
            total_input_rows += file_rows;

            // Read the file using the file_io
            let input_file = file_io.new_input(file_path)?;
            let file_bytes = input_file.read().await?;

            // ParquetRecordBatchReaderBuilder directly supports Bytes
            let reader = match ParquetRecordBatchReaderBuilder::try_new(file_bytes) {
                Ok(builder) => match builder.build() {
                    Ok(reader) => reader,
                    Err(e) => return Err(Error::new(ErrorKind::Unexpected, e.to_string())),
                },
                Err(e) => return Err(Error::new(ErrorKind::Unexpected, e.to_string())),
            };

            let file_schema = reader.schema().clone();

            // Store the schema from the first file or validate schema consistency
            if let Some(ref schema) = schema_opt {
                // Validate that schemas are the same
                if schema != &file_schema {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Schema mismatch between files in compaction batch. First file schema: {:?}, current file schema: {:?}",
                            schema, file_schema
                        ),
                    ));
                }
            } else {
                schema_opt = Some(file_schema);
            }

            // Read all record batches from this file
            for batch_result in reader {
                match batch_result {
                    Ok(record_batch) => {
                        all_record_batches.push(record_batch);
                    }
                    Err(e) => {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            format!("Error reading record batch: {}", e),
                        ));
                    }
                }
            }
        }

        // If we didn't read any files or batches, return early
        let schema = match schema_opt {
            Some(s) => s,
            None => {
                return Ok(CompactionResult {
                    input_row_count: 0,
                    output_row_count: 0,
                    duration: start_time.elapsed(),
                    input_checksum: 0,
                    output_checksum: 0,
                    input_files: Vec::new(),
                });
            }
        };

        // Count output rows
        let mut output_rows = 0;
        for batch in &all_record_batches {
            output_rows += batch.num_rows() as u64;
        }

        // Create an in-memory buffer for the output
        let mut output_buffer = Vec::new();

        // Use custom writer properties if provided, otherwise use defaults
        let writer_props = match &self.options.writer_properties {
            Some(props) => props.clone(),
            None => WriterProperties::builder().build(),
        };

        // Write to the buffer first
        {
            let mut writer = ArrowWriter::try_new(&mut output_buffer, schema, Some(writer_props))?;

            // Write all collected record batches
            for batch in all_record_batches {
                writer.write(&batch)?;
            }

            // Close the writer to ensure all data is flushed
            writer.close()?;
        }

        // Now write the buffer to the output file
        let output_path_str = output_path.to_str().unwrap_or_default();
        let output_file = file_io.new_output(output_path_str)?;
        output_file.write(Bytes::from(output_buffer)).await?;

        // Return the metrics about the compaction operation
        Ok(CompactionResult {
            input_row_count: total_input_rows,
            output_row_count: output_rows,
            duration: start_time.elapsed(),
            input_checksum: 0,
            output_checksum: 0,
            input_files: batch
                .files
                .iter()
                .map(|entry| entry.data_file().clone())
                .collect(),
        })
    }

    async fn enumerate(&self) -> Result<Vec<CompactionBatch>> {
        let table = self.snapshot_produce_action.tx.base_table;
        let table_metadata = table.metadata();
        let file_io = table.file_io();
        let current_snapshot = table_metadata
            .current_snapshot()
            .expect("Current snapshot must exist");
        let current_partition_spec = table_metadata.default_partition_spec_id();
        let manifest_list = current_snapshot
            .load_manifest_list(table.file_io(), &table_metadata)
            .await
            .expect("Manifest list must exist or be empty");

        // Generate a UUID for file naming
        let uuid = Uuid::new_v4();
        let uuid_str = uuid.to_string();
        let uuid_short = &uuid_str[0..8]; // Use first 8 characters of UUID for shorter file names

        // Empty table.
        if manifest_list.entries().len() == 0 {
            return Ok(vec![]);
        }

        // Build a map of data files grouped by partition to be compacted.
        let mut data_files_by_partition: HashMap<Struct, Vec<Arc<ManifestEntry>>> = HashMap::new();

        // Keep track of files that don't meet compaction criteria (files to be included in NoOp batch)
        let mut noop_files_by_partition: HashMap<Struct, Vec<Arc<ManifestEntry>>> = HashMap::new();

        for manifest_file in manifest_list.consume_entries() {
            // Compact files for current partition spec.
            if manifest_file.partition_spec_id != current_partition_spec {
                continue;
            }
            // TODO: Handle delete files.
            if manifest_file.content != ManifestContentType::Data {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!("Position/Equality deletes not supported"),
                ));
            }
            let manifest = manifest_file
                .load_manifest(&file_io)
                .await
                .expect("Manifest pointed by list should exist");
            for entry in manifest.entries() {
                assert!(
                    entry.content_type() == DataContentType::Data,
                    "Data manifest should contain only Data files"
                );
                if !entry.is_alive() {
                    continue;
                }
                if entry.file_format() != DataFileFormat::Parquet {
                    return Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        format!("Only Parquet data files are supported"),
                    ));
                }

                // The schema ID for a data file is stored in the manifest's metadata
                // We want to check if the manifest has a different schema than what we're currently using
                // and skip files from older schema versions
                // TODO: Look for a cleaner way to access the manifest's schema ID
                // For now we'll skip this check since the metadata field is private
                // This would be the right way to implement this once we have access to the schema_id

                // Use the actual partition as the key
                let partition = entry.data_file().partition().clone();

                // Apply input file size filter if configured
                if self.options.input_file_size_mb > 0 {
                    let max_input_size_bytes = self.options.input_file_size_mb * 1024 * 1024;
                    if entry.data_file().file_size_in_bytes() > max_input_size_bytes {
                        // Add to NoOp batch - these files are too large to compact but should be included in output
                        noop_files_by_partition
                            .entry(partition)
                            .or_insert_with(Vec::new)
                            .push(entry.clone());
                        continue;
                    }
                }

                // Add to regular compaction batch
                data_files_by_partition
                    .entry(partition)
                    .or_insert_with(Vec::new)
                    .push(entry.clone());
            }
        }

        // Create compaction batches from files grouped by partition
        let mut all_compaction_batches = Vec::new();

        // Target size in bytes
        let target_size_bytes = self.options.target_file_size_mb as u64 * 1024 * 1024;

        // Counter for generating file names
        let mut file_counter = 0;

        // Now we have all data files grouped by partition
        // Process each partition group separately
        for (partition_key, mut files) in data_files_by_partition {
            // Skip partitions with only one file as they don't need compaction
            if files.len() <= 1 {
                continue;
            }

            // Sort files by size for better bin packing
            files.sort_by_key(|entry| entry.data_file().file_size_in_bytes());

            let mut current_batch_files = Vec::new();
            let mut current_batch_size = 0;

            // Group files into batches that approach but don't exceed target size
            for file in files {
                let file_size = file.data_file().file_size_in_bytes();

                // If adding this file would exceed target size and we already have files in the batch,
                // finalize the current batch and start a new one
                if current_batch_size + file_size > target_size_bytes
                    && !current_batch_files.is_empty()
                {
                    // Generate a file name following the format "{prefix}-{file_count}-{uuid}.{format}"
                    // Similar to how DefaultFileNameGenerator would generate it
                    let output_file_name =
                        format!("part-{:05}-{}.parquet", file_counter, uuid_short);
                    file_counter += 1;

                    let batch = CompactionBatch::new_with_file_name(
                        partition_key.clone(),
                        std::mem::take(&mut current_batch_files),
                        output_file_name,
                    );
                    all_compaction_batches.push(batch);
                    current_batch_size = 0;
                }

                // Add file to current batch
                current_batch_files.push(file);
                current_batch_size += file_size;
            }

            // Don't forget the last batch if it has any files
            if !current_batch_files.is_empty() {
                // Generate a file name for the last batch
                let output_file_name = format!("part-{:05}-{}.parquet", file_counter, uuid_short);

                let batch = CompactionBatch::new_with_file_name(
                    partition_key,
                    current_batch_files,
                    output_file_name,
                );
                all_compaction_batches.push(batch);
            }
        }

        // Implement max_batches limit if it's greater than 0
        if self.options.max_batches > 0 && all_compaction_batches.len() > self.options.max_batches {
            // Sort batches by their utility for compaction
            // Prioritize batches with more small files (greater file count)
            all_compaction_batches.sort_by(|a, b| b.files.len().cmp(&a.files.len()));

            // Limit to max_batches
            all_compaction_batches.truncate(self.options.max_batches);
        }

        // Add NoOp batches for files that don't meet compaction criteria
        for (partition, files) in noop_files_by_partition {
            if !files.is_empty() {
                all_compaction_batches.push(CompactionBatch::new_noop(partition, files));
            }
        }

        Ok(all_compaction_batches)
    }
}

struct CompactionOperation;

impl SnapshotProduceOperation for CompactionOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProduceAction<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProduceAction<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce
            .tx
            .current_table
            .metadata()
            .current_snapshot()
        else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.tx.current_table.file_io(),
                &snapshot_produce.tx.current_table.metadata_ref(),
            )
            .await?;

        Ok(manifest_list
            .entries()
            .iter()
            .filter(|entry| entry.has_added_files() || entry.has_existing_files())
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use uuid::Uuid;

    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestEntry,
        Operation, PrimitiveLiteral, Struct,
    };
    use crate::transaction::compact::{
        CompactionBatch, CompactionOptions, CompactionResult, CompactionType,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::Transaction;
    use crate::TableUpdate;

    #[test]
    fn test_compaction_options() {
        let _options = CompactionOptions::default();
        // Successfully creating an instance is enough for this test
    }

    #[test]
    fn test_compaction_type() {
        let options = CompactionOptions {
            compaction_type: Some(CompactionType::BinPack),
            target_file_size_mb: 512,
            input_file_size_mb: 384,
            max_batches: 1,
            writer_properties: None,
        };
        assert_eq!(options.compaction_type, Some(CompactionType::BinPack));

        let options = CompactionOptions {
            compaction_type: Some(CompactionType::Sort),
            target_file_size_mb: 512,
            input_file_size_mb: 384,
            max_batches: 1,
            writer_properties: None,
        };
        assert_eq!(options.compaction_type, Some(CompactionType::Sort));

        let options = CompactionOptions {
            compaction_type: Some(CompactionType::ZOrder),
            target_file_size_mb: 512,
            input_file_size_mb: 384,
            max_batches: 1,
            writer_properties: None,
        };
        assert_eq!(options.compaction_type, Some(CompactionType::ZOrder));

        let options = CompactionOptions {
            compaction_type: None,
            target_file_size_mb: 512,
            input_file_size_mb: 384,
            max_batches: 1,
            writer_properties: None,
        };
        assert_eq!(options.compaction_type, None);
    }

    #[test]
    fn test_target_file_size() {
        let options = CompactionOptions {
            compaction_type: Some(CompactionType::BinPack),
            target_file_size_mb: 256,
            input_file_size_mb: 192, // 75% of 256
            max_batches: 1,
            writer_properties: None,
        };
        assert_eq!(options.target_file_size_mb, 256);
        assert_eq!(options.input_file_size_mb, 192);

        let default_options = CompactionOptions::default();
        assert_eq!(default_options.target_file_size_mb, 512);
        assert_eq!(default_options.input_file_size_mb, 384); // 75% of 512
        assert_eq!(default_options.max_batches, 1);
    }

    #[test]
    fn test_max_batches() {
        let options = CompactionOptions {
            compaction_type: Some(CompactionType::BinPack),
            target_file_size_mb: 512,
            input_file_size_mb: 384,
            max_batches: 5,
            writer_properties: None,
        };
        assert_eq!(options.max_batches, 5);

        // Test the "no limit" case
        let unlimited_options = CompactionOptions {
            compaction_type: Some(CompactionType::BinPack),
            target_file_size_mb: 512,
            input_file_size_mb: 384,
            max_batches: 0,
            writer_properties: None,
        };
        assert_eq!(unlimited_options.max_batches, 0);

        let default_options = CompactionOptions::default();
        assert_eq!(default_options.max_batches, 1);
    }

    #[test]
    fn test_compaction_batch_new() {
        // Create a simple partition struct
        let partition = Struct::empty();

        // Create an empty vector - since we only care about the length for this test
        let files = Vec::new();

        // Test the new method
        let batch = CompactionBatch {
            partition: partition.clone(),
            files,
            output_file_name: String::new(),
            is_noop: false,
        };

        assert_eq!(batch.output_file_name, "");
        assert!(batch.files.is_empty());
        assert!(!batch.is_noop);

        // Test with a specified filename
        let output_file_name = "test-file.parquet".to_string();
        let batch_with_name = CompactionBatch {
            partition: partition.clone(),
            files: Vec::new(),
            output_file_name: output_file_name.clone(),
            is_noop: false,
        };

        assert_eq!(batch_with_name.output_file_name, output_file_name);

        // Test creating a NoOp batch
        let noop_batch = CompactionBatch::new_noop(partition, Vec::new());
        assert!(noop_batch.is_noop);
        assert_eq!(noop_batch.output_file_name, "");
    }

    #[tokio::test]
    async fn test_noop_compaction_batch() -> Result<(), Box<dyn std::error::Error>> {
        // Create a test batch with files that exceed the input_file_size_mb threshold
        let partition = Struct::empty();

        // Create input files that are larger than the input_file_size_mb setting
        let file1_path = "file1.parquet";
        let file2_path = "file2.parquet";

        // Create data files with sizes larger than the threshold (400MB each)
        let large_file_size = 400 * 1024 * 1024; // 400MB
        let input_file1 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(file1_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(large_file_size)
            .record_count(1000)
            .partition_spec_id(1) // dummy value for test
            .partition(partition.clone())
            .build()
            .unwrap();

        let input_file2 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(file2_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(large_file_size)
            .record_count(2000)
            .partition_spec_id(1) // dummy value for test
            .partition(partition.clone())
            .build()
            .unwrap();

        // Create manifest entries that wrap these data files
        let entry1 = Arc::new(
            ManifestEntry::builder()
                .status(crate::spec::ManifestStatus::Added)
                .data_file(input_file1.clone())
                .build(),
        );

        let entry2 = Arc::new(
            ManifestEntry::builder()
                .status(crate::spec::ManifestStatus::Added)
                .data_file(input_file2.clone())
                .build(),
        );

        // Create a NoOp batch
        let batch = CompactionBatch::new_noop(partition, vec![entry1, entry2]);

        // Verify the batch is marked as NoOp
        assert!(batch.is_noop);
        assert_eq!(batch.files.len(), 2);

        // Set up options with input_file_size_mb = 300MB (smaller than our 400MB files)
        let options = CompactionOptions {
            compaction_type: Some(CompactionType::BinPack),
            target_file_size_mb: 512,
            input_file_size_mb: 300, // 300MB threshold, our files are 400MB
            max_batches: 1,
            writer_properties: None,
        };

        // Now let's compact the batch
        // We need a dummy output path since it won't actually be used for NoOp batches
        let output_path = std::path::Path::new("dummy_path");
        // Create a test table and transaction
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Create a CompactionAction directly
        let action = crate::transaction::CompactionAction::new(
            tx,
            1, // dummy snapshot ID
            Uuid::new_v4(),
            options,
            Vec::new(),     // empty key_metadata
            HashMap::new(), // empty properties
        )
        .unwrap();

        // Compact the batch
        let result = action.compact_one_batch(&batch, output_path).await?;

        // Verify the result contains exactly the same files we put in
        assert_eq!(result.input_files.len(), 2);

        // Verify that the output file paths match the input file paths
        let result_paths: Vec<&str> = result.input_files.iter().map(|f| f.file_path()).collect();
        assert!(result_paths.contains(&file1_path));
        assert!(result_paths.contains(&file2_path));

        // Verify the record counts match
        assert_eq!(result.input_row_count, 3000); // 1000 + 2000
        assert_eq!(result.output_row_count, 3000); // Should be the same

        Ok(())
    }
}
