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

//! This module contains memory catalog implementation.

use std::collections::HashMap;

use async_trait::async_trait;
use futures::lock::Mutex;
use iceberg::io::FileIO;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use itertools::Itertools;
use uuid::Uuid;

use crate::namespace_state::NamespaceState;

/// namespace `location` property
const LOCATION: &str = "location";

/// Memory catalog implementation.
#[derive(Debug)]
pub struct MemoryCatalog {
    root_namespace_state: Mutex<NamespaceState>,
    file_io: FileIO,
    warehouse_location: Option<String>,
}

impl MemoryCatalog {
    /// Creates an memory catalog.
    pub fn new(file_io: FileIO, warehouse_location: Option<String>) -> Self {
        Self {
            root_namespace_state: Mutex::new(NamespaceState::default()),
            file_io,
            warehouse_location,
        }
    }
}

#[async_trait]
impl Catalog for MemoryCatalog {
    /// List namespaces inside the catalog.
    async fn list_namespaces(
        &self,
        maybe_parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        match maybe_parent {
            None => {
                let namespaces = root_namespace_state
                    .list_top_level_namespaces()
                    .into_iter()
                    .map(|str| NamespaceIdent::new(str.to_string()))
                    .collect_vec();

                Ok(namespaces)
            }
            Some(parent_namespace_ident) => {
                let namespaces = root_namespace_state
                    .list_namespaces_under(parent_namespace_ident)?
                    .into_iter()
                    .map(|name| NamespaceIdent::new(name.to_string()))
                    .collect_vec();

                Ok(namespaces)
            }
        }
    }

    /// Create a new namespace inside the catalog.
    async fn create_namespace(
        &self,
        namespace_ident: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        root_namespace_state.insert_new_namespace(namespace_ident, properties.clone())?;
        let namespace = Namespace::with_properties(namespace_ident.clone(), properties);

        Ok(namespace)
    }

    /// Get a namespace information from the catalog.
    async fn get_namespace(&self, namespace_ident: &NamespaceIdent) -> Result<Namespace> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        let namespace = Namespace::with_properties(
            namespace_ident.clone(),
            root_namespace_state
                .get_properties(namespace_ident)?
                .clone(),
        );

        Ok(namespace)
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace_ident: &NamespaceIdent) -> Result<bool> {
        let guarded_namespaces = self.root_namespace_state.lock().await;

        Ok(guarded_namespaces.namespace_exists(namespace_ident))
    }

    /// Update a namespace inside the catalog.
    ///
    /// # Behavior
    ///
    /// The properties must be the full set of namespace.
    async fn update_namespace(
        &self,
        namespace_ident: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        root_namespace_state.replace_properties(namespace_ident, properties)
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace_ident: &NamespaceIdent) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        root_namespace_state.remove_existing_namespace(namespace_ident)
    }

    /// List tables from namespace.
    async fn list_tables(&self, namespace_ident: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        let table_names = root_namespace_state.list_tables(namespace_ident)?;
        let table_idents = table_names
            .into_iter()
            .map(|table_name| TableIdent::new(namespace_ident.clone(), table_name.clone()))
            .collect_vec();

        Ok(table_idents)
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace_ident: &NamespaceIdent,
        table_creation: TableCreation,
    ) -> Result<Table> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        let table_name = table_creation.name.clone();
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name);

        let (table_creation, location) = match table_creation.location.clone() {
            Some(location) => (table_creation, location),
            None => {
                let namespace_properties = root_namespace_state.get_properties(namespace_ident)?;
                let location_prefix = match namespace_properties.get(LOCATION) {
                    Some(namespace_location) => Ok(namespace_location.clone()),
                    None => match self.warehouse_location.clone() {
                        Some(warehouse_location) => Ok(format!(
                            "{}/{}",
                            warehouse_location,
                            namespace_ident.join("/")
                        )),
                        None => Err(Error::new(
                            ErrorKind::Unexpected,
                            format!(
                                "Cannot create table {:?}. No default path is set, please specify a location when creating a table.",
                                &table_ident
                            ),
                        )),
                    },
                }?;

                let location = format!("{}/{}", location_prefix, table_ident.name());

                let new_table_creation = TableCreation {
                    location: Some(location.clone()),
                    ..table_creation
                };

                (new_table_creation, location)
            }
        };

        let metadata = TableMetadataBuilder::from_table_creation(table_creation)?
            .build()?
            .metadata;
        let metadata_location = format!(
            "{}/metadata/{}-{}.metadata.json",
            &location,
            0,
            Uuid::new_v4()
        );

        self.file_io
            .new_output(&metadata_location)?
            .write(serde_json::to_vec(&metadata)?.into())
            .await?;

        root_namespace_state.insert_new_table(&table_ident, metadata_location.clone())?;

        Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(table_ident)
            .build()
    }

    /// Load table from the catalog.
    async fn load_table(&self, table_ident: &TableIdent) -> Result<Table> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        let metadata_location = root_namespace_state.get_existing_table_location(table_ident)?;
        let input_file = self.file_io.new_input(metadata_location)?;
        let metadata_content = input_file.read().await?;
        let metadata = serde_json::from_slice::<TableMetadata>(&metadata_content)?;

        Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(metadata_location.clone())
            .metadata(metadata)
            .identifier(table_ident.clone())
            .build()
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table_ident: &TableIdent) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        let metadata_location = root_namespace_state.remove_existing_table(table_ident)?;
        self.file_io.delete(&metadata_location).await
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table_ident: &TableIdent) -> Result<bool> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        root_namespace_state.table_exists(table_ident)
    }

    /// Rename a table in the catalog.
    async fn rename_table(
        &self,
        src_table_ident: &TableIdent,
        dst_table_ident: &TableIdent,
    ) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        let mut new_root_namespace_state = root_namespace_state.clone();
        let metadata_location = new_root_namespace_state
            .get_existing_table_location(src_table_ident)?
            .clone();
        new_root_namespace_state.remove_existing_table(src_table_ident)?;
        new_root_namespace_state.insert_new_table(dst_table_ident, metadata_location)?;
        *root_namespace_state = new_root_namespace_state;

        Ok(())
    }

    /// Update a table to the catalog.
    async fn update_table(&self, mut commit: TableCommit) -> Result<Table> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;
        // Get table identifier first before we mutably borrow commit
        let table_ident = commit.identifier().clone();

        // Check if the table exists
        if !root_namespace_state.table_exists(&table_ident)? {
            return Err(Error::new(
                ErrorKind::TableNotFound,
                format!("Cannot update non-existent table: {:?}", table_ident),
            ));
        }

        // Get the existing metadata location
        let old_metadata_location =
            root_namespace_state.get_existing_table_location(&table_ident)?;

        // Load the existing metadata
        let input_file = self.file_io.new_input(old_metadata_location)?;
        let existing_metadata_content = input_file.read().await?;
        let existing_metadata =
            serde_json::from_slice::<TableMetadata>(&existing_metadata_content)?;

        // Get requirements and updates from commit
        let requirements = commit.take_requirements();
        let updates = commit.take_updates();

        // Check all requirements
        for requirement in requirements {
            requirement.check(Some(&existing_metadata))?;
        }

        // Apply updates to create new metadata
        let mut builder = TableMetadataBuilder::new_from_metadata(
            existing_metadata,
            Some(old_metadata_location.to_string()),
        );
        for update in updates {
            builder = update.apply(builder)?;
        }
        let updated_metadata = builder.build()?.metadata;

        // Generate new metadata path (format: metadata/uuid-seqno.metadata.json)
        let table_location = updated_metadata.location();
        let metadata_location = format!(
            "{}/metadata/{}-{}.metadata.json",
            table_location,
            updated_metadata.last_sequence_number(),
            uuid::Uuid::new_v4()
        );

        // Write the updated metadata
        self.file_io
            .new_output(&metadata_location)?
            .write(serde_json::to_vec(&updated_metadata)?.into())
            .await?;

        // Remove the old table entry and insert the new one
        root_namespace_state.remove_existing_table(&table_ident)?;
        root_namespace_state.insert_new_table(&table_ident, metadata_location.clone())?;

        // Return the updated table
        Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(metadata_location)
            .metadata(updated_metadata)
            .identifier(table_ident.clone())
            .build()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::hash::Hash;
    use std::iter::FromIterator;

    use iceberg::io::FileIOBuilder;
    use iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, Type};
    use regex::Regex;
    use tempfile::TempDir;

    use super::*;

    fn temp_path() -> String {
        let temp_dir = TempDir::new().unwrap();
        temp_dir.path().to_str().unwrap().to_string()
    }

    fn new_memory_catalog() -> impl Catalog {
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let warehouse_location = temp_path();
        MemoryCatalog::new(file_io, Some(warehouse_location))
    }

    async fn create_namespace<C: Catalog>(catalog: &C, namespace_ident: &NamespaceIdent) {
        let _ = catalog
            .create_namespace(namespace_ident, HashMap::new())
            .await
            .unwrap();
    }

    async fn create_namespaces<C: Catalog>(catalog: &C, namespace_idents: &Vec<&NamespaceIdent>) {
        for namespace_ident in namespace_idents {
            let _ = create_namespace(catalog, namespace_ident).await;
        }
    }

    fn to_set<T: std::cmp::Eq + Hash>(vec: Vec<T>) -> HashSet<T> {
        HashSet::from_iter(vec)
    }

    fn simple_table_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![NestedField::required(
                1,
                "foo",
                Type::Primitive(PrimitiveType::Int),
            )
            .into()])
            .build()
            .unwrap()
    }

    fn extended_table_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap()
    }

    async fn create_table<C: Catalog>(catalog: &C, table_ident: &TableIdent) {
        let _ = catalog
            .create_table(
                &table_ident.namespace,
                TableCreation::builder()
                    .name(table_ident.name().into())
                    .schema(simple_table_schema())
                    .build(),
            )
            .await
            .unwrap();
    }

    async fn create_tables<C: Catalog>(catalog: &C, table_idents: Vec<&TableIdent>) {
        for table_ident in table_idents {
            create_table(catalog, table_ident).await;
        }
    }

    fn assert_table_eq(table: &Table, expected_table_ident: &TableIdent, expected_schema: &Schema) {
        assert_eq!(table.identifier(), expected_table_ident);

        let metadata = table.metadata();

        assert_eq!(metadata.current_schema().as_ref(), expected_schema);

        let expected_partition_spec = PartitionSpec::builder((*expected_schema).clone())
            .with_spec_id(0)
            .build()
            .unwrap();

        assert_eq!(
            metadata
                .partition_specs_iter()
                .map(|p| p.as_ref())
                .collect_vec(),
            vec![&expected_partition_spec]
        );

        let expected_sorted_order = SortOrder::builder()
            .with_order_id(0)
            .with_fields(vec![])
            .build(expected_schema)
            .unwrap();

        assert_eq!(
            metadata
                .sort_orders_iter()
                .map(|s| s.as_ref())
                .collect_vec(),
            vec![&expected_sorted_order]
        );

        assert_eq!(metadata.properties(), &HashMap::new());

        assert!(!table.readonly());
    }

    const UUID_REGEX_STR: &str = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

    fn assert_table_metadata_location_matches(table: &Table, regex_str: &str) {
        let actual = table.metadata_location().unwrap().to_string();
        let regex = Regex::new(regex_str).unwrap();
        assert!(regex.is_match(&actual))
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_empty_vector() {
        let catalog = new_memory_catalog();

        assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_single_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("abc".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![
            namespace_ident
        ]);
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_multiple_namespaces() {
        let catalog = new_memory_catalog();
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![&namespace_ident_1, &namespace_ident_2]).await;

        assert_eq!(
            to_set(catalog.list_namespaces(None).await.unwrap()),
            to_set(vec![namespace_ident_1, namespace_ident_2])
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_only_top_level_namespaces() {
        let catalog = new_memory_catalog();
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![
            &namespace_ident_1,
            &namespace_ident_2,
            &namespace_ident_3,
        ])
        .await;

        assert_eq!(
            to_set(catalog.list_namespaces(None).await.unwrap()),
            to_set(vec![namespace_ident_1, namespace_ident_3])
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_no_namespaces_under_parent() {
        let catalog = new_memory_catalog();
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![&namespace_ident_1, &namespace_ident_2]).await;

        assert_eq!(
            catalog
                .list_namespaces(Some(&namespace_ident_1))
                .await
                .unwrap(),
            vec![]
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_namespace_under_parent() {
        let catalog = new_memory_catalog();
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::new("c".into());
        create_namespaces(&catalog, &vec![
            &namespace_ident_1,
            &namespace_ident_2,
            &namespace_ident_3,
        ])
        .await;

        assert_eq!(
            to_set(catalog.list_namespaces(None).await.unwrap()),
            to_set(vec![namespace_ident_1.clone(), namespace_ident_3])
        );

        assert_eq!(
            catalog
                .list_namespaces(Some(&namespace_ident_1))
                .await
                .unwrap(),
            vec![NamespaceIdent::new("b".into())]
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_multiple_namespaces_under_parent() {
        let catalog = new_memory_catalog();
        let namespace_ident_1 = NamespaceIdent::new("a".to_string());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "a"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_4 = NamespaceIdent::from_strs(vec!["a", "c"]).unwrap();
        let namespace_ident_5 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![
            &namespace_ident_1,
            &namespace_ident_2,
            &namespace_ident_3,
            &namespace_ident_4,
            &namespace_ident_5,
        ])
        .await;

        assert_eq!(
            to_set(
                catalog
                    .list_namespaces(Some(&namespace_ident_1))
                    .await
                    .unwrap()
            ),
            to_set(vec![
                NamespaceIdent::new("a".into()),
                NamespaceIdent::new("b".into()),
                NamespaceIdent::new("c".into()),
            ])
        );
    }

    #[tokio::test]
    async fn test_namespace_exists_returns_false() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert!(!catalog
            .namespace_exists(&NamespaceIdent::new("b".into()))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_namespace_exists_returns_true() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert!(catalog.namespace_exists(&namespace_ident).await.unwrap());
    }

    #[tokio::test]
    async fn test_create_namespace_with_empty_properties() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await
                .unwrap(),
            Namespace::new(namespace_ident.clone())
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_create_namespace_with_properties() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("abc".into());

        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("k".into(), "v".into());

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, properties.clone())
                .await
                .unwrap(),
            Namespace::with_properties(namespace_ident.clone(), properties.clone())
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, properties)
        );
    }

    #[tokio::test]
    async fn test_create_namespace_throws_error_if_namespace_already_exists() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceAlreadyExists => Cannot create namespace {:?}. Namespace already exists.",
                &namespace_ident
            )
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_create_nested_namespace() {
        let catalog = new_memory_catalog();
        let parent_namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &parent_namespace_ident).await;

        let child_namespace_ident = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&child_namespace_ident, HashMap::new())
                .await
                .unwrap(),
            Namespace::new(child_namespace_ident.clone())
        );

        assert_eq!(
            catalog.get_namespace(&child_namespace_ident).await.unwrap(),
            Namespace::with_properties(child_namespace_ident, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_create_deeply_nested_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident_a_b_c, HashMap::new())
                .await
                .unwrap(),
            Namespace::new(namespace_ident_a_b_c.clone())
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b_c).await.unwrap(),
            Namespace::with_properties(namespace_ident_a_b_c, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_create_nested_namespace_throws_error_if_top_level_namespace_doesnt_exist() {
        let catalog = new_memory_catalog();

        let nested_namespace_ident = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&nested_namespace_ident, HashMap::new())
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceNotFound => No such namespace: {:?}",
                NamespaceIdent::new("a".into())
            )
        );

        assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn test_create_deeply_nested_namespace_throws_error_if_intermediate_namespace_doesnt_exist(
    ) {
        let catalog = new_memory_catalog();

        let namespace_ident_a = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident_a).await;

        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident_a_b_c, HashMap::new())
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceNotFound => No such namespace: {:?}",
                NamespaceIdent::from_strs(vec!["a", "b"]).unwrap()
            )
        );

        assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![
            namespace_ident_a.clone()
        ]);

        assert_eq!(
            catalog
                .list_namespaces(Some(&namespace_ident_a))
                .await
                .unwrap(),
            vec![]
        );
    }

    #[tokio::test]
    async fn test_get_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("abc".into());

        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("k".into(), "v".into());
        let _ = catalog
            .create_namespace(&namespace_ident, properties.clone())
            .await
            .unwrap();

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, properties)
        )
    }

    #[tokio::test]
    async fn test_update_table_set_properties() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("namespace".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_ident = TableIdent::new(namespace_ident.clone(), "test_table".into());
        create_table(&catalog, &table_ident).await;

        // Load the table to get its metadata
        let table = catalog.load_table(&table_ident).await.unwrap();

        // Create a transaction
        let tx = iceberg::transaction::Transaction::new(&table);

        // Add properties
        let mut props = HashMap::new();
        props.insert("key".to_string(), "value".to_string());

        // Set the properties and commit
        let new_tx = tx.set_properties(props).unwrap();
        let commit_result = new_tx.commit(&catalog).await;
        assert!(commit_result.is_ok());

        // Verify the property was set
        let updated_table = catalog.load_table(&table_ident).await.unwrap();
        assert_eq!(
            updated_table.metadata().properties().get("key").unwrap(),
            "value"
        );
    }

    #[tokio::test]
    async fn test_update_table_schema_change() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("namespace".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_ident = TableIdent::new(namespace_ident.clone(), "test_table".into());
        create_table(&catalog, &table_ident).await;

        // Load the table to get its metadata
        let table = catalog.load_table(&table_ident).await.unwrap();

        // Create a transaction
        let tx = iceberg::transaction::Transaction::new(&table);

        // Add a property to verify the update
        let mut props = HashMap::new();
        props.insert("test_update".to_string(), "value".to_string());

        let new_tx = tx.set_properties(props).unwrap();
        let commit_result = new_tx.commit(&catalog).await;
        assert!(commit_result.is_ok());

        // Verify the property was set
        let updated_table = catalog.load_table(&table_ident).await.unwrap();
        assert_eq!(
            updated_table
                .metadata()
                .properties()
                .get("test_update")
                .unwrap(),
            "value"
        );
    }

    #[tokio::test]
    async fn test_update_table_with_requirements() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("namespace".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_ident = TableIdent::new(namespace_ident.clone(), "test_table".into());
        create_table(&catalog, &table_ident).await;

        // Load the table to get its metadata
        let table = catalog.load_table(&table_ident).await.unwrap();

        // Create a transaction
        let tx = iceberg::transaction::Transaction::new(&table);

        // Add properties
        let mut props = HashMap::new();
        props.insert("key".to_string(), "value".to_string());

        let new_tx = tx.set_properties(props).unwrap();

        // The uuid requirement is automatically included
        let commit_result = new_tx.commit(&catalog).await;
        assert!(commit_result.is_ok());

        // Verify the property was set
        let updated_table = catalog.load_table(&table_ident).await.unwrap();
        assert_eq!(
            updated_table.metadata().properties().get("key").unwrap(),
            "value"
        );
    }

    #[tokio::test]
    async fn test_table_exists_returns_false_for_nonexistent_table() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("namespace".into());
        create_namespace(&catalog, &namespace_ident).await;

        // Create an identifier for a table that doesn't exist
        let nonexistent_table_ident =
            TableIdent::new(namespace_ident.clone(), "nonexistent".into());

        // Check that the table does not exist
        let exists = catalog
            .table_exists(&nonexistent_table_ident)
            .await
            .unwrap();
        assert!(!exists);
    }

    #[tokio::test]
    async fn test_load_table_fails_for_nonexistent_table() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("namespace".into());
        create_namespace(&catalog, &namespace_ident).await;

        // Create an identifier for a table that doesn't exist
        let nonexistent_table_ident =
            TableIdent::new(namespace_ident.clone(), "nonexistent".into());

        // Attempt to load the nonexistent table
        let result = catalog.load_table(&nonexistent_table_ident).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No such table"));
    }

    #[tokio::test]
    async fn test_update_table_fails_for_nonexistent_table() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("namespace".into());
        create_namespace(&catalog, &namespace_ident).await;

        let nonexistent_table_ident =
            TableIdent::new(namespace_ident.clone(), "nonexistent".into());

        let dummy_commit = TableCommit::builder()
            .ident(nonexistent_table_ident.clone())
            .updates(vec![])
            .requirements(vec![])
            .build();

        let result = catalog.update_table(dummy_commit).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Cannot update non-existent table"));
    }
}
