use std::sync::Arc;

use arrow::{array::RecordBatch, compute::concat_batches, datatypes::SchemaRef, error::ArrowError};
use arrow_flight::{
    sql::{client::FlightSqlServiceClient, CommandGetTables},
    FlightInfo,
};
use async_trait::async_trait;
use datafusion::{
    common::cast::as_string_array,
    error::{DataFusionError, Result},
    physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream},
    sql::unparser::dialect::{DefaultDialect, Dialect},
};
use datafusion_federation::sql::SQLExecutor;
use futures::TryStreamExt;
use tonic::transport::Channel;

pub struct FlightSQLExecutor {
    context: String,
    catalog: String,
    client: FlightSqlServiceClient<Channel>,
}

impl FlightSQLExecutor {
    pub fn new(dsn: String, catalog: String, client: FlightSqlServiceClient<Channel>) -> Self {
        Self {
            context: dsn,
            catalog,
            client,
        }
    }

    pub fn context(&mut self, context: String) {
        self.context = context;
    }
}
async fn make_flight_stream(
    flight_info: FlightInfo,
    mut client: FlightSqlServiceClient<Channel>,
    schema: SchemaRef,
) -> Result<SendableRecordBatchStream> {
    let mut flight_data_streams = Vec::with_capacity(flight_info.endpoint.len());
    for endpoint in flight_info.endpoint {
        let ticket = endpoint.ticket.ok_or(DataFusionError::Execution(
            "FlightEndpoint missing ticket!".to_string(),
        ))?;
        let flight_data = client.do_get(ticket).await?;
        flight_data_streams.push(flight_data);
    }

    let record_batch_stream = futures::stream::select_all(flight_data_streams)
        .map_err(|e| DataFusionError::External(Box::new(e)));

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema,
        record_batch_stream,
    )))
}

async fn make_flight_sql_stream(
    sql: String,
    mut client: FlightSqlServiceClient<Channel>,
    schema: SchemaRef,
) -> Result<SendableRecordBatchStream> {
    let flight_info = client
        .execute(sql.to_string(), None)
        .await
        .map_err(arrow_error_to_df)?;

    make_flight_stream(flight_info, client, schema).await
}

#[async_trait]
impl SQLExecutor for FlightSQLExecutor {
    fn name(&self) -> &str {
        "flight_sql_executor"
    }
    fn compute_context(&self) -> Option<String> {
        Some(self.context.clone())
    }
    fn execute(&self, sql: &str, schema: SchemaRef) -> Result<SendableRecordBatchStream> {
        let future_stream =
            make_flight_sql_stream(sql.to_string(), self.client.clone(), Arc::clone(&schema));
        let stream = futures::stream::once(future_stream).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream,
        )))
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        let flight_info = self
            .client
            .clone()
            .get_tables(CommandGetTables {
                catalog: Some(self.catalog.to_string()),
                db_schema_filter_pattern: None,
                table_name_filter_pattern: None,
                table_types: vec![],
                include_schema: false,
            })
            .await?;
        let schema = Arc::new(
            flight_info
                .clone()
                .try_decode_schema()
                .map_err(arrow_error_to_df)?,
        );
        let stream = make_flight_stream(flight_info, self.client.clone(), schema.clone()).await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let batch = concat_batches(&schema, batches.iter())?;
        let names = batch.column_by_name("table_name").ok_or_else(|| {
            DataFusionError::Internal("table_name column should exist".to_string())
        })?;
        Ok(as_string_array(names)?
            .into_iter()
            .flatten()
            .map(|name| name.to_owned())
            .collect())
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
        let sql = format!("select * from {table_name} limit 1");
        let flight_info = self
            .client
            .clone()
            .execute(sql, None)
            .await
            .map_err(arrow_error_to_df)?;
        let schema = flight_info.try_decode_schema().map_err(arrow_error_to_df)?;
        Ok(Arc::new(schema))
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(DefaultDialect {})
    }
}

fn arrow_error_to_df(err: ArrowError) -> DataFusionError {
    DataFusionError::External(format!("arrow error: {err:?}").into())
}
