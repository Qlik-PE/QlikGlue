/* 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package qlikglue.source.kafka;

import qlikglue.QlikGluePropertyValues;
import qlikglue.QlikGlueVersion;
import qlikglue.common.JsonField;
import qlikglue.common.JsonHelper;
import qlikglue.common.PropertyManagement;
import qlikglue.encoder.EncoderFactory;
import qlikglue.encoder.EncoderType;
import qlikglue.encoder.ParallelEncoder;
import qlikglue.encoder.avro.AvroSchemaFactory;
import qlikglue.meta.schema.DownstreamColumnMetaData;
import qlikglue.meta.schema.DownstreamSchemaMetaData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qlikglue.meta.schema.DownstreamTableMetaData;
import qlikglue.meta.transaction.DownstreamColumnData;
import qlikglue.meta.transaction.DownstreamOperation;

import java.util.List;


/**
 * This class maps messages read from subscribed topics by the QlikGlueKafkaConsumer
 * to the QlikGlue data structures that will be used downstream.
 */
public class KafkaJsonHandler {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonHandler.class);
    private boolean includeBefores;
    private boolean ignoreUnchangedRows;

    private JsonHelper jsonHelper;
    private ParallelEncoder encoder;
    private DownstreamSchemaMetaData downstreamSchemaMetaData;
    private QlikGlueVersion version;
    private PropertyManagement properties = null;

    public KafkaJsonHandler() {
        super();
        version = new QlikGlueVersion();
        jsonHelper = new JsonHelper();
        LOG.info(version.format());
        init();
    }

    /**
     * Create the appropriate encoder and initialize. The encoder will initialize
     * the publishers as well.
     */
    public void init() {

        LOG.info("init(): initializing the client interface");

        // load all the properties for the app
        properties =
                PropertyManagement.getProperties(QlikGluePropertyValues.defaultProperties,
                        QlikGluePropertyValues.externalProperties);



        encoder = new ParallelEncoder();
        encoder.start();

        // create an instance of the class that we will use to keep
        // track of the schema meta data
        downstreamSchemaMetaData = new DownstreamSchemaMetaData();

        includeBefores =
                PropertyManagement.getProperties().asBoolean(QlikGluePropertyValues.INCLUDE_BEFORES,
                        QlikGluePropertyValues.INCLUDE_BEFORES_DEFAULT);
        ignoreUnchangedRows =
                PropertyManagement.getProperties().asBoolean(QlikGluePropertyValues.IGNORE_UNCHANGED,
                        QlikGluePropertyValues.IGNORE_UNCHANGED_DEFAULT);

        /*
         * all properties should be initialized by now. Print them out.
         */
        PropertyManagement.getProperties().printProperties();
    }


    /**
     * Perform all necessary clean up prior to exiting. This would
     * include draining queues, shutting down threads, etc.
     *
     * @throws InterruptedException if interrupted while shutting down
     */
    public void cleanup() throws InterruptedException {
        encoder.cancel();
    }

    /**
     * Report the status of this handler, including all encoder and
     * publisher threads running in BDGlue.
     *
     * @return throughput stats for the handler
     */

    public String reportStatus() {
        StringBuilder status = new StringBuilder();
        status.append("************* Begin GGHandlerClient Status *************\n");
        status.append(encoder.status());
        status.append("*************  End GGHandlerClient Status  *************\n");

        return status.toString();
    }

    /**
     * Process a new Json-formatted record. Presumption is that the topic is the table name.
     *
     * @param topic kafka topic name. Assumes one table per topic in the configuration.
     * @param partition the partition this record was fetched from.
     * @param offset the offset in the partition where this record was found.
     * @param key the kafka message key. Assumption is that this is based on the table's PK, so will be unique.
     * @param record the kafka message "value" ... i.e. the actual data.
     */
    public void newRecord(String topic, int partition, long offset, String key, String record) {
        String tableName;
        String opType;
        String timestamp;
        String position;
        String userTokens;
        String txId;

        tableName = topic;
        userTokens = "NONE";


        List<JsonField> parsedRecord = jsonHelper.parseRecord(tableName, record);
        DownstreamTableMetaData tableMetaData = getMetaData(tableName, parsedRecord);
        // hardcode for now.
        opType = "I";
        timestamp  = Long.toString(System.currentTimeMillis());
        position = Long.toString(offset);
        txId = String.format("%d-%s", partition, position);

        DownstreamOperation dbOperation =
                new DownstreamOperation(opType, timestamp, position, userTokens, txId, tableMetaData);
        setColumnData(dbOperation, parsedRecord, tableMetaData);

        /*
         * We've got the operation parsed and ready to process.
         * Send it on down the line.
         */
        try {
            encoder.put(dbOperation);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted Exception", e);
            reportStatus();
        }
    }

    /**
     * Get the metadata for a table, creating it from the current Json record if
     * the metadata hasn't yet been defined.
     *
     * @param tableName the "long" name for this table (i.e. schema.table format)
     * @param parsedRecord a parsed Json record. We will use the first record
     *                     we find to create the schema for this table. This implies
     *                     that we can't support schema evolution as of yet.
     * @return the metadata for this table
     */
    public DownstreamTableMetaData getMetaData(String tableName, List<JsonField> parsedRecord) {
        LOG.debug("setMetaData()");
        DownstreamTableMetaData downstreamTableMetaData;


        /*
         * for now, add the schema only if we don't have the table cached already.
         * This implies no schema evolution for Json messages at the present time.
         */
        if (!downstreamSchemaMetaData.containsTable(tableName)) {
            downstreamTableMetaData = addTable(tableName, parsedRecord);

            /**
             * TODO: figure out how to make this same decision without knowing about EncoderFactory.
             */
            if (EncoderFactory.getEncoderType() == EncoderType.AVRO_BYTE_ARRAY ||
                    EncoderFactory.getEncoderType() == EncoderType.AVRO_GENERIC_RECORD) {
                AvroSchemaFactory.getAvroSchemaFactory().metaDataChanged(downstreamTableMetaData);
            }
        } else {
            downstreamTableMetaData =  downstreamSchemaMetaData.getTableMetaData(tableName);
        }

        return downstreamTableMetaData;
    }


    /**
     * Add a new table definition to the list of tables that BDGlue
     * keeps track of.
     *
     * @param longTableName the key that will be used in the map for looking
     *            up table information
     * @param parsedRecord a parsed Json message as a List.
     * @return the metadata for the table we added / replaced.
     */
    private DownstreamTableMetaData addTable(String longTableName, List<JsonField> parsedRecord) {
        DownstreamTableMetaData downstreamTable;
        String key = longTableName;
        // get just the table name without the "schema." prefix.
        String tableName = longTableName.substring(longTableName.lastIndexOf('.')+1);

        downstreamTable = new DownstreamTableMetaData(key, tableName, longTableName);
        setColumnMetaData(downstreamTable, parsedRecord);

        downstreamSchemaMetaData.addTable(longTableName, downstreamTable);
        return downstreamTable;
    }


    /**
     * Set the column metadata for the table.
     *
     * @param table the downstream meta data for the table
     * @param parsedRecord a  parsedJson message we will use to decipher the structure
     */
    private void setColumnMetaData(DownstreamTableMetaData table, List<JsonField> parsedRecord) {
        DownstreamColumnMetaData downstreamColumnMetaData;
        String columnName;
        boolean keyCol;
        boolean nullable;
        int jdbcType;


        for (JsonField jsonField : parsedRecord) {
            columnName = jsonField.getName();

            // always fales for now
            keyCol = false;
            nullable = true;

            jdbcType = jsonField.getType();
            LOG.trace("**** columnName: {}   type: {}", columnName, jdbcType);
            downstreamColumnMetaData = new DownstreamColumnMetaData(columnName, keyCol, nullable, jdbcType);
            table.addColumn(downstreamColumnMetaData);
        }
    }


    /**
     * Set the column data values for the current operation.
     *
     * @param downstreamOp the operation we are encoding into
     * @param parsedRecord the parsed Json record for this operation
     */
    private void setColumnData(DownstreamOperation downstreamOp, List<JsonField> parsedRecord,
                               DownstreamTableMetaData tableMetaData) {

        DownstreamColumnData downstreamCol;
        DownstreamColumnMetaData columnMetaData;

        String columnName;
        String stringValue;
        byte[] binaryValue;
        boolean keyCol;
        for(JsonField column : parsedRecord) {
            columnName = column.getName();
            columnMetaData = tableMetaData.getColumn(columnName);
            keyCol = columnMetaData.isKeyCol();

            stringValue = column.getValue();

            // hardcode for now
            binaryValue = null;

            downstreamCol = new DownstreamColumnData(columnName, stringValue,
                    binaryValue, keyCol);
            downstreamOp.addColumn(downstreamCol);
        }
    }
}
