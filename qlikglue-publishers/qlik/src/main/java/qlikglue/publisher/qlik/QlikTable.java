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
package qlikglue.publisher.qlik;

import qlikglue.common.PropertyManagement;
import qlikglue.meta.schema.DownstreamColumnMetaData;
import qlikglue.meta.transaction.DownstreamColumnData;
import qlikglue.meta.transaction.DownstreamOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.io.output.ByteArrayOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class for delivering to a single table in Qlik.
 */
public class QlikTable {
    private static final Logger LOG = LoggerFactory.getLogger(QlikTable.class);
    
    private int opCounter = 0;
    private int totalOps = 0;
    private int batchSize = 0;
    private boolean insertOnly;
    private boolean bufferEmpty;
    private int flushFreq = 500;
    private Timer timer;
    private TimerTask timerTask;
    private ByteArrayOutputStream baos;
    private static final String NEWLINE = System.getProperty("line.separator");
    private static final String SINGLE_QUOTE = "\'";
    private static final String DOUBLE_QUOTE = "\"";
    

    private String tableName;
    

    /**
     * Construct an instance that represents this table.
     * 
     * @param tableName the name of the table
     */
    public QlikTable(String tableName) {
        super();
        
        this.tableName = tableName;
        baos = new ByteArrayOutputStream();
        timer = new Timer();
        init();
    }
    
    private void init() {
        PropertyManagement properties = PropertyManagement.getProperties();
        
        batchSize =
            properties.asInt(QlikPublisherPropertyValues.QLIK_BATCH_SIZE,
                             QlikPublisherPropertyValues.QLIK_BATCH_SIZE_DEFAULT);
        flushFreq =
            properties.asInt(QlikPublisherPropertyValues.QLIK_FLUSH_FREQ,
                             QlikPublisherPropertyValues.QLIK_FLUSH_FREQ_DEFAULT);
        insertOnly =
            properties.asBoolean(QlikPublisherPropertyValues.QLIK_INSERT_ONLY,
                                 QlikPublisherPropertyValues.QLIK_INSERT_ONLY_DEFAULT);
        
        /*
         * Currently, best practice recommendation from Google is to create something similar to
         * an audit table that will be post processed into final destination via an ETL job.
         */
        if (insertOnly == false) {
            LOG.warn("QlikTable(): updates and deletes not currently supported. Defaulting to insertOnly");
            insertOnly = true;
        }

        // reinitialize things
        resetBuffer();
        publishEvents();
    }


    /**
     * Add a row to our current batch.
     *
     * @param op  the parsed but unformatted operation data
     */
    public void addOperation(DownstreamOperation op) {

            synchronized (this) {


                if (LOG.isDebugEnabled()) {
                    LOG.debug("event #{}", totalOps);
                }
                if (bufferEmpty) {
                    bufferEmpty = false;
                    writeBuffer(String.format("%s: %nADD LOAD * INLINE [%n%s%n", tableName, formatHdr(op)));
                }

                writeBuffer(formatRow(op));

                opCounter++;
                totalOps++;
                // publish batch and commit.
                if (opCounter >= batchSize) {
                    publishEvents();
                }
            }


        return;
    }

    private void resetBuffer() {
        baos.reset();
        bufferEmpty = true;
    }

    private void writeBuffer(String s) {
        try {
            baos.write(s.getBytes());
        } catch (IOException e) {
            LOG.error("write error", e);
        }
    }

    private String formatRow(DownstreamOperation op) {
        StringBuilder rowContent = new StringBuilder(256);

        /*
         * populate the meta columns requested in the properties (op type, timestamp, etc.)
         */
        char comma = '\0';
        for(Map.Entry<String, String> opMeta : op.getOpMetadata().entrySet()) {
            if (comma != '\0') rowContent.append(comma);
            rowContent.append(opMeta.getValue());
            comma = ',';
        }


        ArrayList<DownstreamColumnMetaData> colsMeta = op.getTableMeta().getColumns();
        ArrayList<DownstreamColumnData> cols = op.getColumns();


        DownstreamColumnData col;
        DownstreamColumnMetaData meta;
        String value;
        comma = '\0';
        for (int i = 0; i < cols.size(); i++) {
            col = cols.get(i);
            meta = colsMeta.get(i);
            /*
             * col data and meta data should always be in the same order.
             * I would check, but we will be doing this for every column on every
             * row, which would be very expensive.
             */
            value = getDataValue(meta.getJdbcType(), col);
            if (comma != '\0') rowContent.append(comma);
            rowContent.append(value);
            comma = ',';
        }
        rowContent.append(NEWLINE);

        return rowContent.toString();
    }

    private String formatHdr(DownstreamOperation op) {
        StringBuilder hdrContent = new StringBuilder(256);

        /*
         * populate the meta columns requested in the properties (op type, timestamp, etc.)
         */
        char comma = '\0';
        for(Map.Entry<String, String> opMeta : op.getOpMetadata().entrySet()) {
            if (comma != '\0') hdrContent.append(comma);
            hdrContent.append(opMeta.getKey());
            comma = ',';
        }


        ArrayList<DownstreamColumnMetaData> colsMeta = op.getTableMeta().getColumns();
        ArrayList<DownstreamColumnData> cols = op.getColumns();


        DownstreamColumnData col;
        DownstreamColumnMetaData meta;
        String value;
        comma = '\0';
        for (int i = 0; i < cols.size(); i++) {
            col = cols.get(i);
            meta = colsMeta.get(i);
            /*
             * col data and meta data should always be in the same order.
             * I would check, but we will be doing this for every column on every
             * row, which would be very expensive.
             */
            value = getDataValue(meta.getJdbcType(), col);
            if (comma != '\0') hdrContent.append(comma);
            hdrContent.append(col.getBDName());
            comma = ',';
        }

        return hdrContent.toString();
    }

    /**
     * Get the value of the column as an unquoted string if it is a numeric
     * value of some sort, and as a quoted string if it is not.
     *
     * @param jdbcType the SQL type
     * @param col the column data
     * @return return the value for the column, null if not supported.
     */
    private String getDataValue(int jdbcType, DownstreamColumnData col) {
        String value;

        switch (jdbcType) {
            case java.sql.Types.BOOLEAN:
            case java.sql.Types.BIT:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
            case java.sql.Types.INTEGER:
            case java.sql.Types.BIGINT:
            case java.sql.Types.REAL: // JDBC says 7 digits of mantisa
            case java.sql.Types.FLOAT: // JDBC says 15 digits of mantisa
            case java.sql.Types.DOUBLE:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.DECIMAL:
                value = col.asString();
                break;
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.NCHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.CLOB:
            case java.sql.Types.LONGNVARCHAR:
            case java.sql.Types.NCLOB:
            case java.sql.Types.BLOB:
            case java.sql.Types.BINARY:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
                value = col.asQuotedString();
                break;
            case java.sql.Types.DATALINK:
            case java.sql.Types.DISTINCT:
            case java.sql.Types.JAVA_OBJECT:
            case java.sql.Types.NULL:
            case java.sql.Types.ROWID:
            case java.sql.Types.SQLXML:
            case java.sql.Types.STRUCT:
            case java.sql.Types.ARRAY:
            case java.sql.Types.OTHER:
            case java.sql.Types.REF:
            default:
                value = null;
                break;
        }
        return value;
    }


    /**
     * Flush any queued events and clean up timer in
     * preparation for shutdown.
     */
    public void cleanup() {
        LOG.info("Cleaning up table {} in preparation for shutdown", tableName);
        // flush any pending events
        publishEvents();
        // clean up the timer
        timer.cancel();
        
    }
    
    /**
     * Simple timer to ensure that we periodically flush whatever we have queued
     * in the event that we haven't received "batchSize" events by the time
     * that the timer has expired.
     */
    private class FlushQueuedEvents extends TimerTask {
        public void run() {

            publishEvents();
        }
    }

    /**
     * publish all events that we have queued up to Qlik. This is called both by
     * the timer and by writeEvent(). Need to be sure they don't step on each other.
     */
    private void publishEvents() {

        synchronized (this) {
            if (timerTask != null) {
                timerTask.cancel();
                timerTask = null;
            }
            if (opCounter > 0) {
                deliverBatch();
                opCounter = 0;
                writeBuffer("]"+ NEWLINE);
                System.out.println(baos.toString()); //  dump to console for now
                resetBuffer();
            }

            // ensure that we don't keep queued events around very long.
            timerTask = new FlushQueuedEvents();
            timer.schedule(timerTask, flushFreq);
        }
    }


    private void deliverBatch() {
        // write the batch to Qlik
    }



}
