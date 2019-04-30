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

import qlikglue.meta.schema.DownstreamColumnMetaData;
import qlikglue.meta.transaction.DownstreamColumnData;
import qlikglue.meta.transaction.DownstreamOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

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
    private boolean bufferEmpty;
    private ByteArrayOutputStream baos;
    private QlikLoad qlikLoad;
    private static final String NEWLINE = System.getProperty("line.separator");
    private static final char EMPTY ='\0';
    private static final char COMMA =',';


    private String tableName;
    

    /**
     * Construct an instance that represents this table.
     * 
     * @param tableName the name of the table
     */
    public QlikTable(String tableName) {
        super();
        
        this.tableName = tableName;
        qlikLoad = QlikLoad.getInstance();
        baos = new ByteArrayOutputStream();
        init();
    }
    
    private void init() {
        // reinitialize things
        resetBuffer();
        sendBuffer();
    }


    /**
     * Flush any queued events so we can prepare for shutdown.
     */
    public void cleanup() {
        LOG.info("Cleaning up table {} in preparation for shutdown", tableName);
        // flush any pending events
        sendBuffer();
    }

    /**
     * Flush all events that we have queued up to Qlik.
     */
    public void sendBuffer() {
        if (opCounter > 0) {
            appendBUffer("];" + NEWLINE + NEWLINE);
            qlikLoad.appendBuffer(baos);
            opCounter = 0;
            resetBuffer();
        }
    }


    /**
     * Add a row to our current batch.
     *
     * @param op  the parsed but unformatted operation data
     */
    public void addOperation(DownstreamOperation op) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("event #{}", totalOps);
        }
        if (op.getOpTypeId() == DownstreamOperation.REFRESH_ID) {
            TruncateLatch.getInstance().getLatch();
        } else {
            TruncateLatch.getInstance().clearLatch();
        }
        if (bufferEmpty) {
            bufferEmpty = false;
            //appendBUffer(String.format("%s: %nADD LOAD * INLINE [%n%s%n", tableName, formatHdr(op)));
            appendBUffer(String.format("%s: %nADD ONLY LOAD * INLINE [%n%s%n", tableName, formatHdr(op)));
        }

        appendBUffer(formatRow(op));

        opCounter++;
        totalOps++;
    }

    /**
     * Reset our output buffer to prepare for the next batch.
     */
    private void resetBuffer() {
        baos.reset();
        bufferEmpty = true;
    }

    /**
     * Append this string to the output buffer.
     * @param s
     */
    private void appendBUffer(String s) {
        try {
            baos.write(s.getBytes());
        } catch (IOException e) {
            LOG.error("write error", e);
        }
    }

    /**
     * Format a delimited row of data values
     * @param op a DownStreamOperation from the publisher
     * @return the row as a String
     */
    private String formatRow(DownstreamOperation op) {
        StringBuilder rowContent = new StringBuilder(256);

        /*
         * populate the meta columns requested in the properties (op type, timestamp, etc.)
         */
        char delimiter = EMPTY;
        for(Map.Entry<String, String> opMeta : op.getOpMetadata().entrySet()) {
            if (delimiter != EMPTY) {rowContent.append(delimiter);}
            rowContent.append(opMeta.getValue());
            delimiter = COMMA;
        }


        ArrayList<DownstreamColumnMetaData> colsMeta = op.getTableMeta().getColumns();
        ArrayList<DownstreamColumnData> cols = op.getColumns();


        DownstreamColumnData col;
        DownstreamColumnMetaData meta;
        String value;
        delimiter = EMPTY;
        for (int i = 0; i < cols.size(); i++) {
            col = cols.get(i);
            meta = colsMeta.get(i);
            /*
             * col data and meta data should always be in the same order.
             * I would check, but we will be doing this for every column on every
             * row, which would be very expensive.
             */
            value = getDataValue(meta.getJdbcType(), col);
            if (delimiter != EMPTY) rowContent.append(delimiter);
            rowContent.append(value);
            delimiter = COMMA;
        }
        rowContent.append(NEWLINE);

        return rowContent.toString();
    }

    /**
     * Format a delimited row of header (i.e. column name) values
     * @param op a DownStreamOperation from the publisher
     * @return the header row as a String
     */
    private String formatHdr(DownstreamOperation op) {
        StringBuilder hdrContent = new StringBuilder(256);

        /*
         * populate the meta columns requested in the properties (op type, timestamp, etc.)
         */
        char delimiter = EMPTY;
        for(Map.Entry<String, String> opMeta : op.getOpMetadata().entrySet()) {
            if (delimiter != EMPTY) hdrContent.append(delimiter);
            hdrContent.append(opMeta.getKey());
            delimiter = COMMA;
        }


        ArrayList<DownstreamColumnMetaData> colsMeta = op.getTableMeta().getColumns();
        ArrayList<DownstreamColumnData> cols = op.getColumns();


        DownstreamColumnData col;
        delimiter = EMPTY;
        for (int i = 0; i < cols.size(); i++) {
            col = cols.get(i);
            /*
             * col data and meta data should always be in the same order.
             * I would check, but we will be doing this for every column on every
             * row, which would be very expensive.
             */
            if (delimiter != EMPTY) hdrContent.append(delimiter);
            hdrContent.append(col.getBDName());
            delimiter = COMMA;
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

}
