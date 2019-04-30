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
package qlikglue.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class JsonRecord {
    private static final Logger LOG = LoggerFactory.getLogger(JsonRecord.class);
    private static final int UNSUPPORTED = -9999;
    private static final String HEADER = "headers";
    private static final String DATA = "data";
    private static final String BEFORE = "beforeData";
    private static final String OPTYPE = "operation";
    private static final String TIMESTAMP = "timestamp";
    private static final String POSITION = "streamPosition";
    private static final String TXID = "transactionId";
    private static final String CSN = "changeSequence";

    private ObjectMapper mapper;
    JsonNode rootNode;

    // record data
    private String tableName;
    private String opType;
    private String timestamp;
    private String position;
    private String userTokens;
    private String txId;
    private String changeSequence;
    private List<JsonField> data;
    private List<JsonField> before = null;


    /**
     * Parse this Json record.
     * @param topic the Kafka topic name.
     * @param partition the kafka partition id
     * @param offset the offset in the partition
     * @param key the kafka message key
     * @param record the Json record we want to parse.
     */
    public JsonRecord(String topic, int partition, long offset, String key, String record) {
        mapper = new ObjectMapper();
        try {
            rootNode = mapper.readTree(record);
        } catch (IOException e) {
           LOG.error("Json parse error", e);
        }

        tableName = topic;
        JsonNode headerNode = rootNode.findValue(HEADER);
        if ((headerNode != null) && (headerNode.getNodeType() == JsonNodeType.OBJECT)) {
            // We have a "header" node, so get the header values.
            opType = headerNode.findValue(OPTYPE).asText();
            timestamp = headerNode.findValue(TIMESTAMP).asText();
            position = headerNode.findValue(POSITION).asText();
            txId = headerNode.findValue(TXID).asText();
            changeSequence = headerNode.findValue(CSN).asText();
            userTokens = "NONE";
        } else {
            // no header record, so hard code for the time being.
            opType = "I";
            timestamp  = Long.toString(System.currentTimeMillis());
            position = Long.toString(offset);
            txId = String.format("%d-%s", partition, position);
            userTokens = "NONE";
        }

        JsonField keyField = new JsonField("message_key", key, Types.VARCHAR);
        JsonNode dataNode = rootNode.findValue(DATA);
        if ((dataNode != null) && (dataNode.getNodeType() == JsonNodeType.OBJECT)) {
            // We have a "data" node, so get the values of the columns.
            data = parseRecord(dataNode);

            JsonNode beforeNode = rootNode.findValue(BEFORE);
            if ((beforeNode != null) && (beforeNode.getNodeType() == JsonNodeType.OBJECT)) {
                // We have a "before" node, so get the before images of the columns.
                before = parseRecord(beforeNode);
            }
        } else {
            // didn't find subrecords, so assume this is flat
            data = parseRecord(rootNode);
        }
        data.add(keyField);

    }

    /**
     * Parse this record or sub record.
     * @param baseNode
     * @return a list of JsonFields
     */
    private List<JsonField> parseRecord(JsonNode baseNode) {
        List<JsonField> list = new ArrayList<>();
        Iterator<String> itr = baseNode.fieldNames();
        while(itr.hasNext()) {
            String colName = itr.next();
            JsonNode columnNode = baseNode.get(colName);
            String value = columnNode.asText();
            JsonNodeType nodeType = columnNode.getNodeType();
            int jdbcType = getJdbcType(colName, value, nodeType);
            if (jdbcType != UNSUPPORTED) {
                list.add(new JsonField(colName, value, jdbcType));
            }
        }

        return list;
    }

    /**
     * Json is somewhat typeless. Make an interpreted guess as to the type of this column.
     * @param columnName
     * @param value
     * @param nodeType
     * @return the jdbc type
     */
    private int getJdbcType(String columnName, String value, JsonNodeType nodeType) {
        int jdbcType;
        switch (nodeType) {
            case BOOLEAN:
                jdbcType = Types.BOOLEAN;
                break;
            case NUMBER:
                // TODO: perhaps need to worry about
                // double, float, Decimal???
                if (value.toUpperCase().indexOf('E') >= 0) {
                    // scientific notation, so type is FLOAT
                    jdbcType = Types.FLOAT;
                } else {
                    jdbcType = Types.DECIMAL;
                }
                break;
            case NULL: // can't determine type, so default to VARCHAR;
                jdbcType = Types.VARCHAR;
                break;
            case STRING:
                if (NumberUtils.isNumber(value)) {
                    jdbcType = Types.DECIMAL;
                }
                else {
                    jdbcType = Types.VARCHAR;
                }
                break;
            default:
                // not a token we care about right now
                LOG.error("Unhandled Json value type encountered during parsing for table {} column {}",
                        tableName, columnName);
                jdbcType = UNSUPPORTED;
                break;
        }
        return jdbcType;
    }

    /**
     * Get the table name
     * @return the table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Get the operation Type
     * @return the operation type
     */
    public String getOpType() {
        return opType;
    }

    /**
     * Get the operation timestamp.
     * @return the operation timestamp
     */
    public String getTimestamp() {
        return timestamp;
    }

    /**
     * Get the position of this record in the input stream..
     * @return the position
     */
    public String getPosition() {
        return position;
    }

    /**
     * Get any user tokens.
     * @return the user tokens
     */
    public String getUserTokens() {
        return userTokens;
    }

    /**
     * Get the transaction ID.
     * @return the transaction id.
     */
    public String getTxId() {
        return txId;
    }
    /**
     * Get the change sequence number.
     * @return the csn
     */
    public String getChangeSequence() { return changeSequence; }


    /**
     * Get a list of data fields from the Json record
     * @return the data fields
     */
    public List<JsonField> getData() {
        return data;
    }

    /**
     * Get a list of before image values.
     * @return the before image fields
     */
    public List<JsonField> getBefore() {
        return before;
    }

}
