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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;


/**
 * A helper class for parsing Json messages.
 */
public class JsonHelper {
    private static final Logger LOG = LoggerFactory.getLogger(JsonHelper.class);
    JsonFactory factory;

    /**
     * Constructor for this class.
     */
    public JsonHelper() {
        this.factory = new JsonFactory();
    }

    /**
     * Parse a Json record.
     *
     * @param tableName  the name of the table
     * @param record a Json-formatted record to be parsed
     *
     * @return the parsed record as a List
     */
    public List<JsonField> parseRecord(String tableName, String record) {
        ArrayList<JsonField> parsedRecord = new ArrayList<>();
        String data;
        String columnName;
        int jdbcType;
        try {
            JsonParser parser = factory.createParser(record);
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                // This approach does not support arrays and other
                // complex types. Shouldn't be an issue in this case.
                if (parser.getCurrentToken() == JsonToken.FIELD_NAME) {
                    parser.nextToken();
                    columnName = parser.getCurrentName();
                    data = parser.getText();
                    switch (parser.getCurrentToken()) {
                        case VALUE_TRUE:
                        case VALUE_FALSE:
                            jdbcType = Types.BOOLEAN;
                            break;
                        case VALUE_NUMBER_FLOAT:
                            // TODO: perhaps need to worry about
                            // double, float, Decimal???
                            if (data.toUpperCase().indexOf('E') >= 0) {
                                // scientific notation, so type is FLOAT
                                jdbcType = Types.FLOAT;
                            } else {
                                jdbcType = Types.DECIMAL;
                            }
                            break;
                        case VALUE_NUMBER_INT:
                            // TODO: perhaps need to worry about
                            // int, long, etc.?
                            jdbcType = Types.INTEGER;
                            break;
                        case VALUE_NULL: // can't determine type, so default to VARCHAR;
                        case VALUE_STRING:
                            if (NumberUtils.isNumber(data)) {
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
                            continue;
                    }
                    parsedRecord.add(new JsonField(columnName, data, jdbcType));
                }
            }
            parser.close();
        } catch (JsonParseException e) {
            LOG.error("Json parser error", e);
        } catch (IOException e) {
            LOG.error("Json parser I/O exception", e);
        }
        return parsedRecord;
    }

}
