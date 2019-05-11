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
package qlikglue.encoder;

/**
 * Encoder-related properties;
 */
public class EncoderProperties {

    /**
     * The number of threads to have executing the encoding process.
     */
    public static final String ENCODER_THREADS = "qlikglue.encoder.threads";
    /**
     * The default number of encoder threads.
     */
    public static final String ENCODER_THREADS_DEFAULT = "2";
    /**
     * The class name of encoder we will utilize: AvroEncoder, JsonEncoder, etc.
     */
    public static final String ENCODER_CLASS = "qlikglue.encoder.class";

    /**
     * The delimiter to use in Delimited Text encoding. Specify as a number
     * that can be propertly parsed. This is because we want to support
     * not only "typical" delimiters (comma, semicolon), but also binary
     * delimiters such as the Hive default delimiter which is 001
     * (ASCII ctrl-A).
     */
    public static final String ENCODER_DELIMITER = "qlikglue.encoder.delimiter";
    /**
     * the default delimiter to use if one is not specified.
     */
    public static final String ENCODER_DELIMITER_DEFAULT = "001";

    /**
     * boolean: populate a "key" column
     */
    public static final String KEY_COLUMN = "qlikglue.encoder.key-column";
    /**
     * populate a "key" column default
     */
    public static final String KEY_COLUMN_DEFAULT = "false";
    /**
     * the name of the key column
     */
    public static final String KEY_COLUMN_NAME = "qlikglue.encoder.key-column-name";
    /**
     * the name of the key column
     */
    public static final String KEY_COLUMN_NAME_DEFAULT = "message_key";
    /**
     * boolean: populate table name column.
     */
    public static final String TABLENAME = "qlikglue.encoder.tablename";
    /**
     * table name column name to use.
     */
    public static final String TABLENAME_COLUMN = "qlikglue.encoder.tablename-col";
    /**
     * boolean: populate transaction id column.
     */
    public static final String TXID = "qlikglue.encoder.txid";
    /**
     * transaxction id column name to use.
     */
    public static final String TXID_COLUMN = "qlikglue.encoder.txid-col";
    /**
     * boolean: populate operation type column.
     */
    public static final String TX_OPTYPE = "qlikglue.encoder.tx-optype";
    /**
     * op type column name to use.
     */
    public static final String TX_OPTYPE_COLUMN = "qlikglue.encoder.tx-optype-name";
    /**
     * boolean: populate transaction timestamp column.
     */
    public static final String TX_TIMESTAMP = "qlikglue.encoder.tx-timestamp";
    /**
     * timestamp column name to use.
     */
    public static final String TX_TIMESTAMP_COLUMN = "qlikglue.encoder.tx-timestamp-name";
    /**
     * boolean: populate transaction relative position column.
     */
    public static final String TX_POSITION = "qlikglue.encoder.tx-position";
    /**
     * relative position column name to use.
     */
    public static final String TX_POSITION_COLUMN = "qlikglue.encoder.tx-position-name";
    /**
     * boolean: populate transaction user token column.
     */
    public static final String USERTOKEN = "qlikglue.encoder.user-token";
    /**
     * user token column name to use.
     */
    public static final String USERTOKEN_COLUMN = "qlikglue.encoder.user-token-name";
    /**
     * Replace newline characters in string fields with some other character.
     */
    public static final String REPLACE_NEWLINE = "qlikglue.encoder.replace-newline";
    /**
     * The default value for REPLACE_NEWLINE.
     */
    public static final String REPLACE_NEWLINE_DEFAULT = "false";
    /**
     * Replace newline characters in string fields with this character. The
     * default is " " (a blank). Override with this property if a different
     * character is desired.
     */
    public static final String NEWLINE_CHAR = "qlikglue.encoder.newline-char";
    /**
     * true = generate all json value fields as text strings.
     */
    public static final String JSON_TEXT = "qlikglue.encoder.json.text-only";
    /**
     * The default value for JSON_TEXT.
     */
    public static final String JSON_TEXT_DEFAULT = "true";
    /**
     * Send before images of data along.
     */
    public static final String INCLUDE_BEFORES = "qlikglue.encoder.include-befores";
    /**
     * The default value for INCLUDE_BEFORES.
     */
    public static final String INCLUDE_BEFORES_DEFAULT = "false";
    /**
     * Boolean: true = ignore operations where data is unchagned.
     */
    public static final String IGNORE_UNCHANGED = "qlikglue.encoder.ignore-unchanged";
    /**
     * The default value for IGNORE_UNCHANGED.
     */
    public static final String IGNORE_UNCHANGED_DEFAULT = "false";
    /**
     * Set the type we should encode numeric/decimal types to. We
     * need to have this flexibility because Avro does not support
     * these types directly, so users will have to decide how they
     * want this data represented on the other end: string, double,
     * float, etc.
     */
    public static final String NUMERIC_ENCODING = "qlikglue.encoder.numeric-encoding";
    public static final String NUMERIC_ENCODING_DEFAULT = "string";


    /*******************************************************************/
    /**
     * private to prevent explicit object creation
     */
    private EncoderProperties() { super(); }
}
