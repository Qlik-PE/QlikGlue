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
package qlikglue;

/**
 * This class contains configuration constants used by the
 * Big Data Glue code, including property names and where appropriate
 * the default values to use in the event that a property
 * is not defined.
 * 
 */
public final class QlikGluePropertyValues {
    /**
     * The name of the default properties "resource" to look for.
     */
    public static final String defaultProperties = "/qlikglueDefault.properties";
    /**
     * The external properties file to look for. These properties will override
     * the default properties.
     */
    public static final String externalProperties = "qlikglue.properties";
    
   
        
    /*************************************/
    /*** Encoder-related properties    ***/
    /*************************************/
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
    
    /***********************************/
    /** Event-related properties    ***/
    /***********************************/
    /**
     * Boolean as to whether or not to include the operation type in 
     * the event header information.
     */
    public static final String HEADER_OPTYPE = "qlikglue.event.header-optype";
    /**
     * Boolean as to whether or not to include the transaction timestamp in 
     * the event header information.
     */
    public static final String HEADER_TIMESTAMP = "qlikglue.event.header-timestamp";
    /**
     * Boolean as to whether or not to include a value for the row's key as
     * a concatenation of the key columns in the event header information. 
     * HBase and NoSQL KV API need the this. It is also needed if the publisher
     * hash is based on key rather than table name.
     */
    public static final String HEADER_ROWKEY = "qlikglue.event.header-rowkey";
    /**
     * Boolean as to whether or not to include the "long" table name in the header. 
     * FALSE will cause the "short" name to be included. Most prefer the long name.
     * HBase and NoSQL prefer the short name.
     */
    public static final String HEADER_LONGNAME = "qlikglue.event.header-longname";
    /**
     * Boolean as to whether or not to include a "columnFamily" value in the header. This
     * is needed for Hbase.
     */
    public static final String HEADER_COLUMNFAMIILY = "qlikglue.event.header-columnfamily";
    /** 
     * Boolean as to whether or not to include the path to the Avro schema file in
     * the header. This is needed for Avro encoding where Avro-formatted files are created 
     * in HDFS, including those that will be leveraged by Hive.
     */
    public static final String HEADER_AVROPATH = "qlikglue.event.header-avropath";
    /**
     * The URL in HDFS where Avro schemas can be found.
     */
    public static final String AVRO_SCHEMA_URL = "qlikglue.event.avro-hdfs-schema-path";
    
    /**
     * boolean on whether or not to generate the avro schema on the fly.
     * This is really intended for testing and should likely always be false.
     */
    public static final String GENERATE_AVRO_SCHEMA = "qlikglue.event.generate-avro-schema";
    /**
     * The namespace to use in avro schemas if the actual table schema name
     * is not present.
     */
    public static final String AVRO_NAMESPACE = "qlikglue.event.avro-namespace";
    /**
     * The path on local disk where we can find the avro schemas and/or
     * where they will be written if we generate them.
     */
    public static final String AVRO_LOCAL_PATH = "qlikglue.event.avro-schema-path";
    
    /***************************************/
    /*** Publisher-related properties    ***/
    /***************************************/
    /**
     * the name of the implementation of Publisher that should be called.
     */
    public static final String PUBLISHER_CLASS = "qlikglue.publisher.class";
    
    /**
     * The number of threads to have executing the publishing process.
     */
    public static final String PUBLISHER_THREADS = "qlikglue.publisher.threads";
    /**
     * The default number of publisher threads.
     */
    public static final String PUBLISHER_THREADS_DEFAULT = "2";
    /** 
     * Select publisher thread based on hash of table name or rowkey.
     */
    public static final String PUBLISHER_HASH = "qlikglue.publisher.hash";
    
    
    
  
    
    /*******************************************************************/
    /**
     * private to prevent explicit object creation
     */
    private QlikGluePropertyValues() {
        super();
    }
}
