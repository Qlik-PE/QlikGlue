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
 * Event-related properties.
 */
public class EventProperties {
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

    /*******************************************************************/
    /**
     * private to prevent explicit object creation
     */
    private EventProperties() { super(); }
}
