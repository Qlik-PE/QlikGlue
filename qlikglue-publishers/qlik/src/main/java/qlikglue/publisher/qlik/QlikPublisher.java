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

import qlikglue.common.DataAccumulator;
import qlikglue.common.PropertyManagement;
import qlikglue.encoder.EventData;
import qlikglue.encoder.EventHeader;
import qlikglue.meta.transaction.DownstreamOperation;
import qlikglue.publisher.QlikGluePublisher;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publish event data to Qlik. 
 *
 */
public class QlikPublisher extends DataAccumulator implements QlikGluePublisher {
    private static final Logger LOG = LoggerFactory.getLogger(QlikPublisher.class);

    private int batchSize;
    private int flushFreq;
    private boolean insertOnly;
    private int opCount = 0;
    private int totalOps = 0;
    private ConcurrentHashMap<String, QlikTable> tables;
    private QlikLoad qlikLoad;



    public QlikPublisher() {
        super();

        tables = new ConcurrentHashMap<>();
        qlikLoad = QlikLoad.getInstance();
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

        setBatchSize(batchSize);
        setFlushFreq(flushFreq);

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

    protected int bufferSize() {
        return opCount;
    }

    protected void resetBuffer() {
        opCount = 0;
    }

    protected void sendBuffer() {
        // send any queued operations on to QlikSocket
        for(QlikTable qlikTable : tables.values()) {
            qlikTable.sendBuffer();
        }
        // send the buffer to the target
        qlikLoad.sendBuffer();

        resetBuffer();
    }


    @Override
    public void connect() {
        // NO OP for now. Connections are dynamically created.
        logQlikInfo();
    }
    

    @Override
    public void cleanup() {
        LOG.info("disconnecting from Qlik");

        for(QlikTable qlikTable : tables.values()) {
            qlikTable.cleanup();
        }
        publishEvents();
        qlikLoad.cleanup();

        super.cleanup();
        // TODO: there does not appear to be a close / disconnect API. Confirm.
        //qlik.close();
    }

    @Override
    public void writeEvent(String threadName, EventData evt) {
        QlikTable qlikTable;

        String tableName = evt.getMetaValue(EventHeader.TABLE);
        //tableName = tableName.replace('.','_');

        if (!tables.containsKey(tableName)) {
            // table hasn't yet been processed.
            LOG.info("writeEvent(): Processing table {} for the first time", tableName);
            qlikTable = new QlikTable(tableName);
            tables.put(tableName, qlikTable);
        } else {
            qlikTable = tables.get(tableName);
        }
        
        qlikTable.addOperation((DownstreamOperation)evt.eventBody());
        opCount++;
        totalOps++;
    }
    


    /**
     * Log the information related to our Qlik connection.
     */
    private void logQlikInfo() {
        LOG.info("*** Qlik information ***");
        LOG.info("Qlik Connection Info: {}", "undefined");
        LOG.info("*** END Qlik information ***");
    }
}
