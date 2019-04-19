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

import qlikglue.encoder.EventData;
import qlikglue.encoder.EventHeader;
import qlikglue.meta.transaction.DownstreamOperation;
import qlikglue.publisher.QlikGluePublisher;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publish event data to Qlik. 
 *
 */
public class QlikPublisher implements QlikGluePublisher {
    private static final Logger LOG = LoggerFactory.getLogger(QlikPublisher.class);


    private HashMap<String, QlikTable> tables;



    public QlikPublisher() {
        super();

        tables = new HashMap<>();
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
