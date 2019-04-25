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

import qlikapi.QlikSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qlikglue.common.PropertyManagement;

import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Singleton instance that consolidates buffers built by instances of QlikTable.
 */
public class QlikLoad {
    private static QlikLoad instance = null;
    private QlikSocket qlikSocket;
    private static final Logger LOG = LoggerFactory.getLogger(QlikLoad.class);
    private static final boolean testOnly = true;
    private int totalOps = 0;
    private String threadName;
    private ByteArrayOutputStream baos;

    // properties
    String URI;
    int maxBufferSize;

    private QlikLoad(String threadName) {
        this.threadName = threadName;
        baos = new ByteArrayOutputStream(65536);
        qlikSocket = new QlikSocket();
    }

    /**
     * Return the singleton instance of this class
     * @return
     */
    public static QlikLoad getInstance() {
        if (instance == null) {
            synchronized (QlikLoad.class) {
                // double-check because of potential for race
                if (instance == null) {
                    instance = new QlikLoad("QLikSocket");
                }
            }
        }
        return instance;
    }

    /**
     * Call this method to terminate the thread and exit.
     */
    public void cleanup() {
        sendBuffer();
        shutdown();
    }

    /**
     * Send the contents of baos to the web socket
     */
    public void sendBuffer() {
        synchronized (baos) {
            if (baos.size() > 0) {
                if (!testOnly) {
                    sendLoadScript(baos.toString());
                } else {
                    logToFile();
                }
                baos.reset();
            }
        }
    }

    /**
     * Append the the load script from a table to the output buffer.
     *
     * @param qlikTableBaos
     */
    public void appendBuffer(ByteArrayOutputStream qlikTableBaos) {
        synchronized (baos) {
            totalOps++;
            try {
                qlikTableBaos.writeTo(baos);
                if (baos.size() >= maxBufferSize) {
                    sendBuffer();
                    resetBuffer();
                }
            } catch (IOException e) {
                LOG.error("error adding message", e);
            }
        }
    }

    private void shutdown() {
        LOG.info("shutting down QlikLoad");

        // pause briefly to allow things to drain
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            LOG.trace("shutdown() timer");
        }

        qlikSocket.shutdown();

        if ((baos.size() != 0)) {
            LOG.warn("shutdown(): Thread {} baos HAS NOT been drained. Size: {}",
                    threadName, baos.size());
        } else {
            LOG.info("shutdown(): Thread {} baos has been drained. Size: {}",
                    threadName, baos.size());
        }

    }

    /**
     * Send the load script to Qlik
     *
     * @param script
     */
    private void sendLoadScript(String script) {
        System.out.println("Need to set sendLoadScript!!!");
    }


    /**
     * for testing purposes ... log to a file instead of sending to a socket.
     */
    private void logToFile() {
        String filename = "/tmp/QlikLoad" + totalOps;
        try(OutputStream outputStream = new FileOutputStream(filename)) {
            System.out.println("logging BAOS content to file: " + filename);
            baos.writeTo(outputStream);
        } catch (FileNotFoundException e) {
            LOG.error("error writing buffer to file", e);
        } catch (IOException e) {
            LOG.error("IOException when writing buffer to file", e);
        }
    }

    /**
     * Reset the output buffer to prepare for the next batch.
     */
    private void resetBuffer() {
        baos.reset();
    }


}
