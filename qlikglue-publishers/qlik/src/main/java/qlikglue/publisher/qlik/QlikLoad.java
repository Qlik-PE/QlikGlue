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

import qlikapi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.output.ByteArrayOutputStream;
import qlikglue.common.PropertyManagement;

import java.io.*;

import static java.lang.Integer.min;

/**
 * Singleton instance that consolidates buffers built by instances of QlikTable.
 */
public class QlikLoad {
    private static QlikLoad instance = null;
    private QlikSocket qlikSocket;
    private DocApi docApi;
    private GlobalApi globalApi;
    private static final Logger LOG = LoggerFactory.getLogger(QlikLoad.class);
    private static final boolean testOnly = false;
    private int totalOps = 0;
    private String threadName;
    private ByteArrayOutputStream baos;

    // properties
    private int maxBufferSize;
    private String appName;
    private String URI;

    private QlikLoad(String threadName) {
        PropertyManagement properties = PropertyManagement.getProperties();
        this.threadName = threadName;
        baos = new ByteArrayOutputStream(65536);



        maxBufferSize = properties.asInt(QlikLoadProperties.QLIKLOAD_MAXBUFFERSIZE,
                QlikLoadProperties.QLIKLOAD_MAXBUFFERSIZE_DEFAULT);
        URI = properties.getProperty(QlikLoadProperties.QLIKLOAD_URL,
                QlikLoadProperties.QLIKLOAD_URL_DEFAULT);
        appName = properties.getProperty(QlikLoadProperties.QLIKLOAD_APPNAME,
                QlikLoadProperties.QLIKLOAD_APPNAME_DEFAULT);

        qlikSocket = new QlikSocket(URI);
        globalApi = new GlobalApi();
        docApi = new DocApi();
    }

    /**
     * Return the singleton instance of this class
     * @return the singleton value
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
     * Send the contents of baos to the web socket
     */
    public void sendBuffer() {
        synchronized (baos) {
            if (baos.size() > 0) {
                if (!testOnly) {
                    String s;
                    try {
                        s = baos.toString("UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        LOG.error("Baos encoding exception", e);
                        s = "BAOS encoding exception";
                    }
                    sendLoadScript(s, true);
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
     * @param qlikTableBaos a ByteArrayOutputStream
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

    /**
     * Truncate / clear data in the app so we can do a reload.
     */
    public void truncateApp() {
        LOG.info("truncating Qlik app");
        String script = "ADD ONLY LOAD * INLINE [ ];";
        if (!testOnly) {
            sendLoadScript(script, false);
        } else {
           System.out.println("truncating app: " + script);
        }
    }

    /**
     * Send the load script to Qlik
     *
     * @param script
     */
    private void sendLoadScript(String script, boolean partial) {
        String request;
        String response;

        synchronized(this) {
            request = globalApi.openDoc(appName);
            System.out.println("OpenDoc request: " + request);
            response = qlikSocket.sendMessage(request);
            System.out.println("OpenDoc response: " + response);
            JsonResponse jsonResponse = new JsonResponse(response);
            int qHandle;
            if (jsonResponse.isError()) {
                System.out.println(jsonResponse.getError());
            } else {
                qHandle = jsonResponse.getqHandle();
                request = docApi.setScript(qHandle, script);
                System.out.println(String.format("SetScript request: Length(%d) %s",
                        request.length(), request.substring(1, min(request.length(), 256))));
                response = qlikSocket.sendMessage(request);
                System.out.println("SetScript response: " + response);
                if (jsonResponse.isError()) {
                    System.out.println(jsonResponse.getError());
                } else {
                    request = docApi.doReload(qHandle, 0, partial, false);
                    System.out.println("DoReload request: " + request);
                    response = qlikSocket.sendMessage(request);
                    System.out.println("DoReload response: " + response);
                    request = docApi.doSave(qHandle);
                    System.out.println("DoSave request: " + request);
                    response = qlikSocket.sendMessage(request);
                    System.out.println("DoSave response: " + response);
                }

            }
        }
    }

    /**
     * Reset the output buffer to prepare for the next batch.
     */
    private void resetBuffer() {
        baos.reset();
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
     * Call this method to flush buffers, terminate the thread, and exit.
     */
    public void cleanup() {
        sendBuffer();
        shutdown();
    }

    /**
     * give buffers time to flush, then close connection.
     */
    private void shutdown() {
        LOG.info("shutting down QlikLoad");

        // pause briefly to allow things to drain
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            LOG.trace("shutdown() timer");
        }

        if ((baos.size() != 0)) {
            LOG.warn("shutdown(): Thread {} baos HAS NOT been drained. Size: {}",
                    threadName, baos.size());
        } else {
            LOG.info("shutdown(): Thread {} baos has been drained. Size: {}",
                    threadName, baos.size());
        }


        qlikSocket.shutdown();
    }


}
