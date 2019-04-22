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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qlikglue.common.PropertyManagement;

import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.*;
import java.net.URI;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import javax.websocket.ClientEndpoint;
import javax.websocket.CloseReason;
import javax.websocket.ContainerProvider;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;
import javax.json.Json;
import javax.json.JsonObject;

/**
 * Handles the websocket APIs needed to bridge between QlikGlue and the QlikSense server.
 * This is a singleton.
 */
@ClientEndpoint
public class QlikSocket  {
    private static QlikSocket instance = null;
    private static final Logger LOG = LoggerFactory.getLogger(QlikSocket.class);
    private static final boolean testOnly = true;
    private int opCounter = 0;
    private int totalOps = 0;
    private Timer timer;
    private TimerTask timerTask;
    private Session userSession = null;
    private String threadName;
    private ByteArrayOutputStream baos;
    URI endpointURI;

    // properties
    String URI;
    int maxBufferSize;
    int flushFreq;

    private QlikSocket(String threadName) {
        this.threadName = threadName;
        PropertyManagement properties = PropertyManagement.getProperties();
        URI = properties.getProperty(QlikSocketProperties.QLIKSOCKET_URL,
                QlikSocketProperties.QLIKSOCKET_URL_DEFAULT);
        maxBufferSize = properties.asInt(QlikSocketProperties.QLIKSOCKET_MAXBUFFERSIZE,
                QlikSocketProperties.QLIKSOCKET_MAXBUFFERSIZE_DEFAULT);
        flushFreq = properties.asInt(QlikSocketProperties.QLIKSOCKET_FLUSH_FREQ,
                QlikSocketProperties.QLIKSOCKET_FLUSH_FREQ_DEFAULT);
        timer = new Timer();
        baos = new ByteArrayOutputStream(65536);
        // reinitialize things
        resetBuffer();
        publishEvents();
        ws_init();
    }

    /**
     * Return the singleton instance of this class
     * @return
     */
    public static QlikSocket getInstance() {
        if (instance == null) {
            synchronized (QlikSocket.class) {
                // double-check because of potential for race
                if (instance == null) {
                    instance = new QlikSocket("QLikSocket");
                }
            }
        }
        return instance;
    }


    /**
     * Simple timer to ensure that we periodically flush whatever we have queued
     * in the event that we haven't received "batchSize" events by the time
     * that the timer has expired.
     */
    private class FlushQueuedEvents extends TimerTask {
        public void run() {

            publishEvents();
        }
    }

    /**
     * publish all events that we have queued up to Qlik. This is called both by
     * the timer and by queueMessage(). Need to be sure they don't step on each other.
     */
    private void publishEvents() {

        synchronized (baos) {
            if (timerTask != null) {
                timerTask.cancel();
                timerTask = null;
            }
            if (opCounter > 0) {
                opCounter = 0;
                sendBuffer();
                resetBuffer();
            }

            // ensure that we don't keep queued events around very long.
            timerTask = new FlushQueuedEvents();
            timer.schedule(timerTask, flushFreq);
        }
    }

    private void shutdown() {
        LOG.info("Thread {} : shutting down EncoderThread", threadName);

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

    }

    /**
     * Call this method to terminate the thread and exit.
     */
    public void cleanup() {
        publishEvents();
        timer.cancel();
        shutdown();
    }

    /**
     * Create a json-formatted message.
     *
     * @param message
     * @return
     */
    private static String formatMessage(String message) {
        return Json.createObjectBuilder()
                .add("user", "bot")
                .add("message", message)
                .build()
                .toString();
    }


    /**
     * Send a message.
     *
     * @param message
     */
    private void sendMessage(String message) {
        try {
            // TODO: use getAsyncRemote() if we find we don't have to wait.
            this.userSession.getBasicRemote().sendText(message);
        } catch (IOException e) {
            LOG.error("I/O Exception when sending message", e);
        }
    }

    /**
     * Send the contents of baos to the web socket
     */
    private void sendBuffer() {
        if (!testOnly) {
            sendMessage(baos.toString());
        } else {
            logToFile();
        }
    }

    /**
     * for testing purposes ... log to a file instead of sending to a socket.
     */
    private void logToFile() {
        String filename = "/tmp/QlikSocket_" + totalOps;
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
     * Process the message returned to onMessage().
     * @param message
     */
    private void onMessageHandler(String message) {
        JsonObject jsonObject = Json.createReader(new StringReader(message)).readObject();
        String userName = jsonObject.getString("user");
        if (!"bot".equals(userName)) {
            sendMessage(formatMessage("Hello " + userName + ", How are you?"));
            // other dirty bot logic goes here.. :)
        }

    }

    private void resetBuffer() {
        baos.reset();
    }

    /**
     * Add a message to the output buffer.
     *
     * @param qlikTableBaos
     */
    public void queueMessage(ByteArrayOutputStream qlikTableBaos) {
        opCounter++;
        totalOps++;
        synchronized(baos) {
            try {
                qlikTableBaos.writeTo(baos);
                if (baos.size() >= maxBufferSize) {
                    publishEvents();
                }
            } catch (IOException e) {
                LOG.error("error adding message", e);
            }
        }
    }

    /***********************/
    /* Websocket API calls */
    /***********************/

    /**
     * initialize websocket stuff
     */
    private void ws_init() {
        if (!testOnly) {
            try {
                endpointURI = new URI(URI);
                WebSocketContainer container = ContainerProvider
                        .getWebSocketContainer();
                container.connectToServer(this, endpointURI);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            System.out.println("QlikSocket: running in TestOnly mode");
            LOG.warn("Running in TestOnly mode");
        }

    }

    /**
     * Callback hook for Connection open events.
     *
     * @param userSession
     *            the userSession which is opened.
     */
    @OnOpen
    public void onOpen(Session userSession) {
        this.userSession = userSession;
    }

    /**
     * Callback hook for Connection close events.
     *
     * @param userSession
     *            the userSession which is getting closed.
     * @param reason
     *            the reason for connection close
     */
    @OnClose
    public void onClose(Session userSession, CloseReason reason) {
        this.userSession = null;
    }

    /**
     * Callback hook for Message Events. This method will be invoked when a
     * client sends a message.
     *
     * @param message
     *            The text message
     */
    @OnMessage
    public void onMessage(String message) {
        onMessageHandler(message);
    }

}
