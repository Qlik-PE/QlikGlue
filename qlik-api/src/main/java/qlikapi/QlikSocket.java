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
package qlikapi;


import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qlikglue.QlikGluePropertyValues;
import qlikglue.common.PropertyManagement;

import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.websocket.ClientEndpoint;
import javax.websocket.CloseReason;
import javax.websocket.ContainerProvider;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;

/**
 * Handles the websocket APIs needed to bridge between QlikGlue and the QlikSense server.
 * This is a singleton.
 */
@ClientEndpoint
public class QlikSocket  {
    private static final Logger LOG = LoggerFactory.getLogger(QlikSocket.class);
    private static final boolean testOnly = false;
    private long idleTimeout;
    private Session userSession = null;
    private String messageResult;
    private String appName;
    private ByteArrayOutputStream baos;
    private CountDownLatch countDownLatch;
    URI endpointURI;

    // properties
    String URI;
    int maxBufferSize;

    public QlikSocket() {
        PropertyManagement properties = PropertyManagement.getProperties();
        URI = properties.getProperty(QlikSocketProperties.QLIKSOCKET_URL,
                QlikSocketProperties.QLIKSOCKET_URL_DEFAULT);
        appName = properties.getProperty(QlikSocketProperties.QLIKSOCKET_APPNAME,
                QlikSocketProperties.QLIKSOCKET_APPNAME_DEFAULT);
        maxBufferSize = properties.asInt(QlikSocketProperties.QLIKSOCKET_MAXBUFFERSIZE,
                QlikSocketProperties.QLIKSOCKET_MAXBUFFERSIZE_DEFAULT);
        idleTimeout = properties.asLong(QlikSocketProperties.QLIKSOCKET_MAXIDLETIMEOUT,
                QlikSocketProperties.QLIKSOCKET_MAXIDLETIMEOUT_DEFAULT);
        baos = new ByteArrayOutputStream(65536);
        // reinitialize things
        resetBuffer();
        connectToServer();
    }

    /**
     * Close the user session and do any other necessary cleanup.
     */
    public void shutdown() {
        LOG.info("shutting down QlikSocket");
        if (userSession != null) {
            LOG.info("closing the QlikSocket session");
            try {
                userSession.close();
            } catch (IOException e) {
                LOG.error("error closing session", e);
            }
        }
        else {
            LOG.info("QlikSocket session was not open");
        }

    }

    /**
     * Send a json-formatted message to Qlik
     * @param jsonMessage
     */
    public String sendMessage(String jsonMessage) {
        if (userSession == null) {
           reconnect();
        }
        try {
            countDownLatch = new CountDownLatch(1);
            // TODO: should we use getAsyncRemote() ???
            this.userSession.getBasicRemote().sendText(jsonMessage);
            countDownLatch.await(5L, TimeUnit.SECONDS);
        }catch (InterruptedException e) {
           LOG.warn("CountDownLatch timer expired", e);
        } catch (IOException e) {
            LOG.error("I/O Exception when sending message", e);
        }
        return messageResult;
    }

    /**
     * Reset the output buffer to prepare for the next batch.
     */
    private void resetBuffer() {
        baos.reset();
    }

    /***********************/
    /* Websocket API calls */
    /***********************/

    /**
     * initialize websocket stuff
     */
    private void connectToServer() {
        if (!testOnly) {
            try {
                endpointURI = new URI(URI);
                WebSocketContainer container = ContainerProvider
                        .getWebSocketContainer();
                //container.setDefaultMaxSessionIdleTimeout(NEVER);
                container.connectToServer(this, endpointURI);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            System.out.println("QlikSocket: running in TestOnly mode");
            LOG.warn("Running in TestOnly mode");
        }
    }

    private void reconnect() {
        if (!testOnly) {
            LOG.info("reconnecting to the server");
            connectToServer();
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
        LOG.info("userSession is opened");
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
        LOG.info("userSession is closed");
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
        messageResult = message;
        countDownLatch.countDown();
    }

    /**
     * Callback hook for Error events. This method will be
     * invoked when an error occurs.
     *
     * @param userSession
     *          the userSession
     * @param error
     *          the error detected
     */
    @OnError
    public void onError(Session userSession, Throwable error) {
        LOG.error("websocket error received", error);
    }

    /**
     * Quick test of APIs.
     *
     * @param args
     */
    public static void main(String[] args) throws Exception  {
        //This is the root logger provided by log4j
        boolean append = false;
        String logfile = "/tmp/qliksocket.log";
        org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
        rootLogger.setLevel(Level.INFO);

        //Define the log pattern layout
        PatternLayout layout = new PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n");

        //Add an appender to the root logger
        try {
            rootLogger.addAppender(new FileAppender(layout, logfile, append));
        } catch (IOException e) {
            rootLogger.addAppender(new ConsoleAppender(layout));
            rootLogger.error("failed to open log4j log file. Switching to ConsoleAppender.", e);
        }
        PropertyManagement.getProperties(QlikGluePropertyValues.defaultProperties,
                        QlikGluePropertyValues.externalProperties);

        QlikSocket qlikSocket = new QlikSocket();
        GlobalApi globalApi = new GlobalApi();
        DocApi docApi = new DocApi();
        String script = new String(Files.readAllBytes(Paths.get("/tmp/QlikSocket_1")));

        String request = globalApi.openDoc("myApp");
        System.out.println("OpenDoc request: " + request);
        String response = qlikSocket.sendMessage(request);
        System.out.println("OpenDoc response: " + response);
        JsonResponse jsonResponse = new JsonResponse(response);
        int qHandle;
        if (jsonResponse.isError()) {
            System.out.println(jsonResponse.getError());
        }
        else {
            qHandle = jsonResponse.getqHandle();
            request = docApi.setScript(qHandle, script);
            System.out.println("SetScript request: " + request);
            response = qlikSocket.sendMessage(request);
            System.out.println("SetScript response: " + response);
            if (jsonResponse.isError()) {
                System.out.println(jsonResponse.getError());
            } else {
                request = docApi.doReload(qHandle, 0, true, false);
                System.out.println("DoReload request: " + request);
                response = qlikSocket.sendMessage(request);
                System.out.println("DoReload response: " + response);
                request = docApi.doSave(qHandle);
                System.out.println("DoSave request: " + request);
                response = qlikSocket.sendMessage(request);
                System.out.println("DoSave response: " + response);
            }

        }


        qlikSocket.shutdown();

    }


}
