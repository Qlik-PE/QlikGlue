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
    private Session userSession = null;
    private String messageResult = "no message";
    private String previousMessage = "";
    private ByteArrayOutputStream baos;
    private CountDownLatch countDownLatch;
    private boolean pendingResponse = false;
    private String URI;

    /**
     * Create an instance using the default property values.
     */
    public QlikSocket(String URI) {
        this.URI = URI;
        init();
    }

    /**
     * Initialization common to all constructors.
     */
    private void init() {
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
     * Send a json-formatted message to Qlik. Note that this method uses a
     * countdown latch to cause synchronous execution. The latch shouldn't be
     * cleared until the returned message is set in onMessage().
     *
     * @param jsonMessage the json-formatted record we want to send
     *
     * @return the json message returned from the call.
     */
    public String sendMessage(String jsonMessage) {
        if (userSession == null) {
           reconnect();
        }
        try {
            if (pendingResponse) {
                LOG.warn("countDownLatch wait in progress");
                countDownLatch.await();
            }

            countDownLatch = new CountDownLatch(1);
            // TODO: should we use getAsyncRemote() ???
            pendingResponse = true;
            this.userSession.getBasicRemote().sendText(jsonMessage);
            countDownLatch.await(15L, TimeUnit.SECONDS);
            countDownLatch = null;

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
        URI endpointURI;
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
     * Callback hook for Message Events. This method will be invoked when
     * Qlik sends a response message.
     *
     * @param message
     *            The text message
     */
    @OnMessage
    public void onMessage(String message) {
        if (pendingResponse) {
            if (message == null) {
                LOG.warn("NULL message received: " + message);
            } else {
                if (!previousMessage.equals(message)) {
                    previousMessage = messageResult;
                    messageResult = message;
                    pendingResponse = false;
                    countDownLatch.countDown();
                } else {
                    LOG.warn("Duplicate message received: " + message);
                }
            }
        } else {
            LOG.warn("onMessage() while response not pending: " + message);
        }
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
     * @param args the arguments passed in on the command line.
     * @throws Exception rutime exceptions we receive.
     *
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

        QlikSocket qlikSocket = new QlikSocket("ws://10.0.2.2:4848/app/");
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
