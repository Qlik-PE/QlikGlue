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

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
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
 */
@ClientEndpoint
public class QlikSocket  extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(QlikSocket.class);
    private Session userSession = null;
    private BlockingQueue<String> messageQueue;
    private String threadName;
    URI endpointURI;

    // TODO: move these to properties
    String URI = "ws://localhost:8080/jee7-websocket-api/chat";
    int queueSize = 5;

    public QlikSocket(String threadName) {
        this.threadName = threadName;
        messageQueue = new ArrayBlockingQueue<>(queueSize);
        //ws_init();
    }

    /**
     * initialize websocket stuff
     */
    private void ws_init() {
        try {
            endpointURI = new URI(URI);
            WebSocketContainer container = ContainerProvider
                    .getWebSocketContainer();
            container.connectToServer(this, endpointURI);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void run() {
        /*
         * Loop until terminated, reading messages from the queue and sending them
         * on to Qlik.
         */
       while(!Thread.currentThread().isInterrupted()) {
           try {
               sendMessage(messageQueue.take());
           } catch (InterruptedException e) {
               LOG.info("messageQueue.take() interrupted", e);
           }

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

        if ((messageQueue.size() != 0)) {
            LOG.warn("shutdown(): Thread {} queue HAS NOT been drained. Depth: {}",
                    threadName, messageQueue.size());
        } else {
            LOG.info("shutdown(): Thread {} queue has been drained. Depth: {}",
                    threadName, messageQueue.size());
        }

    }

    /**
     * Call this method to terminate the thread and exit.
     */
    public void cancel() {
        shutdown();

        interrupt();
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

    /**
     * Send a message.
     *
     * @param message
     */
    private void sendMessage(String message) {
        /*
        try {
            // TODO: use getAsyncRemote() if we find we don't have to wait.
            this.userSession.getBasicRemote().sendText(message);
        } catch (IOException e) {
            LOG.error("I/O Exception when sending message", e);
        }
        */
        System.out.println(message);
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

    /**
     * Add a message to the queue. Note that the add request will block
     * if the queue is full.
     *
      * @param message
     */
    public void queueMessage(String message) {
        messageQueue.add(message);
    }

}
