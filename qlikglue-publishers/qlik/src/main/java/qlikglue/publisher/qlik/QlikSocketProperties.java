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

/**
 * This class contains configuration constants used by QlikSocket,
 * including property names and where appropriate the default valuses to
 * use in the event that a property is not defined elsewhere.
 */
public final class QlikSocketProperties {
    /**
     * Properties related to QlikSocket.
     */
    /**
     * The websocket URL to connect to.
     */
    public static final String QLIKSOCKET_URL = "qlikglue.qliksocket.url";
    public static final String QLIKSOCKET_URL_DEFAULT = "ws://localhost:8080/jee7-websocket-api/chat";
    /**
     * The maximum size of the output buffer before we force a flush
     */
    public static final String QLIKSOCKET_MAXBUFFERSIZE = "qlikglue.qliksocket.maxbuffersize";
    public static final String QLIKSOCKET_MAXBUFFERSIZE_DEFAULT = "65536";
    /**
     * The flush frequency in milliseconds.
     */
    public static final String QLIKSOCKET_FLUSH_FREQ = "qlikglue.qliksocket.queuesize";
    public static final String QLIKSOCKET_FLUSH_FREQ_DEFAULT = "10000";

    /**
     * make constructor private to prevent explicit instantiation.
     */
    private QlikSocketProperties() {
        super();
    }
}
