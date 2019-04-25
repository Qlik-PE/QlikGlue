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
    public static final String QLIKSOCKET_URL = "qliksocket.url";
    public static final String QLIKSOCKET_URL_DEFAULT = "ws://10.0.2.2:4848/app/ ";
    /**
     * The maximum size of the output buffer before we force a flush
     */
    public static final String QLIKSOCKET_MAXBUFFERSIZE = "qliksocket.maxbuffersize";
    public static final String QLIKSOCKET_MAXBUFFERSIZE_DEFAULT = "65536";
    /**
     * The maximum default time in milliseconds after which any web socket sessions in this
     * container will be closed if it has been inactive. A value that is 0 or negative
     * indicates the sessions will never timeout due to inactivity.
     */
    public static final String QLIKSOCKET_MAXIDLETIMEOUT = "qliksocket.max-idle-timeout";
    public static final String QLIKSOCKET_MAXIDLETIMEOUT_DEFAULT = "-1";
    /**
     * The name of the Qlik application we want to load.
     */
    public static final String QLIKSOCKET_APPNAME = "qliksocket.app-name";
    public static final String QLIKSOCKET_APPNAME_DEFAULT = "myApp";

    /**
     * make constructor private to prevent explicit instantiation.
     */
    private QlikSocketProperties() {
        super();
    }
}
