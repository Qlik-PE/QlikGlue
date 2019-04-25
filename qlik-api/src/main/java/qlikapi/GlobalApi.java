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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalApi {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalApi.class);
    private JsonRequest request;

    /**
     * Constructor.
     */
    public GlobalApi() {
        request = new JsonRequest();
    }


    /**
     * Creates a Qlik OpenDoc API request for the appName, taking the
     * defaults for the other parameters.
     *
     * @param appName the name or GUID of the app to retrieve
     * @return the Json request
     */
    public String openDoc(String appName) {
        request.reset();
        request.id(RequestId.next());
        request.handle(-1);
        request.method("OpenDoc");
        request.param("qDocName", appName);

        return request.serialize();
    }

    /**
     * Creates a Qlik OpenDoc API request for the specified parameters.
     *
     * @param appName the name or GUID of the app to retrieve
     * @param userName the name of the user that opens the app
     * @param password password of the user
     * @param serial current Qlik Sense serial number.
     * @param noData Set this parameter to true to be able to open an app without
     *               loading its data. When this parameter is set to true, the objects
     *               in the app are present but contain no data. The script can be
     *               edited and reloaded. The default value is false.
     *
     * @return the Json request
     */
    public String openDoc(String appName, String userName, String password, String serial, boolean noData) {
        request.reset();
        request.id(RequestId.next());
        request.handle(-1);
        request.method("OpenDoc");
        request.param("qDocName", appName);
        request.param("qUserName", userName);
        request.param("qPassword", password);
        request.param("qSserial", serial);
        request.param("qNoData", noData);

        return request.serialize();
    }
}
