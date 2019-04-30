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

import java.nio.file.Files;
import java.nio.file.Paths;

public class DocApi {
    private static final Logger LOG = LoggerFactory.getLogger(DocApi.class);
    private JsonRequest request;

    /**
     * Constructor.
     */
    public DocApi() {
        request = new JsonRequest();
    }


    /**
     * Creates a Qlik SetScript API request.
     *
     * @param qHandle the qHandle returned from the opened application
     * @param script the inline script to load
     * @return the Json request
     */
    public String setScript(int qHandle, String script) {
        request.reset();
        request.id(RequestId.next());
        request.handle(qHandle);
        request.method("SetScript");
        request.param("qScript", script);

        return request.serialize();
    }

    /**
     * Reloads the scrpt that is set in an app, taking the default for
     * all the parameters..
     *
     * @param qHandle the handle returned from the opened application
     * @return the Json request
     */
    public String doReload(int qHandle) {
        request.reset();
        request.id(RequestId.next());
        request.handle(qHandle);
        request.method("DoReload");
        request.param("qMode", 0);
        request.param("qPartial", false);
        request.param("qDebug", false);

        return request.serialize();
    }

    /**
     * Reloads the script that is set in a app.
     *
     * @param qHandle the handle returned from the opened application
     * @param qMode the error handling mode (0: default mode, 1: ABEND, 2: ignore and continue)
     * @param qPartial Set to true for partial reload. The default value is false
     * @param qDebug Set to true if debug breakpoints are to be honored. The execution of
     *               the script will be in debug mode. The default value is false.
     * @return the Json request
     */
    public String doReload(int qHandle, int qMode, boolean qPartial, boolean qDebug) {
        request.reset();
        request.id(RequestId.next());
        request.handle(qHandle);
        request.method("DoReload");
        request.param("qMode", qMode);
        request.param("qPartial", qPartial);
        request.param("qDebug", qDebug);

        return request.serialize();
    }

    /**
     * Saves an app. All  objects and data in the data model are saved.
     * @param qHandle the handle returned from the opened application.
     * @return the Json request
     */
    public String doSave(int qHandle) {
        request.reset();
        request.id(RequestId.next());
        request.handle(qHandle);
        request.method("DoSave");

        return request.serialize();
    }

    /**
     * Saves an app to the specified file. All  objects and data in the data model are saved.
     * @param qHandle the handle returned from the opened application.
     * @param qFileName the name of the file to save
     * @return the Json request
     */
    public String doSave(int qHandle, String qFileName) {
        request.reset();
        request.id(RequestId.next());
        request.handle(qHandle);
        request.method("DoSave");
        request.param("qFileName", qFileName);

        return request.serialize();
    }

    /**
     * Quick test of APIs.
     *
     * @param args the arguments passed in on the command line.
     * @throws Exception runtime exceptions
     */
    public static void main(String[] args) throws Exception  {
        DocApi docApi = new DocApi();
        String contents = new String(Files.readAllBytes(Paths.get("/tmp/QlikSocket_1")));


        System.out.println(docApi.setScript(1, contents));
        System.out.println(docApi.doReload(1));
        System.out.println(docApi.doReload(1, 0, true, false));
        System.out.println(docApi.doSave(1));
        System.out.println(docApi.doSave(1, "/tmp/myfile.qvf"));

    }

}
