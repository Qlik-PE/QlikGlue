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
 * A singleton counter that generates a new requestId for each call we make.
 */
public class RequestId {
    private static RequestId instance = new RequestId();
    private int requestId;

    /**
     * Return the next request ID.
     * @return requestId
     */
    public static int next() {
        return ++instance.requestId;
    }

    private RequestId() {
        requestId = 0;
    }
}
