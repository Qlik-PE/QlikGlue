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
package qlikglue.publisher;


/**
 * Publisher-related properties.
 */
public class PublisherProperties {
    /***************************************/
    /*** Publisher-related properties    ***/
    /***************************************/
    /**
     * the name of the implementation of Publisher that should be called.
     */
    public static final String PUBLISHER_CLASS = "qlikglue.publisher.class";

    /**
     * The number of threads to have executing the publishing process.
     */
    public static final String PUBLISHER_THREADS = "qlikglue.publisher.threads";
    /**
     * The default number of publisher threads.
     */
    public static final String PUBLISHER_THREADS_DEFAULT = "2";
    /**
     * Select publisher thread based on hash of table name or rowkey.
     */
    public static final String PUBLISHER_HASH = "qlikglue.publisher.hash";


    /*******************************************************************/
    /**
     * private to prevent explicit object creation
     */
    private PublisherProperties() { super(); }
}
