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
 * This class contains configuration constants used by the
 * Qlik Publisher, including property names and where appropriate
 * the default values to use in the event that a property
 * is not defined.
 *
 */
public final class QlikPublisherPropertyValues {
    /**
     * Properties related to QlikPublisher.
     */
    /**
     * The Qlik dataset name to connect to.
     */
    /**
     * The number of operations to batch together.
     */
    public static final String QLIK_BATCH_SIZE = "qlikglue.qlikpublisher.batch-size";
    public static final String QLIK_BATCH_SIZE_DEFAULT = "5";
    /**
     * The frequency to flush operations if the batch-size isn't reached (milliseconds).
     */
    public static final String QLIK_FLUSH_FREQ = "qlikglue.qlikpublisher.flush-frequency";
    public static final String QLIK_FLUSH_FREQ_DEFAULT = "500";
    /**
     * Boolean: True if we want to convert deletes and updates into inserts. Assumes that
     * inclusion of operation type and timestamp has been specified in the properties.
     */
    public static final String QLIK_INSERT_ONLY = "qlikglue.qlikpublisher.insert-only";
    public static final String QLIK_INSERT_ONLY_DEFAULT = "true";
    
    
    /*******************************************************************/
    /**
     * private to prevent explicit object creation
     */
    private QlikPublisherPropertyValues() {
        super();
    }
}
