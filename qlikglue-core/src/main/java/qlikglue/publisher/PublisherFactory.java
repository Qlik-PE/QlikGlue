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

import com.google.common.base.Throwables;

import qlikglue.QlikGluePropertyValues;
import qlikglue.common.PropertyManagement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory class that creates an instance of the appropriate publisher.
 */
public class PublisherFactory {
    private static final Logger LOG = LoggerFactory.getLogger(PublisherFactory.class);

    private static PublisherFactory myFactory = null;
    
    
    private PublisherFactory() {
        super();
    }
    
    
    /**
     * Return an instance of Publisher that
     * corresponds to the specified TargetType.
     *
     * @return a handle to the Publisher
     */
    @SuppressWarnings("unchecked")
    public static QlikGluePublisher publisherFactory() {
        if (myFactory == null) {
            myFactory = new PublisherFactory();
        }

        QlikGluePublisher rval = null;
                
        PropertyManagement properties;
        String className = null;
        Class<QlikGluePublisher> clazz = null;
        properties = PropertyManagement.getProperties();
        className = properties.getProperty(QlikGluePropertyValues.PUBLISHER_CLASS);
        if (className == null) {
            LOG.info("Publisher not set. Defaulting to ConsolePublisher");
            className = "qlikglue.publisher.ConsolePublisher";
        }
        try {
            clazz = (Class<QlikGluePublisher>) Class.forName(className);
            rval = clazz.newInstance();

        } catch (Exception e) {
            LOG.error("Could not instantiate publisher.", e);
            Throwables.propagate(e);
        }
        
        return rval;
    }
    
}
