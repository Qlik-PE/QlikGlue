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
package qlikglue;

/**
 * This class contains configuration constants used by the
 * Big Data Glue code, including property names and where appropriate
 * the default values to use in the event that a property
 * is not defined.
 * 
 */
public final class QlikGlueProperties {
    /**
     * The name of the default properties "resource" to look for.
     */
    public static final String defaultProperties = "/qlikglueDefault.properties";
    /**
     * The external properties file to look for. These properties will override
     * the default properties.
     */
    public static final String externalProperties = "qlikglue.properties";
    
   


    /*******************************************************************/
    /**
     * private to prevent explicit object creation
     */
    private QlikGlueProperties() {
        super();
    }
}
