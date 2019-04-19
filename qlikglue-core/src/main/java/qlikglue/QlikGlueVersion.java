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
 * A simple class that allows us to store the current version number. This is also where
 * we keep tabs on changes to the code.
 * <p>
 *  Version: 1.0.0.0 04/13/2019
 *  <ul>
 *  <li>mm/dd/2019: 1.0.0.1: Description here.</li>
 * </ul>
 */
public class QlikGlueVersion {

    private static final String name = "QlikGlue";
    private static final String major = "1";
    private static final String minor = "0";
    private static final String fix = "0";
    private static final String build = "0";
    private static final String date = "2019/04/13 17:45";
    
    /**
     * Track the version number.
     */
    public QlikGlueVersion() {
        super();
    }

    /**
     * Main entry point to return the version information.
     * 
     * @param args not used at this time
     */
    public static void main(String[] args) {
        QlikGlueVersion version = new QlikGlueVersion();
        
        version.versionInfo();
    }

    /**
     * Format the version information. This is useful for logging
     * the version information using LOG4J.
     * 
     * @return a formatted String containing version info.
     */
    public String format() {
        String rval;
        
        rval = String.format("%s Version: %s.%s.%s.%s  Date: %s", 
                    name, major, minor, fix, build, date);
        
        return  rval;
    }
    
    /**
     * Write the version info to stdout.
     */
    public void versionInfo() {
        System.out.println(format());
    }
}
