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
package qlikglue.common;

import java.sql.Types;

public class JsonField {
    private String name;
    private String value;
    private int jdbcType;

    /**
     * Construct an instance, defaulting jdbcType to Types.VARCHAR.
     *
     * @param name the field / column name
     * @param value the field value
     */
    public JsonField(String name, String value) {
       this.name = name;
       this.value = value;
       this.jdbcType = Types.VARCHAR;
    }
    /**
     * Construct an instance
     *
     * @param name the field / column name
     * @param value the field value
     * @param jdbcType the java.sql.Types value
     */
    public JsonField(String name, String value, int jdbcType) {
        this.name = name;
        this.value = value;
        this.jdbcType = jdbcType;
    }

    /**
     * @return the name of the field
     */
    public String getName() {
        return this.name;
    }

    /**
     * @return the value of the field
     */
    public String getValue() {
        return this.value;
    }

    /**
     * @return the jdbcType of the field
     */
    public int getType() {
        return this.jdbcType;
    }
}
