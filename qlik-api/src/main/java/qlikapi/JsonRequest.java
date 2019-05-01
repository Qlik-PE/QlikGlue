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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A simple class containing only public variables. Public variables can be automatically
 * serialized into Json format without having to have method annotations and such.
 */
public class JsonRequest {
    // Json properties need to be public for serialization to work without getters
    @JsonProperty
    public String jsonrpc = "2.0";
    @JsonProperty public int id;
    @JsonProperty public int handle;
    @JsonProperty public String method;
    @JsonProperty public Map<String,Object> params;

    private static final Logger LOG = LoggerFactory.getLogger(JsonRequest.class);
    private ObjectMapper mapper;

    public JsonRequest() {
        params = new HashMap<>();
        mapper = new ObjectMapper();
    }

    public String serialize() {
        String json = null;

        try {
            json = mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            LOG.error("Json serialization issue", e);
        }
        return json;
    }

    public void id(int id) { this.id = id; }
    public void handle(int handle) { this.handle = handle; }
    public void method(String method) { this.method = method; }
    public void param(String key, Object value) { this.params.put(key, value); }
    public void reset() {
        id = 0;
        handle = 0;
        method = "unset";
        params.clear();
    }
}
