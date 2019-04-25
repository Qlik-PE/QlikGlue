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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A simple class that lets us look at the contents of the response from
 * a Qlik API request. Responses are somewhat inconsistent in their formatting, so
 * we will use convenience methods to short-circuit some of the interpretation.
 */
public class JsonResponse {
    private static final Logger LOG = LoggerFactory.getLogger(JsonResponse.class);
    private String response;
    private ObjectMapper mapper;
    private JsonNode rootNode;

    /**
     * Creates an instance.
     * @param response the Json response from an API call.
     */
    public JsonResponse(String response) {
       this.response = response;
       mapper = new ObjectMapper();
        try {
            rootNode = mapper.readTree(response);
        } catch (IOException e) {
            LOG.error("Json parse error", e);
        }
    }

    /**
     * Looks to see if we have a node in the tree called "error"
     * and returns true if so.
     * @return true if there is an error node. False otherwise.
     */
    public boolean isError() {
        boolean rval = false;
        if (rootNode.findValue("error") != null) {
            rval = true;
        }
        return rval;
    }

    /**
     * Presumptive method that assumes we have the error node, which should
     * be the case if isError() returned true.
     * @return Returns an error message from the response.
     */
    public String getError() {
        JsonNode error = rootNode.findValue("error");
        int code = error.findValue("code").asInt();
        String message = error.findValue("message").asText();

        return String.format("Qlik API Error: %d: %s", code, message);
    }

    /**
     * Presumptive method that assumes that qHandle will be present if we
     * are calling it.
     * @return qHandle from an OpenDoc request.
     */
    public int getqHandle() {
        return rootNode.findValue("qHandle").asInt();
    }

}
