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

import org.apache.commons.io.output.ByteArrayOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class provides a controlled way for a truncate within the Qlik
 * application to occur. It can be instantiated both as a singleton
 * to support a global truncate of all tables, and as individual instances
 * to support table-by-table truncates.
 *
 * For a singleton, the first thread to encounter a REFRESH operation
 * will get the latch and call QlikLoad.truncateApp(). All
 * other calls will receive "true", but the truncate function will not
 * be called repeatedly. The latch will remain latched to prevent any other threads
 * from causing reload to occur until we begin CDC mode. At that point,
 * the latch is cleared and won't be reset until the next time we encounter
 * a REFRESH. Attunity will not begin CDC mode until all tables have
 * been (re)loaded, so this approach will work.
 *
 * For table-by-table truncation, a local instance is called. Behavior is similar
 * to what is described above, but only for one table.
 */
public class TruncateLatch {
    private static final Logger LOG = LoggerFactory.getLogger(TruncateLatch.class);
    private boolean latched;
    private static TruncateLatch globalInstance = new TruncateLatch();


    /**
     * Constructor.
     */
    public TruncateLatch() {
        this.latched = false;
    }

    /**
     * return the global instance. Getting a latch on this instance will cause a
     * "truncate all" to occur.
     *
     * @return the global instance
     */
    public static TruncateLatch getGlobalInstance() {
        return globalInstance;
    }

    /**
     * Get the latch if it isn't already set and call QlikLoad.truncateApp().
     * @param tableName the name of the table we will truncate. "null" for truncate all.
     * @return value of latched. Will always be true, but only one
     * thread will cause the truncate to occur.
     */
    public boolean getLatch(String tableName, String hdr) {
        if (!latched) {
            // only synchronize if we are the thread making the change
            synchronized(this) {
                if (!latched) {
                    String script;
                    latched = true;
                    if (this == TruncateLatch.getGlobalInstance()) {
                        // if this is the singleton, send a truncate all.
                        script = String.format("ADD LOAD * INLINE [%n%s%n];", "fakecolumn");
                        LOG.info("truncating all tables: {}", script);
                    } else {
                        // else send the truncate just for this table.
                        script = String.format("%s: %nADD LOAD * INLINE [%n%s%n];", tableName, hdr);
                        LOG.info("truncating table: {}", script);
                    }
                    QlikLoad.getInstance().truncateApp(script);
                }
            }
        }
        return true;
    }

    /**
     * clear the latch for the next time if it hasn't already been cleared..
     * @return the value of latched which will always be false;
     */
    public boolean clearLatch() {
        if (latched) {
            // only synchronize if we are the thread making the change
            synchronized(this) {
               if (latched) {
                   latched = false;
               }
            }
        }
        return false;
    }
}
