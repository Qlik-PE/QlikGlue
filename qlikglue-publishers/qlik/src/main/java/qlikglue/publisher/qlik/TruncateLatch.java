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
 * This singleton class provides a controlled way for a truncate of the Qlik
 * application to occur. The first thread to encounter a REFRESH operation
 * will get the latch and cause QlikLoad.truncateApp() to be called. All
 * other calls will receive "true", but the truncate function will not
 * be called. The latch will remain latched to prevent any other threads
 * from causing reload to occur until we begin CDC mode. At that point,
 * the latch is cleared and won't be reset until the next time we encounter
 * a REFRESH. Attunity will not begin CDC mode until all tables have
 * been (re)loaded, so this approach will work.
 *
 * CAUTION: a reload must occur on ALL TABLES. A partial reload will still
 * cause all data to be flushed from Qlik, which may not be what is desired.
 */
public class TruncateLatch {
    private boolean latched;
    private static TruncateLatch instance = new TruncateLatch();

    /**
     * Private constructor for the singleton.
     */
    private TruncateLatch() {
        latched = false;
    }

    /**
     * return the instance.
     * @return instance
     */
    public static TruncateLatch getInstance() {
        return instance;
    }

    /**
     * Get the latch if it isn't already set and call QlikLoad.truncateApp().
     * @return value of latched. Will always be true, but only one
     * thread will cause the truncate to occur.
     */
    public boolean getLatch() {
        if (!latched) {
            // only synchronize if we are the thread making the change
            synchronized(this) {
                if (!latched) {
                    latched = true;
                    // now send a "truncate"
                    QlikLoad.getInstance().truncateApp();
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
