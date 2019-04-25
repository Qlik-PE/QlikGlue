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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * A class that accumulates data records and flushes them either when the record count
 * reaches the specified threshold or when a specified period of time has expired.
 */
public abstract class DataAccumulator {
    private static final Logger LOG = LoggerFactory.getLogger(DataAccumulator.class);
    private Timer timer;
    private TimerTask timerTask;
    private boolean shutdown = false;
    private int batchSize;
    private int flushFreq;

    public DataAccumulator() {
        timer = new Timer();
    }

    public void setBatchSize (int batchSize) {
        this.batchSize = batchSize;
    }

    public void setFlushFreq(int flushFreq) {
        this.flushFreq = flushFreq;
    }

    public void cleanup() {
        LOG.debug("cleanup");
        // flush any pending events
        shutdown = true;
        publishEvents();
        // clean up the timer
        timer.cancel();
        if (timerTask != null) {
            timerTask.cancel();
            timerTask = null;
        }

    }

    /**
     * Simple timer to ensure that we periodically flush whatever we have queued
     * in the event that we haven't received "batchSize" events by the time
     * that the timer has expired.
     */
    private class FlushQueuedEvents extends TimerTask {
        public void run() {

           LOG.debug("timer expired");

            publishEvents();
        }
    }


    /**
     * publish all events that we have queued up to Qlik. This is called both by
     * the timer and by writeEvent(). Need to be sure they don't step on each other.
     */
    protected void publishEvents() {
        LOG.debug("in publishEvents");

        synchronized (this) {
            if (timerTask != null) {
                LOG.debug("cancelling timer");
                timerTask.cancel();
                timerTask = null;
            }
            if (bufferSize() >= 0) {
                LOG.debug("sending buffer");
                sendBuffer();
                resetBuffer();
            }

            if (!shutdown) {
                LOG.debug("DataAccumulator: starting timer");
                // ensure that we don't keep queued events around very long.
                timerTask = new FlushQueuedEvents();
                timer.schedule(timerTask, flushFreq);
            } else {
                timerTask = null;
            }
        }
    }

    protected abstract void resetBuffer();
    protected abstract void sendBuffer();
    protected abstract int bufferSize();

}
