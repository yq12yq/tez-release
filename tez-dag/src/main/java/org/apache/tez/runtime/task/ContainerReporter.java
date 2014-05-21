/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.task;


import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.TezTaskUmbilicalProtocol;

/**
 * Responsible for communication between a running Container and the ApplicationMaster. The main
 * functionality is to poll for new tasks.
 * 
 */
public class ContainerReporter implements Callable<ContainerTask> {

  private static final Logger LOG = Logger.getLogger(ContainerReporter.class);

  private final TezTaskUmbilicalProtocol umbilical;
  private final ContainerContext containerContext;
  private final int getTaskMaxSleepTime;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final long LOG_INTERVAL = 2000l;

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();

  private long nextGetTaskPrintTime;

  ContainerReporter(TezTaskUmbilicalProtocol umbilical, ContainerContext containerContext,
      int getTaskMaxSleepTime) {
    this.umbilical = umbilical;
    this.containerContext = containerContext;
    this.getTaskMaxSleepTime = getTaskMaxSleepTime;
  }

  @Override
  public ContainerTask call() throws Exception {
    ContainerTask containerTask = null;
    boolean isNewGetTask = true;
    while (!stopped.get()) {
      long getTaskPollStartTime = System.currentTimeMillis();
      nextGetTaskPrintTime = getTaskPollStartTime + LOG_INTERVAL;
      for (int idle = 0; containerTask == null; idle++) {
        if (!isNewGetTask) { // Don't sleep on the first iteration.
          long sleepTimeMilliSecs = Math.min(idle * 10, getTaskMaxSleepTime);
          maybeLogSleepMessage(sleepTimeMilliSecs);
          lock.lock();
          try {
            condition.await(sleepTimeMilliSecs, TimeUnit.MILLISECONDS);
          } finally {
            lock.unlock();
          }
        } else {
          LOG.info("Attempting to fetch new task");
        }
        isNewGetTask = false;
        containerTask = umbilical.getTask(containerContext);
      }
      LOG.info("Got TaskUpdate: "
          + (System.currentTimeMillis() - getTaskPollStartTime)
          + " ms after starting to poll."
          + " TaskInfo: shouldDie: "
          + containerTask.shouldDie()
          + (containerTask.shouldDie() == true ? "" : ", currentTaskAttemptId: "
              + containerTask.getTaskSpec().getTaskAttemptID()));
      return containerTask;
    }
    return null;
  }

  public void shutdown() {
    stopped.set(true);
    lock.lock();
    try {
      condition.signal();
    } finally {
      lock.unlock();
    }
  }

  private void maybeLogSleepMessage(long sleepTimeMilliSecs) {
    long currentTime = System.currentTimeMillis();
    if (sleepTimeMilliSecs + currentTime > nextGetTaskPrintTime) {
      LOG.info("Sleeping for " + sleepTimeMilliSecs
          + "ms before retrying getTask again. Got null now. "
          + "Next getTask sleep message after " + LOG_INTERVAL + "ms");
      nextGetTaskPrintTime = currentTime + sleepTimeMilliSecs + LOG_INTERVAL;
    }
  }
}
