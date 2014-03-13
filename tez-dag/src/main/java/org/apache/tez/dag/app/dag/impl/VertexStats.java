/* Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tez.dag.app.dag.impl;

import org.apache.tez.dag.api.oldrecords.TaskReport;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.records.TezTaskID;

public class VertexStats {

  long firstTaskStartTime = -1;
  TezTaskID firstTaskToStart = null;
  long lastTaskFinishTime = -1;
  TezTaskID lastTaskToFinish = null;

  long minTaskDuration = -1;
  long maxTaskDuration = -1;
  double avgTaskDuration = -1;
  long numSuccessfulTasks = 0;

  TezTaskID shortestDurationTask = null;
  TezTaskID longestDurationTask = null;

  public long getFirstTaskStartTime() {
    return firstTaskStartTime;
  }

  public TezTaskID getFirstTaskToStart() {
    return firstTaskToStart;
  }

  public long getLastTaskFinishTime() {
    return lastTaskFinishTime;
  }

  public TezTaskID getLastTaskToFinish() {
    return lastTaskToFinish;
  }

  public long getMinTaskDuration() {
    return minTaskDuration;
  }

  public long getMaxTaskDuration() {
    return maxTaskDuration;
  }

  public double getAvgTaskDuration() {
    return avgTaskDuration;
  }

  public TezTaskID getShortestDurationTask() {
    return shortestDurationTask;
  }

  public TezTaskID getLongestDurationTask() {
    return longestDurationTask;
  }

  void updateStats(TaskReport taskReport) {
    if (firstTaskStartTime == -1
      || firstTaskStartTime > taskReport.getStartTime()) {
      firstTaskStartTime = taskReport.getStartTime();
      firstTaskToStart = taskReport.getTaskId();
    }
    if (lastTaskFinishTime == -1
        || lastTaskFinishTime < taskReport.getFinishTime()) {
      lastTaskFinishTime = taskReport.getFinishTime();
      lastTaskToFinish = taskReport.getTaskId();
    }

    if (!taskReport.getTaskState().equals(
        TaskState.SUCCEEDED)) {
      // ignore non-successful tasks when calculating durations
      return;
    }

    ++numSuccessfulTasks;
    long taskDuration = taskReport.getFinishTime() -
        taskReport.getStartTime();

    if (minTaskDuration == -1
      || minTaskDuration > taskDuration) {
      minTaskDuration = taskDuration;
      shortestDurationTask = taskReport.getTaskId();
    }
    if (maxTaskDuration == -1
      || maxTaskDuration < taskDuration) {
      maxTaskDuration = taskDuration;
      longestDurationTask = taskReport.getTaskId();
    }

    avgTaskDuration = ((avgTaskDuration * (numSuccessfulTasks-1)) + taskDuration)
        /numSuccessfulTasks;
  }

  @Override
  public String toString() {
    return "firstTaskStartTime=" + firstTaskStartTime
        + ", firstTaskToStart="
        + (firstTaskToStart == null? "null" : firstTaskToStart)
        + ", lastTaskFinishTime=" + lastTaskFinishTime
        + ", lastTaskToFinish="
        + (lastTaskToFinish == null ? "null" : lastTaskToFinish)
        + ", minTaskDuration=" + minTaskDuration
        + ", maxTaskDuration=" + maxTaskDuration
        + ", avgTaskDuration=" + avgTaskDuration
        + ", numSuccessfulTasks=" + numSuccessfulTasks
        + ", shortestDurationTask="
        + (shortestDurationTask == null ? "null" : shortestDurationTask)
        + ", longestDurationTask="
        + (longestDurationTask == null ? "null" : longestDurationTask);
  }
}
