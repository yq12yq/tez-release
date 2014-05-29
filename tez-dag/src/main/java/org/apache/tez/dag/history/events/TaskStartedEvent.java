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

package org.apache.tez.dag.history.events;

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.ats.EntityTypes;
import org.apache.tez.dag.history.utils.ATSConstants;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.recovery.records.RecoveryProtos.TaskStartedProto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class TaskStartedEvent implements HistoryEvent {

  private TezTaskID taskID;
  private String vertexName;
  private long scheduledTime;
  private long startTime;

  public TaskStartedEvent(TezTaskID taskId,
      String vertexName, long scheduledTime, long startTime) {
    this.vertexName = vertexName;
    this.taskID = taskId;
    this.scheduledTime = scheduledTime;
    this.startTime = startTime;
  }

  public TaskStartedEvent() {
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.TASK_STARTED;
  }

  @Override
  public boolean isRecoveryEvent() {
    return true;
  }

  @Override
  public boolean isHistoryEvent() {
    return true;
  }

  public TaskStartedProto toProto() {
    return TaskStartedProto.newBuilder()
        .setTaskId(taskID.toString())
        .setLaunchTime(startTime)
        .setScheduledTime(scheduledTime)
        .build();
  }

  public void fromProto(TaskStartedProto proto) {
    this.taskID = TezTaskID.fromString(proto.getTaskId());
    this.startTime = proto.getLaunchTime();
    this.scheduledTime = proto.getScheduledTime();
  }

  @Override
  public void toProtoStream(OutputStream outputStream) throws IOException {
    toProto().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    TaskStartedProto proto = TaskStartedProto.parseDelimitedFrom(inputStream);
    if (proto == null) {
      throw new IOException("No data found in stream");
    }
    fromProto(proto);
  }

  @Override
  public TimelineEntity convertToTimelineEntity() {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(taskID.toString());
    atsEntity.setEntityType(EntityTypes.TEZ_TASK_ID.name());

    atsEntity.addRelatedEntity(EntityTypes.TEZ_VERTEX_ID.name(),
        taskID.getVertexID().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_DAG_ID.name(),
        taskID.getVertexID().getDAGId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_VERTEX_ID.name(),
        taskID.getVertexID().toString());

    TimelineEvent startEvt = new TimelineEvent();
    startEvt.setEventType(HistoryEventType.TASK_STARTED.name());
    startEvt.setTimestamp(startTime);
    atsEntity.addEvent(startEvt);

    atsEntity.addOtherInfo(ATSConstants.START_TIME, startTime);
    atsEntity.addOtherInfo(ATSConstants.SCHEDULED_TIME, scheduledTime);

    return atsEntity;
  }

  @Override
  public String toString() {
    return "vertexName=" + vertexName
        + ", taskId=" + taskID.toString()
        + ", scheduledTime=" + scheduledTime
        + ", launchTime=" + startTime;
  }

  public TezTaskID getTaskID() {
    return taskID;
  }

  public long getScheduledTime() {
    return scheduledTime;
  }

  public long getStartTime() {
    return startTime;
  }

}
