/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.history.logging.ats;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.CacheId;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.hadoop.yarn.server.timeline.TimelineCacheIdPlugin;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;

import com.google.common.collect.Sets;

public class TimelineCachePluginImpl extends TimelineCacheIdPlugin {

  private static Set<String> summaryEntityTypes;
  private static Set<String> knownEntityTypes;

  static {
    knownEntityTypes = Sets.newHashSet(
        EntityTypes.TEZ_DAG_ID.name(),
        EntityTypes.TEZ_VERTEX_ID.name(),
        EntityTypes.TEZ_TASK_ID.name(),
        EntityTypes.TEZ_TASK_ATTEMPT_ID.name());
    summaryEntityTypes = Sets.newHashSet(
        EntityTypes.TEZ_DAG_ID.name(),
        EntityTypes.TEZ_APPLICATION_ATTEMPT.name(),
        EntityTypes.TEZ_APPLICATION.name());
  }

  // Empty public constructor
  public TimelineCachePluginImpl() {
  }

  private CacheId convertToCacheId(String entityType, String entityId) {
    if (entityType == null || entityType.isEmpty()
        || entityId == null || entityId.isEmpty()) {
      return null;
    }
    if (entityType.equals(EntityTypes.TEZ_DAG_ID.name())) {
      TezDAGID dagId = TezDAGID.fromString(entityId);
      if (dagId != null) {
        return CacheId.newInstance(dagId.getApplicationId(), dagId.toString());
      }
    } else if (entityType.equals(EntityTypes.TEZ_VERTEX_ID.name())) {
      TezVertexID vertexID = TezVertexID.fromString(entityId);
      if (vertexID != null) {
        return CacheId.newInstance(vertexID.getDAGId().getApplicationId(),
            vertexID.getDAGId().toString());
      }

    } else if (entityType.equals(EntityTypes.TEZ_TASK_ID.name())) {
      TezTaskID taskID = TezTaskID.fromString(entityId);
      if (taskID != null) {
        return CacheId.newInstance(taskID.getVertexID().getDAGId().getApplicationId(),
            taskID.getVertexID().getDAGId().toString());
      }
    } else if (entityType.equals(EntityTypes.TEZ_TASK_ATTEMPT_ID.name())) {
      TezTaskAttemptID taskAttemptID = TezTaskAttemptID.fromString(entityId);
      if (taskAttemptID != null) {
        return CacheId.newInstance(
            taskAttemptID.getTaskID().getVertexID().getDAGId().getApplicationId(),
            taskAttemptID.getTaskID().getVertexID().getDAGId().toString());
      }
    }
    return null;
  }


  @Override
  public Set<CacheId> getCacheId(String entityType,
      NameValuePair primaryFilter,
      Collection<NameValuePair> secondaryFilters) {
    if (!knownEntityTypes.contains(entityType)
        || primaryFilter == null
        || !knownEntityTypes.contains(primaryFilter.getName())
        || summaryEntityTypes.contains(entityType)) {
      return null;
    }
    CacheId cacheId = convertToCacheId(primaryFilter.getName(),
        primaryFilter.getValue().toString());
    if (cacheId != null) {
      CacheId appCacheId = CacheId.newInstance(cacheId.getApplicationId(),
          cacheId.getApplicationId().toString());
      return Sets.newHashSet(cacheId, appCacheId);
    }
    return null;
  }

  @Override
  public Set<CacheId> getCacheId(String entityId, String entityType) {
    if (!knownEntityTypes.contains(entityType) || summaryEntityTypes.contains(entityType)) {
      return null;
    }
    CacheId cacheId = convertToCacheId(entityType, entityId);
    if (cacheId != null) {
      CacheId appCacheId = CacheId.newInstance(cacheId.getApplicationId(),
          cacheId.getApplicationId().toString());
      return Sets.newHashSet(cacheId, appCacheId);
    }
    return null;
  }

  @Override
  public Set<CacheId> getCacheId(String entityType, SortedSet<String> entityIds,
      Set<String> eventTypes) {
    if (!knownEntityTypes.contains(entityType)
        || summaryEntityTypes.contains(entityType)
        || entityIds == null || entityIds.isEmpty()) {
      return null;
    }
    Set<CacheId> cacheIds = new HashSet<CacheId>();
    Set<ApplicationId> appIdSet = new HashSet<ApplicationId>();

    for (String entityId : entityIds) {
      CacheId cacheId = convertToCacheId(entityType, entityId);
      if (cacheId != null) {
        cacheIds.add(cacheId);
        appIdSet.add(cacheId.getApplicationId());
      }
    }
    for (ApplicationId appId : appIdSet) {
      cacheIds.add(CacheId.newInstance(appId, appId.toString()));
    }
    return cacheIds;
  }

}
