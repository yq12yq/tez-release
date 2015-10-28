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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.CacheId;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTimelineCachePluginImpl {

  static ApplicationId appId1;
  static ApplicationAttemptId appAttemptId1;
  static ApplicationId appId2;
  static TezDAGID dagID1;
  static TezVertexID vertexID1;
  static TezTaskID taskID1;
  static TezTaskAttemptID attemptID1;
  static TezDAGID dagID2;
  static TezVertexID vertexID2;
  static TezTaskID taskID2;
  static TezTaskAttemptID attemptID2;
  static Map<String, String> typeIdMap1;
  static Map<String, String> typeIdMap2;

  TimelineCachePluginImpl plugin =
      new TimelineCachePluginImpl();

  @BeforeClass
  public static void beforeClass() {
    appId1 = ApplicationId.newInstance(1000l, 111);
    appId2 = ApplicationId.newInstance(1001l, 121);
    appAttemptId1 = ApplicationAttemptId.newInstance(appId1, 11);

    dagID1 = TezDAGID.getInstance(appId1, 1);
    vertexID1 = TezVertexID.getInstance(dagID1, 12);
    taskID1 = TezTaskID.getInstance(vertexID1, 11144);
    attemptID1 = TezTaskAttemptID.getInstance(taskID1, 4);

    dagID2 = TezDAGID.getInstance(appId2, 111);
    vertexID2 = TezVertexID.getInstance(dagID2, 121);
    taskID2 = TezTaskID.getInstance(vertexID2, 113344);
    attemptID2 = TezTaskAttemptID.getInstance(taskID2, 14);

    typeIdMap1 = new HashMap<String, String>();
    typeIdMap1.put(EntityTypes.TEZ_DAG_ID.name(), dagID1.toString());
    typeIdMap1.put(EntityTypes.TEZ_VERTEX_ID.name(), vertexID1.toString());
    typeIdMap1.put(EntityTypes.TEZ_TASK_ID.name(), taskID1.toString());
    typeIdMap1.put(EntityTypes.TEZ_TASK_ATTEMPT_ID.name(), attemptID1.toString());

    typeIdMap2 = new HashMap<String, String>();
    typeIdMap2.put(EntityTypes.TEZ_DAG_ID.name(), dagID2.toString());
    typeIdMap2.put(EntityTypes.TEZ_VERTEX_ID.name(), vertexID2.toString());
    typeIdMap2.put(EntityTypes.TEZ_TASK_ID.name(), taskID2.toString());
    typeIdMap2.put(EntityTypes.TEZ_TASK_ATTEMPT_ID.name(), attemptID2.toString());

  }

  @Test
  public void testGetCacheIdByPrimaryFilter() {
    for (Entry<String, String> entry : typeIdMap1.entrySet()) {
      NameValuePair primaryFilter = new NameValuePair(entry.getKey(), entry.getValue());
      Assert.assertNull(plugin.getCacheId(EntityTypes.TEZ_APPLICATION.name(), primaryFilter, null));
      Set<CacheId> cacheIds = plugin.getCacheId(entry.getKey(), primaryFilter, null);
      if (entry.getKey().equals(EntityTypes.TEZ_DAG_ID.name())) {
        Assert.assertNull(cacheIds);
        continue;
      }
      Assert.assertEquals(2, cacheIds.size());
      Iterator<CacheId> iter = cacheIds.iterator();
      while (iter.hasNext()) {
        CacheId cacheId = iter.next();
        Assert.assertEquals(appId1, cacheId.getApplicationId());
        Assert.assertTrue((dagID1.toString().equals(cacheId.getCacheId()))
            || (appId1.toString().equals(cacheId.getCacheId())));
      }
    }
  }

  @Test
  public void testGetCacheIdById() {
    for (Entry<String, String> entry : typeIdMap1.entrySet()) {
      Set<CacheId> cacheIds = plugin.getCacheId(entry.getValue(), entry.getKey());
      if (entry.getKey().equals(EntityTypes.TEZ_DAG_ID.name())) {
        Assert.assertNull(cacheIds);
        continue;
      }
      Assert.assertEquals(2, cacheIds.size());
      Iterator<CacheId> iter = cacheIds.iterator();
      while (iter.hasNext()) {
        CacheId cacheId = iter.next();
        Assert.assertEquals(appId1, cacheId.getApplicationId());
        Assert.assertTrue((dagID1.toString().equals(cacheId.getCacheId()))
            || (appId1.toString().equals(cacheId.getCacheId())));
      }
    }
  }

  @Test
  public void testGetCacheIdByIds() {
    for (Entry<String, String> entry : typeIdMap1.entrySet()) {
      SortedSet<String> entityIds = new TreeSet<String>();
      entityIds.add(entry.getValue());
      entityIds.add(typeIdMap2.get(entry.getKey()));
      Set<CacheId> cacheIds = plugin.getCacheId(entry.getKey(), entityIds, null);
      if (entry.getKey().equals(EntityTypes.TEZ_DAG_ID.name())) {
        Assert.assertNull(cacheIds);
        continue;
      }
      Assert.assertEquals(4, cacheIds.size());
      int found = 0;
      Iterator<CacheId> iter = cacheIds.iterator();
      while (iter.hasNext()) {
        CacheId cacheId = iter.next();
        if (cacheId.getApplicationId().equals(appId1)
            && cacheId.getCacheId().equals(dagID1.toString())) {
          ++found;
        } else if (cacheId.getApplicationId().equals(appId2)
            && cacheId.getCacheId().equals(dagID2.toString())) {
          ++found;
        } else if (cacheId.getApplicationId().equals(appId1)
            && cacheId.getCacheId().equals(appId1.toString())) {
          ++found;
        } else if (cacheId.getApplicationId().equals(appId2)
            && cacheId.getCacheId().equals(appId2.toString())) {
          ++found;
        }
      }
      Assert.assertEquals("All cacheIds not returned", 4, found);
    }
  }

  @Test
  public void testInvalidIds() {
    Assert.assertNull(plugin.getCacheId(EntityTypes.TEZ_DAG_ID.name(), vertexID1.toString()));
    Assert.assertNull(plugin.getCacheId(EntityTypes.TEZ_VERTEX_ID.name(), taskID1.toString()));
    Assert.assertNull(plugin.getCacheId(EntityTypes.TEZ_TASK_ID.name(), attemptID1.toString()));
    Assert.assertNull(plugin.getCacheId(EntityTypes.TEZ_TASK_ATTEMPT_ID.name(), dagID1.toString()));
    Assert.assertNull(plugin.getCacheId("", ""));
    Assert.assertNull(plugin.getCacheId(null, null));
    Assert.assertNull(plugin.getCacheId("adadasd", EntityTypes.TEZ_DAG_ID.name()));
  }

  @Test
  public void testInvalidTypeRequests() {
    Assert.assertNull(plugin.getCacheId(EntityTypes.TEZ_APPLICATION.name(), appId1.toString()));
    Assert.assertNull(plugin.getCacheId(EntityTypes.TEZ_APPLICATION_ATTEMPT.name(),
        appAttemptId1.toString()));
    Assert.assertNull(plugin.getCacheId(EntityTypes.TEZ_CONTAINER_ID.name(), appId1.toString()));

    Assert.assertNull(plugin.getCacheId(EntityTypes.TEZ_TASK_ID.name(), null,
        new HashSet<String>()));
    Assert.assertNull(plugin.getCacheId(EntityTypes.TEZ_TASK_ID.name(), null,
        new HashSet<NameValuePair>()));

  }

}
