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

package org.apache.tez.dag.api;

import org.apache.hadoop.yarn.api.records.Resource;

import org.apache.tez.client.CallerContext;

import org.junit.Assert;
import org.junit.Test;

public class TestDAG {

  private final int dummyTaskCount = 2;
  private final Resource dummyTaskResource = Resource.newInstance(1, 1);

  @Test(timeout = 5000)
  public void testDuplicatedVertexGroup() {
    Vertex v1 = Vertex.create("v1", ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2", ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = Vertex.create("v3", ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);

    DAG dag = DAG.create("testDAG");
    dag.createVertexGroup("group_1", v1,v2);

    try {
      dag.createVertexGroup("group_1", v2, v3);
      Assert.fail("should fail it due to duplicated VertexGroups");
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertEquals("VertexGroup group_1 already defined!", e.getMessage());
    }
    // it is possible to create vertex group with same member but different group name 
    dag.createVertexGroup("group_2", v1, v2);

  }

  @Test
  public void testCallerContext() {
    try {
      CallerContext.create("ctxt", "", "", "desc");
      Assert.fail("Expected failure for invalid args");
    } catch (Exception e) {
      // Expected
    }
    try {
      CallerContext.create("", "desc");
      Assert.fail("Expected failure for invalid args");
    } catch (Exception e) {
      // Expected
    }

    CallerContext.create("ctxt", "a", "a", "desc");
    CallerContext.create("ctxt", null);

    CallerContext callerContext = CallerContext.create("ctxt", "desc");
    Assert.assertTrue(callerContext.toString().contains("desc"));
    Assert.assertFalse(callerContext.contextAsSimpleString().contains("desc"));

  }

}
