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

package org.apache.tez.dag.history.ats.acls;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;


import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.tez.dag.history.logging.ats.ATSV15HistoryLoggingService;
import org.apache.tez.test.MiniTezCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.runtime.library.processor.SleepProcessor;
import org.apache.tez.runtime.library.processor.SleepProcessor.SleepProcessorConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestATSHistoryV15 {

  private static final Logger LOG = LoggerFactory.getLogger(TestATSHistoryV15.class);

  protected static MiniTezCluster mrrTezCluster = null;
  protected static MiniDFSCluster dfsCluster = null;
  private static String timelineAddress;
  private Random random = new Random();

  private static Configuration conf = new Configuration();
  private static FileSystem remoteFs;

  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestATSHistoryV15.class.getName() + "-tmpDir";

  private static String user;

  @BeforeClass
  public static void setup() throws IOException {
    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).racks(null)
          .build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }

    if (mrrTezCluster == null) {
      try {
        mrrTezCluster = new MiniTezCluster(TestATSHistoryV15.class.getName(),
            1, 1, 1);
        Configuration conf = new Configuration();
        conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
        conf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
        conf.setInt("yarn.nodemanager.delete.debug-delay-sec", 20000);
        mrrTezCluster.init(conf);
        mrrTezCluster.start();
      } catch (Throwable e) {
        LOG.info("Failed to start Mini Tez Cluster", e);
      }
    }
    user = UserGroupInformation.getCurrentUser().getShortUserName();
    timelineAddress = mrrTezCluster.getConfig().get(
        YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS);
    if (timelineAddress != null) {
      // Hack to handle bug in MiniYARNCluster handling of webapp address
      timelineAddress = timelineAddress.replace("0.0.0.0", "localhost");
    }
  }

  @AfterClass
  public static void tearDown() throws InterruptedException {
    LOG.info("Shutdown invoked");
    Thread.sleep(10000);
    if (mrrTezCluster != null) {
      mrrTezCluster.stop();
      mrrTezCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  @Test (timeout=50000)
  public void testSimpleDAG() throws Exception {
    TezClient tezSession = null;
    ApplicationId applicationId;
    String viewAcls = "nobody nobody_group";
    try {
      SleepProcessorConfig spConf = new SleepProcessorConfig(1);

      DAG dag = DAG.create("TezSleepProcessor");
      Vertex vertex = Vertex.create("SleepVertex", ProcessorDescriptor.create(
              SleepProcessor.class.getName()).setUserPayload(spConf.toUserPayload()), 1,
          Resource.newInstance(256, 1));
      dag.addVertex(vertex);

      TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());

      Path atsActivePath = new Path("/tmp/ats/active/" + random.nextInt(100000));
      Path atsDonePath = new Path("/tmp/ats/done/" + random.nextInt(100000));

      remoteFs.mkdirs(atsActivePath);
      remoteFs.mkdirs(atsDonePath);

      tezConf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
      tezConf.set(YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_ACTIVE_DIR,
          remoteFs.resolvePath(atsActivePath).toString());
      tezConf.set(YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_DONE_DIR,
          remoteFs.resolvePath(atsDonePath).toString());
      tezConf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_PLUGIN_ENABLED, true);
      tezConf.set(YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_SUMMARY_ENTITY_TYPES, "TEZ_DAG");

      tezConf.set(TezConfiguration.TEZ_AM_VIEW_ACLS, viewAcls);
      tezConf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS,
          ATSV15HistoryLoggingService.class.getName());
      Path remoteStagingDir = remoteFs.makeQualified(new Path("/tmp", String.valueOf(random
          .nextInt(100000))));
      remoteFs.mkdirs(remoteStagingDir);
      tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());

      tezSession = TezClient.create("TezSleepProcessor", tezConf, true);
      tezSession.start();

      applicationId = tezSession.getAppMasterApplicationId();

      DAGClient dagClient = tezSession.submitDAG(dag);

      DAGStatus dagStatus = dagClient.getDAGStatus(null);
      while (!dagStatus.isCompleted()) {
        LOG.info("Waiting for job to complete. Sleeping for 500ms." + " Current state: "
            + dagStatus.getState());
        Thread.sleep(500l);
        dagStatus = dagClient.getDAGStatus(null);
      }
      assertEquals(DAGStatus.State.SUCCEEDED, dagStatus.getState());

      // Verify HDFS data
      int count = verifyATSDataOnHDFS(atsActivePath, 0, applicationId);
      Assert.assertTrue("Count is: " + count, count > 0);

    } finally {
      if (tezSession != null) {
        tezSession.stop();
      }
    }

  }

  private int verifyATSDataOnHDFS(Path p, int count, ApplicationId applicationId) throws IOException {
    RemoteIterator<LocatedFileStatus> iter = remoteFs.listFiles(p, true);
    while (iter.hasNext()) {
      LocatedFileStatus f = iter.next();
      LOG.info("Found file " + f.toString());
      if (f.isDirectory()) {
        verifyATSDataOnHDFS(f.getPath(), count, applicationId);
      } else {
        if (f.getPath().getName().contains(
            "" + applicationId.getClusterTimestamp() + "_" + applicationId.getId())) {
          ++count;
        }
      }
    }
    return count;
  }

}
