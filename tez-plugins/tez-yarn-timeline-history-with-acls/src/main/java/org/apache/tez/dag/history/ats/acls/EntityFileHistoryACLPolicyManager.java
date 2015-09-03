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

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.tez.common.security.ACLConfigurationParser;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.common.security.ACLType;
import org.apache.tez.common.security.DAGAccessControls;
import org.apache.tez.common.security.HistoryACLPolicyException;
import org.apache.tez.common.security.HistoryACLPolicyManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.codehaus.jackson.util.MinimalPrettyPrinter;
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO: refactor to commonize code with ATSHistoryACLPolicyManager
public class EntityFileHistoryACLPolicyManager implements
    HistoryACLPolicyManager {

  private final static Logger LOG = LoggerFactory.getLogger(
      EntityFileHistoryACLPolicyManager.class);
  final static String DOMAIN_ID_PREFIX = "Tez_ATS_";

  //TODO: get these from YarnConfiguration
  public static final String TIMELINE_SERVICE_ENTITYFILE_ACTIVE_DIR =
      "yarn.timeline-service.entity-file-store.active-dir";
  public static final String TIMELINE_SERVICE_ENTITYFILE_ACTIVE_DIR_DEFAULT =
      "/tmp/entity-file-history/active";

  //TODO: Commonize with EntityFileLoggingService versions
  // App log directory must be readable by group so server can access logs
  // and writable by group so it can be deleted by server
  private static final FsPermission APP_LOG_DIR_PERMISSION =
      new FsPermission((short)0770);
  // Logs must be readable by group so server can access them
  private static final FsPermission FILE_LOG_PERMISSION =
      new FsPermission((short)0640);

  private static final String CONF_FILENAME = "conf.xml";
  private static final String DOMAIN_LOG_PREFIX = "domainlog-";

  Configuration conf;
  JsonFactory jsonFactory = new JsonFactory();
  ObjectMapper objMapper = null;
  FileSystem fs = null;
  Path activePath = null;

  @Override
  public synchronized void setConf(Configuration config) {
    this.conf = config;
    activePath = new Path(conf.get(
        TIMELINE_SERVICE_ENTITYFILE_ACTIVE_DIR,
        TIMELINE_SERVICE_ENTITYFILE_ACTIVE_DIR_DEFAULT));
    try {
      fs = activePath.getFileSystem(conf);
      if (!fs.exists(activePath)) {
        throw new TezUncheckedException(activePath + " does not exist");
      }
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
  }

  @Override
  public synchronized Configuration getConf() {
    return conf;
  }

  @Override
  public Map<String, String> setupSessionACLs(Configuration tezConf,
      ApplicationId applicationId) throws IOException,
      HistoryACLPolicyException {
    return createSessionDomain(tezConf, applicationId, null);
  }

  @Override
  public Map<String, String> setupNonSessionACLs(Configuration tezConf,
      ApplicationId applicationId, DAGAccessControls dagAccessControls)
      throws IOException, HistoryACLPolicyException {
    return createSessionDomain(tezConf, applicationId, null);
  }

  @Override
  public Map<String, String> setupSessionDAGACLs(Configuration tezConf,
      ApplicationId applicationId, String dagName,
      DAGAccessControls dagAccessControls) throws IOException,
      HistoryACLPolicyException {
    return createDAGDomain(tezConf, applicationId, dagName, dagAccessControls);
  }

  @Override
  public void updateTimelineEntityDomain(Object timelineEntity, String domainId) {
    if (!(timelineEntity instanceof TimelineEntity)) {
      throw new UnsupportedOperationException("Invalid object provided of type"
          + timelineEntity.getClass().getName());
    }
    TimelineEntity entity = (TimelineEntity) timelineEntity;
    entity.setDomainId(domainId);
  }

  private Map<String, String> createSessionDomain(Configuration tezConf,
      ApplicationId appId, DAGAccessControls dagAccessControls)
          throws IOException, HistoryACLPolicyException {
    String domainId =
        tezConf.get(TezConfiguration.YARN_ATS_ACL_SESSION_DOMAIN_ID);
    if (!tezConf.getBoolean(TezConfiguration.TEZ_AM_ACLS_ENABLED,
        TezConfiguration.TEZ_AM_ACLS_ENABLED_DEFAULT)) {
      if (domainId != null) {
        throw new TezUncheckedException("ACLs disabled but DomainId is specified"
            + ", aclsEnabled=true, domainId=" + domainId);
      }
      return null;
    }

    boolean autoCreateDomain = tezConf.getBoolean(TezConfiguration.YARN_ATS_ACL_DOMAINS_AUTO_CREATE,
        TezConfiguration.YARN_ATS_ACL_DOMAINS_AUTO_CREATE_DEFAULT);

    if (domainId != null) {
      // do nothing
      LOG.info("Using specified domainId with Timeline, domainId=" + domainId);
      return null;
    } else {
      if (!autoCreateDomain) {
        // Error - Cannot fallback to default as that leaves ACLs open
        throw new TezUncheckedException("Timeline DomainId is not specified and auto-create"
            + " Domains is disabled");
      }
      domainId = DOMAIN_ID_PREFIX + appId.toString();
      createTimelineDomain(domainId, tezConf, appId, dagAccessControls);
      LOG.info("Created Timeline Domain for History ACLs, domainId=" + domainId);
      return Collections.singletonMap(TezConfiguration.YARN_ATS_ACL_SESSION_DOMAIN_ID, domainId);
    }
  }

  private Map<String, String> createDAGDomain(Configuration tezConf,
      ApplicationId appId, String dagName, DAGAccessControls dagAccessControls)
      throws IOException, HistoryACLPolicyException {
    if (dagAccessControls == null) {
      // No DAG specific ACLs
      return null;
    }

    String domainId =
        tezConf.get(TezConfiguration.YARN_ATS_ACL_DAG_DOMAIN_ID);
    if (!tezConf.getBoolean(TezConfiguration.TEZ_AM_ACLS_ENABLED,
        TezConfiguration.TEZ_AM_ACLS_ENABLED_DEFAULT)) {
      if (domainId != null) {
        throw new TezUncheckedException("ACLs disabled but domainId for DAG is specified"
            + ", aclsEnabled=true, domainId=" + domainId);
      }
      return null;
    }

    boolean autoCreateDomain = tezConf.getBoolean(TezConfiguration.YARN_ATS_ACL_DOMAINS_AUTO_CREATE,
        TezConfiguration.YARN_ATS_ACL_DOMAINS_AUTO_CREATE_DEFAULT);

    if (domainId != null) {
      // do nothing
      LOG.info("Using specified domainId with Timeline, domainId=" + domainId);
      return null;
    } else {
      if (!autoCreateDomain) {
        // Error - Cannot fallback to default as that leaves ACLs open
        throw new TezUncheckedException("Timeline DomainId is not specified and auto-create"
            + " Domains is disabled");
      }

      domainId = DOMAIN_ID_PREFIX + appId.toString() + "_" + dagName;
      createTimelineDomain(domainId, tezConf, appId, dagAccessControls);
      LOG.info("Created Timeline Domain for DAG-specific History ACLs, domainId=" + domainId);
      return Collections.singletonMap(TezConfiguration.YARN_ATS_ACL_DAG_DOMAIN_ID, domainId);
    }
  }

  private void createTimelineDomain(String domainId,
      Configuration tezConf, ApplicationId appId,
      DAGAccessControls dagAccessControls)
          throws IOException, HistoryACLPolicyException {
    String user = UserGroupInformation.getCurrentUser().getShortUserName();

    TimelineDomain timelineDomain = new TimelineDomain();
    timelineDomain.setId(domainId);

    ACLConfigurationParser parser = new ACLConfigurationParser(tezConf, false);
    timelineDomain.setReaders(getMergedViewACLs(user, parser,
        dagAccessControls));
    timelineDomain.setWriters(user);

    writeDomain(appId, timelineDomain);
  }

  private synchronized void writeDomain(ApplicationId appId,
      TimelineDomain domain) throws IOException, HistoryACLPolicyException {
    if (objMapper == null) {
      objMapper = new ObjectMapper();
      objMapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector());
      objMapper.setSerializationInclusion(Inclusion.NON_NULL);
      objMapper.configure(Feature.CLOSE_CLOSEABLE, false);
    }

    String appIdStr = appId.toString();
    Path appDir = createAppDir(appIdStr);
    writeConf(appDir);

    FSDataOutputStream out = null;
    try {
      out = createDomainFile(appDir, appIdStr);
      JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(out);
      jsonGenerator.setPrettyPrinter(new MinimalPrettyPrinter("\n"));
      objMapper.writeValue(jsonGenerator, domain);
      jsonGenerator.close();
      out.close();
      out = null;
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  private String getMergedViewACLs(String user, ACLConfigurationParser parser,
      DAGAccessControls dagAccessControls) {
    Map<ACLType, Set<String>> allowedUsers = parser.getAllowedUsers();
    Map<ACLType, Set<String>> allowedGroups = parser.getAllowedGroups();

    Set<String> viewUsers = new HashSet<String>();
    viewUsers.add(user);
    if (allowedUsers.containsKey(ACLType.AM_VIEW_ACL)) {
      viewUsers.addAll(allowedUsers.get(ACLType.AM_VIEW_ACL));
    }
    if (dagAccessControls != null && dagAccessControls.getUsersWithViewACLs() != null) {
      viewUsers.addAll(dagAccessControls.getUsersWithViewACLs());
    }

    if (viewUsers.contains(ACLManager.WILDCARD_ACL_VALUE)) {
      return ACLManager.WILDCARD_ACL_VALUE;
    }

    Set<String> viewGroups = new HashSet<String>();
    if (allowedGroups.containsKey(ACLType.AM_VIEW_ACL)) {
      viewGroups.addAll(allowedGroups.get(ACLType.AM_VIEW_ACL));
    }
    if (dagAccessControls != null && dagAccessControls.getGroupsWithViewACLs() != null) {
      viewGroups.addAll(dagAccessControls.getGroupsWithViewACLs());
    }

    return ACLManager.toCommaSeparatedString(viewUsers) + " " +
        ACLManager.toCommaSeparatedString(viewGroups);
  }

  private Path createAppDir(String appIdStr) throws IOException {
    Path appDir = new Path(activePath, appIdStr);
    if (!fs.exists(appDir)) {
      FileSystem.mkdirs(fs, appDir, APP_LOG_DIR_PERMISSION);
    }
    return appDir;
  }

  private FSDataOutputStream createDomainFile(Path appDir, String appIdStr)
      throws IOException {
    Path logPath = new Path(appDir, DOMAIN_LOG_PREFIX + appIdStr);
    LOG.info("Writing domains for " + appIdStr + " to " + logPath);
    FSDataOutputStream stream;
    if (!fs.exists(logPath)) {
      stream = fs.create(logPath, false);
      fs.setPermission(logPath, FILE_LOG_PERMISSION);
    } else {
      stream = fs.append(logPath);
    }
    return stream;
  }

  private void writeConf(Path appDir) throws IOException {
    Path confPath = new Path(appDir, CONF_FILENAME);
    if (!fs.exists(confPath)) {
      FSDataOutputStream out = FileSystem.create(fs, confPath,
          FILE_LOG_PERMISSION);
      try {
        conf.writeXml(out);
      } finally {
        out.close();
      }
    }
  }
}
