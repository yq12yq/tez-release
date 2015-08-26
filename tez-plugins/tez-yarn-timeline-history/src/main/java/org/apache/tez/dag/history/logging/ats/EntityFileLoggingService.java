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

package org.apache.tez.dag.history.logging.ats;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.security.HistoryACLPolicyManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.DAGRecoveredEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.logging.HistoryLoggingService;
import org.apache.tez.dag.records.TezDAGID;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.codehaus.jackson.util.MinimalPrettyPrinter;
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector;

public class EntityFileLoggingService extends HistoryLoggingService {

  private static final Log LOG =
      LogFactory.getLog(EntityFileLoggingService.class);

  // App log directory must be readable by group so server can access logs
  // and writable by group so it can be deleted by server
  private static final short APP_LOG_DIR_PERMISSIONS = 0770;
  // Logs must be readable by group so server can access them
  private static final short FILE_LOG_PERMISSIONS = 0640;

  private static final String TEZ_ENTITY_FILE_HISTORY_FLUSH_INTERVAL_SECS =
      "tez.entity-file-history.flush-interval-secs";
  private static final long
      TEZ_ENTITY_FILE_HISTORY_FLUSH_INTERVAL_SECS_DEFAULT = 10;

  //TODO: get these from YarnConfiguration
  public static final String TIMELINE_SERVICE_ENTITYFILE_ACTIVE_DIR =
      "yarn.timeline-service.entity-file-store.active-dir";
  public static final String TIMELINE_SERVICE_ENTITYFILE_ACTIVE_DIR_DEFAULT = "/tmp/entity-file-history/active";
  public static final String TIMELINE_SERVICE_ENTITYFILE_SUMMARY_ENTITY_TYPES =
      "yarn.timeline-service.entity-file-store.summary-entity-types";

  private static final String ACL_MANAGER_CLASS_NAME =
      "org.apache.tez.dag.history.ats.acls.EntityFileHistoryACLPolicyManager";

  public static final String SUMMARY_LOG_PREFIX = "summarylog-";
  public static final String ENTITY_LOG_PREFIX = "entitylog-";

  private LinkedBlockingQueue<DAGHistoryEvent> eventQueue =
      new LinkedBlockingQueue<DAGHistoryEvent>();
  private Thread eventHandlerThread;
  private AtomicBoolean stopped = new AtomicBoolean(false);
  private int eventCounter = 0;
  private HistoryACLPolicyManager aclPolicyManager;
  private HashSet<TezDAGID> skippedDAGs = new HashSet<TezDAGID>();
  private Map<TezDAGID, String> dagDomainIdMap =
      new HashMap<TezDAGID, String>();
  private String sessionDomainId;
  private EntityLog summaryLog;
  private EntityLog entityLog;
  private ObjectMapper objMapper;
  private Set<String> summaryEntityTypes;
  private volatile Exception exception = null;

  public EntityFileLoggingService() {
    super(EntityFileLoggingService.class.getSimpleName());
  }

  private static Path setupLogDir(Configuration conf, ApplicationId appId)
      throws IOException {
    Path activePath = new Path(conf.get(
        TIMELINE_SERVICE_ENTITYFILE_ACTIVE_DIR,
        TIMELINE_SERVICE_ENTITYFILE_ACTIVE_DIR_DEFAULT));
    FileSystem fs = activePath.getFileSystem(conf);
    if (!fs.exists(activePath)) {
      throw new IOException(activePath + " does not exist");
    }
    Path appDir = new Path(activePath, appId.toString());
    if (!fs.exists(appDir)) {
      FileSystem.mkdirs(fs, appDir, new FsPermission(APP_LOG_DIR_PERMISSIONS));
    }
    return appDir;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    LOG.info("Initializing " + EntityFileLoggingService.class.getSimpleName());
    sessionDomainId =
        conf.get(TezConfiguration.YARN_ATS_ACL_SESSION_DOMAIN_ID);
    LOG.info("Using " + ACL_MANAGER_CLASS_NAME + " to manage Timeline ACLs");
    try {
      aclPolicyManager = ReflectionUtils.createClazzInstance(
          ACL_MANAGER_CLASS_NAME);
      aclPolicyManager.setConf(conf);
    } catch (TezUncheckedException e) {
      LOG.warn("Could not instantiate object for " + ACL_MANAGER_CLASS_NAME
          + ". ACLs cannot be enforced correctly for history data in Timeline",
          e);
      if (!conf.getBoolean(
          TezConfiguration.TEZ_AM_ALLOW_DISABLED_TIMELINE_DOMAINS,
          TezConfiguration.TEZ_AM_ALLOW_DISABLED_TIMELINE_DOMAINS_DEFAULT)) {
        throw e;
      }
      aclPolicyManager = null;
    }

    Collection<String> filterStrings = conf.getStringCollection(
        TIMELINE_SERVICE_ENTITYFILE_SUMMARY_ENTITY_TYPES);
    if (filterStrings.isEmpty()) {
      throw new IllegalArgumentException(
          TIMELINE_SERVICE_ENTITYFILE_SUMMARY_ENTITY_TYPES + " is not set");
    }
    LOG.info("Entity types for summary store: " + filterStrings);
    summaryEntityTypes = new HashSet<String>(filterStrings);
  }

  @Override
  public void serviceStart() {
    LOG.info("Starting " + EntityFileLoggingService.class.getSimpleName());
    Configuration conf = getConfig();
    objMapper = createObjectMapper();

    try {
      ApplicationAttemptId attemptId = appContext.getApplicationAttemptId();
      Path logDir = setupLogDir(conf, attemptId.getApplicationId());
      FileSystem fs = logDir.getFileSystem(conf);
      Path logPath = new Path(logDir,
          SUMMARY_LOG_PREFIX + attemptId.toString());
      summaryLog = new EntityLog(conf, fs, logPath, objMapper);
      logPath = new Path(logDir, ENTITY_LOG_PREFIX + attemptId.toString());
      entityLog = new EntityLog(conf, fs, logPath, objMapper);
    } catch (IOException e) {
      throw new TezUncheckedException("Error initializing entity logs", e);
    }

    eventHandlerThread = new Thread(new EventProcessor(),
        EntityFileLoggingService.class.getSimpleName() + "EventHandler");
    eventHandlerThread.start();
  }

  private ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector());
    mapper.setSerializationInclusion(Inclusion.NON_NULL);
    mapper.configure(Feature.CLOSE_CLOSEABLE, false);
    return mapper;
  }

  @Override
  public void serviceStop() {
    LOG.info("Stopping " + EntityFileLoggingService.class.getSimpleName()
        + ", eventQueueBacklog=" + eventQueue.size());
    stopped.set(true);
    try {
      if (eventHandlerThread != null) {
        eventHandlerThread.interrupt();
        LOG.debug("Waiting for event handler thread to complete");
        eventHandlerThread.join();
      }
    } catch (InterruptedException ie) {
      LOG.info("Interrupted Exception while stopping", ie);
    }

    // write all the events remaining in the queue
    if (!eventQueue.isEmpty()) {
      LOG.info("Writing the remaining " + eventQueue.size() + " events");
      for (DAGHistoryEvent ev : eventQueue) {
        try {
          handleEvent(ev);
        } catch (IOException e) {
          throw new YarnRuntimeException("Error writing log", e);
        }
      }
    }

    IOUtils.cleanup(LOG, summaryLog);
    IOUtils.cleanup(LOG, entityLog);
  }

  @Override
  public void handle(DAGHistoryEvent event) {
    if (exception != null) {
      throw new YarnRuntimeException("Error writing log", exception);
    }
    eventQueue.add(event);
  }

  private boolean isValidEvent(DAGHistoryEvent event) {
    HistoryEventType eventType = event.getHistoryEvent().getEventType();
    TezDAGID dagId = event.getDagID();

    if (dagId == null) {
      return false;
    }

    if (eventType.equals(HistoryEventType.DAG_SUBMITTED)) {
      DAGSubmittedEvent dagSubmittedEvent =
          (DAGSubmittedEvent) event.getHistoryEvent();
      String dagName = dagSubmittedEvent.getDAGName();
      if (dagName != null
          && dagName.startsWith(
          TezConstants.TEZ_PREWARM_DAG_NAME_PREFIX)) {
        // Skip recording pre-warm DAG events
        skippedDAGs.add(dagId);
        return false;
      }
      if (aclPolicyManager != null) {
        String dagDomainId = dagSubmittedEvent.getConf().get(
            TezConfiguration.YARN_ATS_ACL_DAG_DOMAIN_ID);
        if (dagDomainId != null) {
          dagDomainIdMap.put(dagId, dagDomainId);
        }
      }
    }
    if (eventType.equals(HistoryEventType.DAG_RECOVERED)) {
      DAGRecoveredEvent dagRecoveredEvent = (DAGRecoveredEvent) event.getHistoryEvent();
      if (!dagRecoveredEvent.isHistoryLoggingEnabled()) {
        skippedDAGs.add(dagRecoveredEvent.getDagID());
        return false;
      }
    }
    if (eventType.equals(HistoryEventType.DAG_FINISHED)) {
      // Remove from set to keep size small
      // No more events should be seen after this point.
      if (skippedDAGs.remove(dagId)) {
        return false;
      }
    }

    if (dagId != null && skippedDAGs.contains(dagId)) {
      // Skip pre-warm DAGs
      return false;
    }

    return true;
  }

  private void handleEvent(DAGHistoryEvent event) throws IOException {
    if (!isValidEvent(event)) {
      return;
    }

    String domainId = sessionDomainId;
    TezDAGID dagId = event.getDagID();
    if (aclPolicyManager != null && dagId != null) {
      String dagDomainId = dagDomainIdMap.get(dagId);
      if (dagDomainId != null) {
        domainId = dagDomainId;
      }
    }

    TimelineEntity entity = HistoryEventTimelineConversion
        .convertToTimelineEntity(event.getHistoryEvent());
    if (aclPolicyManager != null && domainId != null && !domainId.isEmpty()) {
        aclPolicyManager.updateTimelineEntityDomain(entity, domainId);
    }

    EntityLog log = entityLog;
    if (summaryEntityTypes.contains(entity.getEntityType())) {
      log = summaryLog;
    }
    log.writeEntity(entity);
  }

  private class EventProcessor implements Runnable {
    @Override
    public void run() {
      boolean errorOccurred = false;
      DAGHistoryEvent event = null;
      while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
        // Log the size of the event-queue every so often.
        ++eventCounter;
        if (eventCounter % 1000 == 0) {
          LOG.info("Event queue size=" + eventQueue.size());
        }

        try {
          event = eventQueue.take();
        } catch (InterruptedException e) {
          LOG.info("EventQueue take interrupted. Returning");
          return;
        }

        if (!errorOccurred) {
          try {
            handleEvent(event);
          } catch (Exception e) {
            exception = e;
            errorOccurred = true;
            LOG.error("Error writing entity log", e);
          }
        }
      }
    }
  }

  private static class EntityLog implements Closeable, Flushable {
    private FSDataOutputStream stream;
    private Timer flushTimer;
    private FlushTimerTask flushTimerTask;
    private ObjectMapper objMapper;
    private JsonGenerator jsonGenerator;
    private volatile IOException exception = null;

    public EntityLog(Configuration conf, FileSystem fs, Path logPath,
        ObjectMapper objMapper) throws IOException {
      LOG.info("Writing history to " + logPath);
      this.objMapper = objMapper;
      this.stream = fs.create(logPath, false);
      fs.setPermission(logPath, new FsPermission(FILE_LOG_PERMISSIONS));
      this.jsonGenerator = new JsonFactory().createJsonGenerator(stream);
      this.jsonGenerator.setPrettyPrinter(new MinimalPrettyPrinter("\n"));
      this.flushTimer = new Timer(EntityLog.class.getSimpleName()
          + "FlushTimer", true);
      this.flushTimerTask = new FlushTimerTask();
      long flushIntervalSecs = conf.getLong(
          TEZ_ENTITY_FILE_HISTORY_FLUSH_INTERVAL_SECS,
          TEZ_ENTITY_FILE_HISTORY_FLUSH_INTERVAL_SECS_DEFAULT);
      long flushIntervalMillis = flushIntervalSecs * 1000;
      this.flushTimer.schedule(flushTimerTask, flushIntervalMillis,
          flushIntervalMillis);
    }

    @Override
    public synchronized void close() throws IOException {
      cancelFlushTimer();
      if (stream != null) {
        jsonGenerator.close();
        stream.close();
        stream = null;
      }
      if (exception != null) {
        throw new IOException("Error detected at close", exception);
      }
    }

    @Override
    public synchronized void flush() throws IOException {
      if (stream != null && exception == null) {
        stream.hflush();
      }
    }

    public synchronized void writeEntity(TimelineEntity entity)
        throws IOException {
      if (exception != null) {
        throw new IOException("Error writing entity", exception);
      }
      if (stream != null) {
        objMapper.writeValue(jsonGenerator, entity);
      }
    }

    public synchronized void cancelFlushTimer() {
      flushTimer.cancel();
    }

    private class FlushTimerTask extends TimerTask {
      @Override
      public void run() {
        try {
          if (exception == null) {
            flush();
          }
        } catch (IOException e) {
          exception = e;
        }
      }
    }
  }
}
