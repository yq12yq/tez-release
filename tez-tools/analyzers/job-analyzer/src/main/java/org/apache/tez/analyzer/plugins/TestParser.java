package org.apache.tez.analyzer.plugins;

import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.parser.ATSFileParser;
import org.apache.tez.history.parser.datamodel.Container;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.util.ajax.JSON;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

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
public class TestParser {

  //DagId --> QueryID
  static Map<String, String> queryToDAGMapping = new HashMap<String, String>();

  public static UTF8String fromBytes(byte[] bytes, int offset, int numBytes) {
    if (bytes != null) {
      return new UTF8String(bytes);
    } else {
      return null;
    }
  }

  static class UTF8String {
    byte[] b;

    UTF8String(byte[] b) {
      this.b = b;
    }

    @Override public String toString() {
      //System.out.println(b.hashCode());
      return new String(b);
    }
  }

  public static void populateMapping(Path filePath) throws IOException {
    for (String line : Files.readAllLines(filePath, Charset.forName("UTF-8"))) {
      String qId = line.split("\t")[0];
      String dagId = line.split("\t")[1];
      queryToDAGMapping.put(dagId, qId);
    }
    System.out.println("Done mapping!!! " + queryToDAGMapping);
  }

  public static void iterateFile(File baseFile) throws IOException {
    if (baseFile.isDirectory()) {
      File[] files = baseFile.listFiles();
      for (File f : files) {
        if (f.isDirectory()) {
          iterateFile(f);
        } else {
          if (f.getName().endsWith(".zip")) {
            ATSFileParser parser = null;
            try {
              parser = new ATSFileParser(f);
              String dagName = f.getName().replace(".zip", "");
              DagInfo dagInfo = parser.getDAGData(dagName);
              System.out.println(dagInfo.getVersionInfo());
              TreeSet<TaskAttemptInfo> attempts = new TreeSet<TaskAttemptInfo>
                  (new Comparator<TaskAttemptInfo>() {
                    @Override
                    public int compare(TaskAttemptInfo o1, TaskAttemptInfo o2) {
                      return (o1.getStartTimeInterval() < o2
                          .getStartTimeInterval()) ? -1 :
                          ((o1.getStartTimeInterval() == o2
                              .getStartTimeInterval()) ?
                              0 : 1);
                    }
                  });
              for (Container container : dagInfo.getContainerMapping()
                  .keySet()) {
                attempts.addAll(dagInfo.getContainerMapping().get(container)
                    );
              }


              String queryId = queryToDAGMapping.get(dagName);


              System.out.println(
                  queryId + ", " + dagName + ", " +
                  attempts.first().getTezCounters()
                      .findCounter("org.apache.tez.common.counters.TaskCounter",
                  "OUTPUT_RECORDS").getValue());

              System.out.println();
              System.out.println();

              /*
              CriticalPathAnalyzer analyzer = new CriticalPathAnalyzer();
              Configuration conf  = new Configuration();
              String baseCriticalPathDir =
                  "/Users/rbalamohan/Downloads/apple/8_mar/runTezMar8/ats/criticalPath";
              new File(baseCriticalPathDir).mkdirs();

              conf.set("output-dir", baseCriticalPathDir);

              conf.set("tez.critical-path-analyzer.draw-svg", "true");
              analyzer.setConf(conf);
              analyzer.analyze(dagInfo);



              String queryId = queryToDAGMapping.get(dagName);

              Path SVG = new File(baseCriticalPathDir + "/" + dagName + ".svg").toPath();
              Path SVGWithQueryId = new File(baseCriticalPathDir + "/" +
                  queryId + "_" + dagName + ".svg").toPath();
              Files.move(SVG, SVGWithQueryId);

              String sqlFileName = queryId + "_" + dagName + ".sql";
              String queryFile = baseCriticalPathDir + "/" + sqlFileName;
              Files.write(Paths.get(queryFile),
                  StringEscapeUtils.unescapeJava(dagInfo.getCallerContext()
                      .getBlob()).getBytes());

              System.out.println("Complete");
              */

            } catch (TezException e) {
              e.printStackTrace();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      }
    }

  }

  public static void main(String[] args)
      throws TezException, IOException, JSONException {

    /*
    populateMapping(new File
        ("/Users/rbalamohan/Downloads/apple/8_mar/runTezMar8/ats/mapping.txt")
        .toPath());

    iterateFile(new File("/Users/rbalamohan/Downloads/apple/8_mar/runTezMar8"));

    */

    File additionalInfoFile = new File
        ("/Users/rbalamohan/Downloads/apple/hive_query_plan_formatted_ats"
            + "/additionalInfo.json");
    String contents = new String(Files.readAllBytes(additionalInfoFile.toPath
        ()));
    JSONObject additionalInfo = new JSONObject(contents).getJSONObject("additionalInfo");
    JSONObject hive = additionalInfo.getJSONObject("hive");
    JSONArray dag = hive.getJSONArray("dag");
    for(int i=0;i<dag.length();i++) {
      JSONArray entities = dag.getJSONObject(i).getJSONArray("entities");
      for(int j=0;j<entities.length();j++) {
        JSONObject entity = entities.getJSONObject(j);
        String entityName = entity.getString("entity");
        System.out.println("Entity : " + entityName);
        JSONObject otherInfo = entity.optJSONObject("otherinfo");
        JSONObject query = new JSONObject(otherInfo.getString("QUERY"));
        String queryText = query.getString("queryText");
        String queryPlan = query.getString("queryPlan");
        System.out.println(queryText);
        System.out.println(queryPlan);

        JSONObject jsonPlan = new JSONObject(queryPlan).getJSONObject("STAGE PLANS");
        System.out.println(jsonPlan);

        //Stage iterator
        Iterator<String> it = jsonPlan.keys();
        while(it.hasNext()) {
          String key = it.next();
          if (jsonPlan.optJSONObject(key) != null) {
            JSONObject tez = jsonPlan.getJSONObject(key).optJSONObject("Tez");
            if (tez != null) {
              String dagId = tez.optString("DagId:");
              String dagName = tez.optString("DagName:");
              JSONObject vertices = tez.optJSONObject("Vertices:");
              if (vertices != null) {

                Iterator<String> vIt = vertices.keys();
                while (vIt.hasNext()) {
                  String vertexName = vIt.next();
                  System.out.println(vertexName);
                  JSONObject vertexJson = vertices.getJSONObject(vertexName);
                  //get operator tree
                  Iterator<String> optTreeIt = vertexJson.keys();
                  while(optTreeIt.hasNext()) {
                    String operatorName = optTreeIt.next();
                    System.out.println(operatorName);
                    JSONObject operatorJson =
                        vertexJson.optJSONObject(operatorName);
                    if (operatorJson != null) {
                      //Idenitfy and check the oeprator
                      Iterator<String> opIt = operatorJson.keys();
                      while (opIt.hasNext()) {
                        String opName = opIt.next();
                        JSONObject op = operatorJson.getJSONObject(opName);
                        //Now you can get all stats etc
                        System.out.println("Stats: " + op.optString
                            ("Statistics"));
                        System.out.println(opName + ", " + op.optString("Statistics:"));
                      }
                      System.out.println();
                    }
                    System.out.println();
                  }
                }
              }
            }
          }
        }
      }
    }

    System.out.println("here");




    /*
    Configuration conf  = new Configuration();
    conf.set("output-dir", "/Users/rbalamohan/Downloads/");
    conf.set("tez.critical-path-analyzer.draw-svg", "true");
    analyzer.setConf(conf);
    analyzer.analyze(dagInfo);
    System.out.println("Done!!!");

    */
    /*

    Configuration conf  = new Configuration();
    SlowNodeAnalyzer slowNode = new SlowNodeAnalyzer(conf);
    slowNode.analyze(dagInfo);
    ((CSVResult)slowNode.getResult())
        .dumpToFile("/Users/rbalamohan/Downloads"
            + "/dag_1456364467760_0067_1_slowNodes.txt");


    FileWriter writer = new FileWriter
        ("/Users/rbalamohan/Downloads/dag_1456364467760_0067_1.txt");
    writer.write(dagInfo.getVertexMapping().toString());
    writer.append("\n");
    writer.write(dagInfo.getVertexNameIDMapping().toString());
    writer.append("\n");
    System.out.println(dagInfo.getVertexMapping());

    for(VertexInfo v : dagInfo.getVertices()) {

      System.out.println(v.getVertexName() + ", " + v.getVertexId() + ", " +
          v.getTimeTaken());

      writer.write(v.getVertexId()
          + ", name=" + v.getVertexName()
          + ", timeTaken=" + v.getTimeTaken());
      writer.append("\n");

      for(TaskAttemptInfo t : v.getTaskAttempts()) {
        writer.write("\t attempt:"+ t.getShortName()
            + ", node=" + t.getNodeId()
            + ", status=" + t.getStatus()
            + ", time=" + t.getTimeTaken()
            + ", counters=" + t.getTezCounters()
        );
        writer.append("\n");
      }
    }
    writer.close();

*/
    System.out.println();

    /*
    SimpleHistoryParser parser = new SimpleHistoryParser
        (new File("/Users/rbalamohan/Downloads/q50_10TB_cn041.output_dag_1454105149098_0036_1.txt"));
    DagInfo dagInfo = parser.getDAGData("dag_1454105149098_0036_1");
    System.out.println("Got dag : " + dagInfo);

    Configuration conf = new Configuration();
    conf.setInt("tez.task-concurrency-analyzer.time.interval", 100);

    TaskConcurrencyAnalyzer analyzer = new TaskConcurrencyAnalyzer(conf);
    analyzer.analyze(dagInfo);

    CSVResult result = analyzer.getResult();
    //System.out.println(result.toString());

    */

  }
}
