// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer.datamodel;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.hadoop.util.Preconditions;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ClusterConnector;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.NodePlan;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.Planner;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.PlannerFactory;
import org.apache.hadoop.hdfs.web.JsonUtil;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 文件级注释：磁盘均衡器的HDFS集群信息模型，负责管理集群中所有DataNode节点信息，处理包含/排除节点规则，并生成磁盘均衡迁移计划
 * 
 * DiskBalancerCluster represents the nodes that we are working against.
 * <p>
 * Please Note :
 * Semantics of inclusionList and exclusionLists.
 * <p>
 * If a non-empty inclusionList is specified then the diskBalancer assumes that
 * the user is only interested in processing that list of nodes. This node list
 * is checked against the exclusionList and only the nodes in inclusionList but
 * not in exclusionList is processed.
 * <p>
 * if inclusionList is empty, then we assume that all live nodes in the nodes is
 * to be processed by diskBalancer. In that case diskBalancer will avoid any
 * nodes specified in the exclusionList but will process all nodes in the
 * cluster.
 * <p>
 * In other words, an empty inclusionList is means all the nodes otherwise
 * only a given list is processed and ExclusionList is always honored.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DiskBalancerCluster {

  private static final Logger LOG =
      LoggerFactory.getLogger(DiskBalancerCluster.class);
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(DiskBalancerCluster.class);
  private final Set<String> exclusionList;
  private final Set<String> inclusionList;
  private ClusterConnector clusterConnector;
  private List<DiskBalancerDataNode> nodes;
  private String outputpath;

  @JsonIgnore
  private List<DiskBalancerDataNode> nodesToProcess;
  @JsonIgnore
  private final Map<String, DiskBalancerDataNode> ipList;
  @JsonIgnore
  private final Map<String, DiskBalancerDataNode> hostNames;
  @JsonIgnore
  private final Map<String, DiskBalancerDataNode>  hostUUID;

  private float threshold;

  /**
   * 空构造器，Jackson JSON反序列化需要
   */
  public DiskBalancerCluster() {
    nodes = new LinkedList<>();
    exclusionList = new TreeSet<>();
    inclusionList = new TreeSet<>();
    ipList = new HashMap<>();
    hostNames = new HashMap<>();
    hostUUID = new HashMap<>();
  }

  /**
   * 基于指定连接器构造磁盘均衡集群对象
   *
   * @param connector 集群连接器，用于从HDFS获取节点信息
   * @throws IOException
   */
  public DiskBalancerCluster(ClusterConnector connector) throws IOException {
    this();
    Preconditions.checkNotNull(connector);
    clusterConnector = connector;
  }

  /**
   * 从JSON字符串反序列化为DiskBalancerCluster对象
   *
   * @param json 输入JSON字符串
   * @return 反序列化得到的DiskBalancerCluster对象
   * @throws IOException
   */
  public static DiskBalancerCluster parseJson(String json) throws IOException {
    return READER.readValue(json);
  }

  /**
   * 从集群连接器读取所有DataNode节点信息，建立各类索引映射，供后续均衡计算使用
   */
  public void readClusterInfo() throws Exception {
    Preconditions.checkNotNull(clusterConnector);
    LOG.debug("Using connector : {}" , clusterConnector.getConnectorInfo());
    // 获取所有DataNode节点信息
    nodes = clusterConnector.getNodes();
    // 按IP、主机名、UUID分别建立索引，方便后续查询节点
    for(DiskBalancerDataNode node : nodes) {

      if(node.getDataNodeIP()!= null && !node.getDataNodeIP().isEmpty()) {
        ipList.put(node.getDataNodeIP(), node);
      }

      if(node.getDataNodeName() != null && !node.getDataNodeName().isEmpty()) {
        // TODO : should we support Internationalized Domain Names ?
        // Disk balancer assumes that host names are ascii. If not
        // end user can always balance the node via IP address or DataNode UUID.
        hostNames.put(node.getDataNodeName().toLowerCase(Locale.US), node);
      }

      if(node.getDataNodeUUID() != null && !node.getDataNodeUUID().isEmpty()) {
        hostUUID.put(node.getDataNodeUUID(), node);
      }
    }
  }

  /**
   * 获取集群中所有DataNode节点列表
   *
   * @return 所有DataNode节点列表
   */
  public List<DiskBalancerDataNode> getNodes() {
    return nodes;
  }

  /**
   * 设置集群节点列表
   *
   * @param clusterNodes 节点列表
   */
  public void setNodes(List<DiskBalancerDataNode> clusterNodes) {
    this.nodes = clusterNodes;
  }

  /**
   * 获取不参与均衡的节点排除列表
   *
   * @return 排除节点集合
   */
  public Set<String> getExclusionList() {
    return exclusionList;
  }

  /**
   * 设置不参与均衡的节点排除列表
   *
   * @param excludedNodes 排除节点集合
   */
  public void setExclusionList(Set<String> excludedNodes) {
    this.exclusionList.addAll(excludedNodes);
  }

  /**
   * 获取磁盘均衡允许的阈值，该值表示磁盘间使用率允许的最大偏差百分比，超过阈值才会进行均衡
   *
   * @return 偏差阈值百分比
   */
  public float getThreshold() {
    return threshold;
  }

  /**
   * 设置磁盘均衡允许的偏差阈值百分比
   *
   * @param thresholdPercent 偏差阈值百分比，范围0-100
   */
  public void setThreshold(float thresholdPercent) {
    Preconditions.checkState((thresholdPercent >= 0.0f) &&
        (thresholdPercent <= 100.0f), "A percentage value expected.");
    this.threshold = thresholdPercent;
  }

  /**
   * 获取需要参与均衡的节点包含列表
   *
   * @return 包含节点集合
   */
  public Set<String> getInclusionList() {
    return inclusionList;
  }

  /**
   * 设置需要参与均衡的节点包含列表
   *
   * @param includeNodes 包含节点集合
   */
  public void setInclusionList(Set<String> includeNodes) {
    this.inclusionList.addAll(includeNodes);
  }

  /**
   * 将当前集群对象序列化为JSON字符串
   *
   * @return 序列化后的JSON字符串
   * @throws IOException
   */
  public String toJson() throws IOException {
    return JsonUtil.toJsonString(this);
  }

  /**
   * 获取实际需要进行磁盘均衡处理的节点列表
   *
   * @return 需要处理的节点列表
   */
  @JsonIgnore
  public List<DiskBalancerDataNode> getNodesToProcess() {
    return nodesToProcess;
  }

  /**
   * 设置需要进行磁盘均衡处理的节点列表
   *
   * @param dnNodesToProcess 需要处理的节点列表
   */
  @JsonIgnore
  public void setNodesToProcess(List<DiskBalancerDataNode> dnNodesToProcess) {
    this.nodesToProcess = dnNodesToProcess;
  }

  /**
   * 获取输出结果路径
   */
  public String getOutput() {
    return outputpath;
  }

  /**
   * 设置本次均衡运行的输出路径
   *
   * @param output 输出路径字符串
   */
  public void setOutput(String output) {
    this.outputpath = output;
  }

  /**
   * 将当前集群节点信息快照写入指定目录的JSON文件
   *
   * @param snapShotName 快照文件名
   */
  public void createSnapshot(String snapShotName) throws IOException {
    String json = this.toJson();
    File outFile = new File(getOutput() + "/" + snapShotName);
    FileUtils.writeStringToFile(outFile, json, StandardCharsets.UTF_8);
  }

  /**
   * 并行为所有待处理节点生成磁盘数据迁移均衡计划
   * <p>
   * This function creates a thread pool and executes a planner on each node
   * that we are supposed to plan for. Each of these planners return a NodePlan
   * that we can persist or schedule for execution with a diskBalancer
   * Executor.
   *
   * @param thresholdPercent 允许的磁盘使用率偏差阈值百分比
   * @return 所有节点的均衡计划列表
   */
  public List<NodePlan> computePlan(double thresholdPercent) {
    List<NodePlan> planList = new LinkedList<>();

    if (nodesToProcess == null) {
      LOG.warn("Nodes to process is null. No nodes processed.");
      return planList;
    }
    // 根据待处理节点数量计算合适的线程池大小
    int poolSize = computePoolSize(nodesToProcess.size());
    // 创建固定大小线程池并行生成计划
    ExecutorService executorService = Executors.newFixedThreadPool(poolSize);
    List<Future<NodePlan>> futureList = new LinkedList<>();
    // 为每个待处理节点提交计划生成任务
    for (int x = 0; x < nodesToProcess.size(); x++) {
      final DiskBalancerDataNode node = nodesToProcess.get(x);
      final Planner planner = PlannerFactory
          .getPlanner(PlannerFactory.GREEDY_PLANNER, node,
              thresholdPercent);
      futureList.add(executorService.submit(new Callable<NodePlan>() {
        @Override
        public NodePlan call() throws Exception {
          assert planner != null;
          return planner.plan(node);
        }
      }));
    }
    // 收集所有节点的计划结果
    for (Future<NodePlan> f : futureList) {
      try {
        planList.add(f.get());
      } catch (InterruptedException e) {
        LOG.error("Compute Node plan was cancelled or interrupted : ", e);
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        LOG.error("Unable to compute plan : ", e);
      }
    }
    // 关闭线程池
    executorService.shutdown();
    return planList;
  }

  /**
   * 根据待处理节点数量，按启发式规则计算并行计划生成的线程池大小
   * 规则：每100个节点对应1个线程，最小为节点数，最大不超过100，结果向上取整为10的倍数
   *
   * @param nodeCount 待处理节点总数
   * @return 计算得到的线程池大小
   */
  private int computePoolSize(int nodeCount) {

    if (nodeCount < 10) {
      return nodeCount;
    }

    int threadRatio = nodeCount / 100;
    int modValue = threadRatio % 10;

    if (((10 - modValue) + threadRatio) > 100) {
      return 100;
    } else {
      return (10 - modValue) + threadRatio;
    }
  }

  /**
   * 根据UUID查询对应的DataNode节点
   * @param uuid DataNode的UUID
   * @return 对应的DiskBalancerDataNode对象
   */
  public DiskBalancerDataNode getNodeByUUID(String uuid) {
    return hostUUID.get(uuid);
  }

  /**
   * 根据IP地址查询对应的DataNode节点
   * @param ipAddresss DataNode的IP地址
   * @return 对应的DiskBalancerDataNode对象
   */
  public DiskBalancerDataNode getNodeByIPAddress(String ipAddresss) {
    return ipList.get(ipAddresss);
  }

  /**
   * 根据主机名查询对应的DataNode节点
   * @param hostName DataNode的主机名
   * @return 对应的DiskBalancerDataNode对象
   */
  public DiskBalancerDataNode getNodeByName(String hostName) {
    return hostNames.get(hostName.toLowerCase(Locale.US));
  }
}