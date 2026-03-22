// 这个文件已经全部加上中文注释
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
package org.apache.hadoop.hdfs.server.balancer;

import static org.apache.hadoop.hdfs.protocol.BlockType.CONTIGUOUS;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.net.NetworkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode.StorageGroup;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.Source;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.Task;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicies;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.VersionInfo;

/**
 * HDFS数据均衡工具，通过移动数据块使集群中各DataNode磁盘使用率达到均衡状态。
 * 当集群新增节点或部分节点磁盘使用率过高时运行，可将高利用率节点上的块移动到低利用率节点，
 * 保持集群数据分布均衡，提升集群读写性能，避免热点问题。
 * 
 * <p>The balancer is a tool that balances disk space usage on an HDFS cluster
 * when some datanodes become full or when new empty nodes join the cluster.
 * The tool is deployed as an application program that can be run by the 
 * cluster administrator on a live HDFS cluster while applications
 * adding and deleting files.
 * 
 * <p>SYNOPSIS
 * <pre>
 * To start:
 *      bin/start-balancer.sh [-threshold {@literal <threshold>}]
 *      Example: bin/ start-balancer.sh 
 *                     start the balancer with a default threshold of 10%
 *               bin/ start-balancer.sh -threshold 5
 *                     start the balancer with a threshold of 5%
 *               bin/ start-balancer.sh -idleiterations 20
 *                     start the balancer with maximum 20 consecutive idle iterations
 *               bin/ start-balancer.sh -idleiterations -1
 *                     run the balancer with default threshold infinitely
 * To stop:
 *      bin/ stop-balancer.sh
 * </pre>
 * 
 * <p>DESCRIPTION
 * <p>The threshold parameter is a fraction in the range of (1%, 100%) with a 
 * default value of 10%. The threshold sets a target for whether the cluster 
 * is balanced. A cluster is balanced if for each datanode, the utilization 
 * of the node (ratio of used space at the node to total capacity of the node) 
 * differs from the utilization of the (ratio of used space in the cluster 
 * to total capacity of the cluster) by no more than the threshold value. 
 * The smaller the threshold, the more balanced a cluster will become. 
 * It takes more time to run the balancer for small threshold values. 
 * Also for a very small threshold the cluster may not be able to reach the 
 * balanced state when applications write and delete files concurrently.
 * 
 * <p>The tool moves blocks from highly utilized datanodes to poorly 
 * utilized datanodes iteratively. In each iteration a datanode moves or 
 * receives no more than the lesser of 10G bytes or the threshold fraction 
 * of its capacity. Each iteration runs no more than 20 minutes.
 * At the end of each iteration, the balancer obtains updated datanodes
 * information from the namenode.
 * 
 * <p>A system property that limits the balancer's use of bandwidth is 
 * defined in the default configuration file:
 * <pre>
 * &lt;property&gt;
 *   &lt;name&gt;dfs.datanode.balance.bandwidthPerSec&lt;/name&gt;
 *   &lt;value&gt;1048576&lt;/value&gt;
 * &lt;description&gt;  Specifies the maximum bandwidth that each datanode
 * can utilize for the balancing purpose in term of the number of bytes 
 * per second.
 * &lt;/description&gt;
 * &lt;/property&gt;
 * </pre>
 * 
 * <p>This property determines the maximum speed at which a block will be 
 * moved from one datanode to another. The default value is 1MB/s. The higher 
 * the bandwidth, the faster a cluster can reach the balanced state, 
 * but with greater competition with application processes. If an 
 * administrator changes the value of this property in the configuration 
 * file, the change is observed when HDFS is next restarted.
 * 
 * <p>MONITERING BALANCER PROGRESS
 * <p>After the balancer is started, an output file name where the balancer 
 * progress will be recorded is printed on the screen.  The administrator 
 * can monitor the running of the balancer by reading the output file. 
 * The output shows the balancer's status iteration by iteration. In each 
 * iteration it prints the starting time, the iteration number, the total 
 * number of bytes that have been moved in the previous iterations, 
 * the total number of bytes that are left to move in order for the cluster 
 * to be balanced, and the number of bytes that are being moved in this 
 * iteration. Normally "Bytes Already Moved" is increasing while "Bytes Left 
 * To Move" is decreasing.
 * 
 * <p>Running multiple instances of the balancer in an HDFS cluster is 
 * prohibited by the tool.
 * 
 * <p>The balancer automatically exits when any of the following five 
 * conditions is satisfied:
 * <ol>
 * <li>The cluster is balanced;
 * <li>No block can be moved;
 * <li>No block has been moved for specified consecutive iterations (5 by default);
 * <li>An IOException occurs while communicating with the namenode;
 * <li>Another balancer is running.
 * </ol>
 * 
 * <p>Upon exit, a balancer returns an exit code and prints one of the 
 * following messages to the output file in corresponding to the above exit 
 * reasons:
 * <ol>
 * <li>The cluster is balanced. Exiting
 * <li>No block can be moved. Exiting...
 * <li>No block has been moved for specified iterations (5 by default). Exiting...
 * <li>Received an IO exception: failure reason. Exiting...
 * <li>Another balancer is running. Exiting...
 * </ol>
 * 
 * <p>The administrator can interrupt the execution of the balancer at any 
 * time by running the command "stop-balancer.sh" on the machine where the 
 * balancer is running.
 */

@InterfaceAudience.Private
/**
 * HDFS均衡器核心实现类，提供均衡器主逻辑，实现了JMX监控接口支持运维监控
 */
public class Balancer implements BalancerMXBean {
  static final Logger LOG = LoggerFactory.getLogger(Balancer.class);
  // 均衡器ID在ZNode上的存储路径
  static final Path BALANCER_ID_PATH = new Path("/system/balancer.id");

  private static final String USAGE = "Usage: hdfs balancer"
      + "\n\t[-policy <policy>]\tthe balancing policy: "
      + BalancingPolicy.Node.INSTANCE.getName() + " or "
      + BalancingPolicy.Pool.INSTANCE.getName()
      + "\n\t[-threshold <threshold>]\tPercentage of disk capacity"
      + "\n\t[-exclude [-f <hosts-file> | <comma-separated list of hosts>]]"
      + "\tExcludes the specified datanodes."
      + "\n\t[-include [-f <hosts-file> | <comma-separated list of hosts>]]"
      + "\tIncludes only the specified datanodes."
      + "\n\t[-source [-f <hosts-file> | <comma-separated list of hosts>]]"
      + "\tPick only the specified datanodes as source nodes."
      + "\n\t[-excludeSource [-f <hosts-file> | <comma-separated list of hosts>]]"
      + "\tExcludes the specified datanodes to be selected as a source."
      + "\n\t[-target [-f <hosts-file> | <comma-separated list of hosts>]]"
      + "\tPick only the specified datanodes as target nodes."
      + "\n\t[-excludeTarget [-f <hosts-file> | <comma-separated list of hosts>]]"
      + "\tExcludes the specified datanodes from being selected as a target."
      + "\n\t[-blockpools <comma-separated list of blockpool ids>]"
      + "\tThe balancer will only run on blockpools included in this list."
      + "\n\t[-idleiterations <idleiterations>]"
      + "\tNumber of consecutive idle iterations (-1 for Infinite) before "
      + "exit."
      + "\n\t[-runDuringUpgrade]"
      + "\tWhether to run the balancer during an ongoing HDFS upgrade."
      + "This is usually not desired since it will not affect used space "
      + "on over-utilized machines."
      + "\n\t[-asService]\tRun as a long running service."
      + "\n\t[-sortTopNodes]"
      + "\tSort datanodes based on the utilization so "
      + "that highly utilized datanodes get scheduled first."
      + "\n\t[-limitOverUtilizedNum <specified maximum number of overUtilized datanodes>]"
      + "\tLimit the maximum number of overUtilized datanodes."
      + "\n\t[-hotBlockTimeInterval]\tprefer to move cold blocks.";

  @VisibleForTesting
  private static volatile boolean serviceRunning = false;

  private static final AtomicInteger EXCEPTIONS_SINCE_LAST_BALANCE =
      new AtomicInteger(0);
  private static final AtomicInteger
      FAILED_TIMES_SINCE_LAST_SUCCESSFUL_BALANCE = new AtomicInteger(0);

  private final Dispatcher dispatcher;
  private final NameNodeConnector nnc;
  private final BalancingPolicy policy;
  private final Set<String> sourceNodes;
  private final Set<String> excludedSourceNodes;
  private final Set<String> targetNodes;
  private final Set<String> excludedTargetNodes;
  private final boolean runDuringUpgrade;
  private final double threshold;
  private final long maxSizeToMove;
  private final long defaultBlockSize;
  private final boolean sortTopNodes;
  private final int limitOverUtilizedNum;
  private final BalancerMetrics metrics;
  private ObjectName balancerInfoBeanName;

  // 各类存储分组分类列表
  private final Collection<Source> overUtilized = new LinkedList<Source>();
  private final Collection<Source> aboveAvgUtilized = new LinkedList<Source>();
  private final Collection<StorageGroup> belowAvgUtilized
      = new LinkedList<StorageGroup>();
  private final Collection<StorageGroup> underUtilized
      = new LinkedList<StorageGroup>();

  /**
   * 检查当前NameNode使用的块放置策略是否与均衡器兼容，仅支持默认块放置策略
   * @param conf Hadoop配置对象
   * @throws UnsupportedActionException 如果不兼容则抛出异常
   */
  /* Check that this Balancer is compatible with the Block Placement Policy
   * used by the Namenode.
   */
  private static void checkReplicationPolicyCompatibility(Configuration conf
      ) throws UnsupportedActionException {
    BlockPlacementPolicies placementPolicies =
        new BlockPlacementPolicies(conf, null, NetworkTopology.getInstance(conf), null);
    if (!(placementPolicies.getPolicy(CONTIGUOUS) instanceof
        BlockPlacementPolicyDefault)) {
      throw new UnsupportedActionException(
          "Balancer without BlockPlacementPolicyDefault");
    }
  }

  /**
   * 从配置中读取长整型参数，进行参数合法性校验，要求必须为正整数
   * @param conf 配置对象
   * @param key 参数键
   * @param defaultValue 默认值
   * @return 读取到的合法参数值
   */
  static long getLong(Configuration conf, String key, long defaultValue) {
    final long v = conf.getLong(key, defaultValue);
    LOG.info(key + " = " + v + " (default=" + defaultValue + ")");
    if (v <= 0) {
      throw new HadoopIllegalArgumentException(key + " = " + v  + " <= " + 0);
    }
    return v;
  }

  /**
   * 从配置中读取字节数参数（支持单位后缀），进行参数合法性校验，要求必须为正
   * @param conf 配置对象
   * @param key 参数键
   * @param defaultValue 默认值
   * @return 读取到的合法字节数
   */
  static long getLongBytes(Configuration conf, String key, long defaultValue) {
    final long v = conf.getLongBytes(key, defaultValue);
    LOG.info(key + " = " + v + " (default=" + defaultValue + ")");
    if (v <= 0) {
      throw new HadoopIllegalArgumentException(key + " = " + v  + " <= " + 0);
    }
    return v;
  }

  /**
   * 从配置中读取整型参数，进行参数合法性校验，要求必须为正整数
   * @param conf 配置对象
   * @param key 参数键
   * @param defaultValue 默认值
   * @return 读取到的合法参数值
   */
  static int getInt(Configuration conf, String key, int defaultValue) {
    final int v = conf.getInt(key, defaultValue);
    LOG.info(key + " = " + v + " (default=" + defaultValue + ")");
    if (v <= 0) {
      throw new HadoopIllegalArgumentException(key + " = " + v  + " <= " + 0);
    }
    return v;
  }

  /**
   * 获取上次均衡以来累计异常次数，用于长期服务模式错误重试计数
   * @return 异常次数
   */
  static int getExceptionsSinceLastBalance() {
    return EXCEPTIONS_SINCE_LAST_BALANCE.get();
  }

  /**
   * 获取上次成功均衡以来累计失败次数
   * @return 失败次数
   */
  static int getFailedTimesSinceLastSuccessfulBalance() {
    return FAILED_TIMES_SINCE_LAST_SUCCESSFUL_BALANCE.get();
  }

  /**
   * 构造均衡器实例，初始化配置、连接NameNode、创建任务分发器并注册JMX监控
   * @param theblockpool NameNode连接器，用于和NameNode通信获取数据节点信息
   * @param p 均衡器运行参数
   * @param conf Hadoop配置对象
   */
  Balancer(NameNodeConnector theblockpool, BalancerParameters p,
      Configuration conf) {
    // 读取并校验NameNode侧均衡相关配置
    getInt(conf, DFSConfigKeys.DFS_NAMENODE_GETBLOCKS_MAX_QPS_KEY,
        DFSConfigKeys.DFS_NAMENODE_GETBLOCKS_MAX_QPS_DEFAULT);
    final long movedWinWidth = getLong(conf,
        DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY,
        DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_DEFAULT);
    final int moverThreads =