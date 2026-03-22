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

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.apache.hadoop.util.StringInterner;

/**
 * 存储MapReduce集群当前状态的信息类，供客户端获取集群概览使用
 * 
 * <p><code>ClusterStatus</code> 向客户端提供以下信息:
 * <ol>
 *   <li>
 *   集群规模（节点数量）
 *   </li>
 *   <li>
 *   所有TaskTracker节点名称
 *   </li>
 *   <li>
 *   集群任务总容量
 *   </li>
 *   <li>
 *   当前正在运行的Map和Reduce任务数量
 *   </li>
 *   <li>
 *   JobTracker运行状态
 *   </li>
 *   <li>
 *   黑名单TaskTracker的详细信息
 *   </li>
 * </ol>
 * 
 * <p>客户端可通过 {@link JobClient#getClusterStatus()} 获取最新集群状态</p>
 * 
 * @see JobClient
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ClusterStatus implements Writable {
  /**
   * 封装被列入黑名单的TaskTracker节点信息
   *  
   * 包含TaskTracker节点名称、被拉黑原因、详细描述报告。
   * toString方法输出空格分隔格式，方便后续解析处理。
   */
  public static class BlackListInfo implements Writable {

    private String trackerName;

    private String reasonForBlackListing;
    
    private String blackListReport;
    
    BlackListInfo() {
    }
    

    /**
     * 获取被拉黑TaskTracker的节点名称
     * 
     * @return TaskTracker节点名称
     */
    public String getTrackerName() {
      return trackerName;
    }

    /**
     * 获取TaskTracker被拉黑的原因
     * 
     * @return 拉黑原因
     */
    public String getReasonForBlackListing() {
      return reasonForBlackListing;
    }

    /**
     * 设置被拉黑TaskTracker的节点名称
     * 
     * @param trackerName TaskTracker节点名称
     */
    void setTrackerName(String trackerName) {
      this.trackerName = trackerName;
    }

    /**
     * 设置TaskTracker被拉黑的原因
     * 
     * @param reasonForBlackListing 拉黑原因
     */
    void setReasonForBlackListing(String reasonForBlackListing) {
      this.reasonForBlackListing = reasonForBlackListing;
    }

    /**
     * 获取TaskTracker被拉黑的详细描述报告
     * 
     * @return 拉黑原因详细报告
     */
    public String getBlackListReport() {
      return blackListReport;
    }

    /**
     * 设置TaskTracker被拉黑的详细描述报告
     * @param blackListReport 拉黑原因详细报告
     */
    void setBlackListReport(String blackListReport) {
      this.blackListReport = blackListReport;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      // 从输入流反序列化黑名单信息，使用弱引用字符串驻留节省内存
      trackerName = StringInterner.weakIntern(Text.readString(in));
      reasonForBlackListing = StringInterner.weakIntern(Text.readString(in));
      blackListReport = StringInterner.weakIntern(Text.readString(in));
    }

    @Override
    public void write(DataOutput out) throws IOException {
      // 序列化黑名单信息到输出流
      Text.writeString(out, trackerName);
      Text.writeString(out, reasonForBlackListing);
      Text.writeString(out, blackListReport);
    }

    @Override
    /**
     * 以空格分隔格式输出黑名单节点信息，将报告中的换行替换为冒号方便解析
     * @return 格式化的黑名单节点信息字符串
     */
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(trackerName);
      sb.append("\t");
      sb.append(reasonForBlackListing);
      sb.append("\t");
      sb.append(blackListReport.replace("\n", ":"));
      return sb.toString();
    }

    @Override
    public int hashCode() {
      int result = trackerName != null ? trackerName.hashCode() : 0;
      result = 31 * result + (reasonForBlackListing != null ?
          reasonForBlackListing.hashCode() : 0);
      result = 31 * result + (blackListReport != null ?
          blackListReport.hashCode() : 0);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final BlackListInfo that = (BlackListInfo) obj;
      if (trackerName == null ? that.trackerName != null :
          !trackerName.equals(that.trackerName)) {
        return false;
      }
      if (reasonForBlackListing == null ? that.reasonForBlackListing != null :
          !reasonForBlackListing.equals(that.reasonForBlackListing)) {
        return false;
      }
      if (blackListReport == null ? that.blackListReport != null :
          !blackListReport.equals(that.blackListReport)) {
        return false;
      }
      return true;
    }
  }
  
  public static final long UNINITIALIZED_MEMORY_VALUE = -1;
  
  private int numActiveTrackers;
  private Collection<String> activeTrackers = new ArrayList<String>();
  private int numBlacklistedTrackers;
  private int numExcludedNodes;
  private long ttExpiryInterval;
  private int map_tasks;
  private int reduce_tasks;
  private int max_map_tasks;
  private int max_reduce_tasks;
  private JobTrackerStatus status;
  private Collection<BlackListInfo> blacklistedTrackersInfo =
    new ArrayList<BlackListInfo>();
  private int grayListedTrackers;

  ClusterStatus() {}
  
  /**
   * 构造集群状态对象
   * 
   * @param trackers 集群中TaskTracker总数量
   * @param blacklists 集群中黑名单TaskTracker数量
   * @param ttExpiryInterval TaskTracker超时判断时间间隔
   * @param maps 当前正在运行的Map任务数量
   * @param reduces 当前正在运行的Reduce任务数量
   * @param maxMaps 集群最大可运行Map任务总数
   * @param maxReduces 集群最大可运行Reduce任务总数
   * @param status JobTracker的运行状态
   */
  ClusterStatus(int trackers, int blacklists, long ttExpiryInterval, 
                int maps, int reduces,
                int maxMaps, int maxReduces, JobTrackerStatus status) {
    this(trackers, blacklists, ttExpiryInterval, maps, reduces, maxMaps, 
         maxReduces, status, 0);
  }

  /**
   * 构造集群状态对象
   * 
   * @param trackers 集群中TaskTracker总数量
   * @param blacklists 集群中黑名单TaskTracker数量
   * @param ttExpiryInterval TaskTracker超时判断时间间隔
   * @param maps 当前正在运行的Map任务数量
   * @param reduces 当前正在运行的Reduce任务数量
   * @param maxMaps 集群最大可运行Map任务总数
   * @param maxReduces 集群最大可运行Reduce任务总数
   * @param status JobTracker的运行状态
   * @param numDecommissionedNodes 已下线节点数量
   */
  ClusterStatus(int trackers, int blacklists, long ttExpiryInterval, int maps,
      int reduces, int maxMaps, int maxReduces, JobTrackerStatus status,
      int numDecommissionedNodes) {
    this(trackers, blacklists, ttExpiryInterval, maps, reduces, maxMaps,
      maxReduces, status, numDecommissionedNodes, 0);
  }

  /**
   * 构造集群状态对象
   * 
   * @param trackers 集群中TaskTracker总数量
   * @param blacklists 集群中黑名单TaskTracker数量
   * @param ttExpiryInterval TaskTracker超时判断时间间隔
   * @param maps 当前正在运行的Map任务数量
   * @param reduces 当前正在运行的Reduce任务数量
   * @param maxMaps 集群最大可运行Map任务总数
   * @param maxReduces 集群最大可运行Reduce任务总数
   * @param status JobTracker的运行状态
   * @param numDecommissionedNodes 已下线节点数量
   * @param numGrayListedTrackers 灰名单节点数量
   */
  ClusterStatus(int trackers, int blacklists, long ttExpiryInterval, int maps,
      int reduces, int maxMaps, int maxReduces, JobTrackerStatus status,
      int numDecommissionedNodes, int numGrayListedTrackers) {
    numActiveTrackers = trackers;
    numBlacklistedTrackers = blacklists;
    this.numExcludedNodes = numDecommissionedNodes;
    this.ttExpiryInterval = ttExpiryInterval;
    map_tasks = maps;
    reduce_tasks = reduces;
    max_map_tasks = maxMaps;
    max_reduce_tasks = maxReduces;
    this.status = status;
    this.grayListedTrackers = numGrayListedTrackers;
  }

  /**
   * 构造包含详细节点信息的集群状态对象
   * 
   * @param activeTrackers 集群中活跃TaskTracker节点名称集合
   * @param blacklistedTrackers 集群中黑名单TaskTracker信息集合
   * @param ttExpiryInterval TaskTracker超时判断时间间隔
   * @param maps 当前正在运行的Map任务数量
   * @param reduces 当前正在运行的Reduce任务数量
   * @param maxMaps 集群最大可运行Map任务总数
   * @param maxReduces 集群最大可运行Reduce任务总数
   * @param status JobTracker的运行状态
   */
  ClusterStatus(Collection<String> activeTrackers, 
      Collection<BlackListInfo> blacklistedTrackers,
      long ttExpiryInterval,
      int maps, int reduces, int maxMaps, int maxReduces, 
      JobTrackerStatus status) {
    this(activeTrackers, blacklistedTrackers, ttExpiryInterval, maps, reduces, 
         maxMaps, maxReduces, status, 0);
  }


  /**
   * 构造包含详细节点信息的集群状态对象
   * 
   * @param activeTrackers 集群中活跃TaskTracker节点名称集合
   * @param blackListedTrackerInfo 集群中黑名单TaskTracker信息集合
   * @param ttExpiryInterval TaskTracker超时判断时间间隔
   * @param maps 当前正在运行的Map任务数量
   * @param reduces 当前正在运行的Reduce任务数量
   * @param maxMaps 集群最大可运行Map任务总数
   * @param maxReduces 集群最大可运行Reduce任务总数
   * @param status JobTracker的运行状态
   * @param numDecommissionNodes 已下线节点数量
   */
  ClusterStatus(Collection<String> activeTrackers,
      Collection<BlackListInfo> blackListedTrackerInfo, long ttExpiryInterval,
      int maps, int reduces, int maxMaps, int maxReduces,
      JobTrackerStatus status, int numDecommissionNodes) {
    this(activeTrackers.size(), blackListedTrackerInfo.size(),
        ttExpiryInterval, maps, reduces, maxMaps, maxReduces, status,
        numDecommissionNodes);
    this.activeTrackers = activeTrackers;
    this.blacklistedTrackersInfo = blackListedTrackerInfo;
  }

  /**
   * 获取集群中活跃TaskTracker的总数量
   * 
   * @return 活跃TaskTracker数量
   */
  public int getTaskTrackers() {
    return numActiveTrackers;
  }
  
  /**
   * 获取集群中所有活跃TaskTracker的名称集合
   * 
   * @return 活跃TaskTracker名称集合
   */
  public Collection<String> getActiveTrackerNames() {
    return activeTrackers;
  }

  /**
   * 获取集群中所有黑名单TaskTracker的名称集合
   * 
   * @return 黑名单TaskTracker名称集合
   */
  public Collection<String> getBlacklistedTrackerNames() {
    ArrayList<String> blacklistedTrackers = new ArrayList<String>();
    for(BlackListInfo bi : blacklistedTrackersInfo) {
      blacklistedTrackers.add(bi.getTrackerName());
    }
    return blacklistedTrackers;
  }

  /**
   * 获取集群中所有灰名单TaskTracker的名称集合
   *
   * M/R 2.x版本已不再支持灰名单机制，此方法仅为兼容旧版M/R 1.x应用保留
   *
   * @return 返回空集合
   */
  @Deprecated
  public Collection<String> getGraylistedTrackerNames() {
    return Collections.emptySet();
  }

  /**
   * 获取集群中灰名单TaskTracker的总数量
   *
   * M/R 2.x版本已不再支持灰名单机制，此方法仅为兼容旧版M/R 1.x应用保留
   *
   * @return 返回0
   */
  @Deprecated
  public int getGraylistedTrackers() {
    return grayListedTrackers;
  }

  /**
   * 获取集群中黑名单TaskTracker的总数量
   * 
   * @return 黑名单TaskTracker数量
   */
  public int getBlacklistedTrackers() {
    return numBlacklistedTrackers;
  }
  
  /**
   * 获取集群中被排除（下线）节点的总数量
   * @return 被排除节点数量
   */
  public int getNumExcludedNodes() {
    return numExcludedNodes;
  }
  
  /**
   * 获取TaskTracker超时判断时间间隔
   * @return 超时时间间隔，单位毫秒
   */
  public long getTTExpiryInterval() {
    return ttExpiryInterval;
  }
  
  /**
   * 获取当前集群中正在运行的Map任务数量
   * 
   * @return 当前运行Map任务数
   */
  public int getMapTasks() {
    return map_tasks;
  }
  
  /**
   * 获取当前集群中正在运行的Reduce任务数量
   * 
   * @return 当前运行Reduce任务数
   */
  public int getReduceTasks() {
    return reduce_tasks;
  }
  
  /**
   * 获取集群最大可同时运行Map任务总数
   * 
   * @return 集群Map任务总容量
   */
  public int getMaxMapTasks() {
    return max_map_tasks;
  }

  /**
   * 获取集群最大可同时运行Reduce任务总数
   * 
   * @return 集群Reduce任务总容量
   */
  public int getMaxReduceTasks() {
    return max_reduce_tasks;
  }
  
  /**
   * 获取JobTracker的运行状态
   * 
   * @return JobTracker运行状态
   */
  public JobTrackerStatus getJobTrackerStatus() {
    return status;
  }
  
  /**
   * 已废弃，返回未初始化内存标记值-1
   */
  @Deprecated
  public long getMaxMemory() {
    return UNINITIALIZED_MEMORY_VALUE;
  }
  
  /**
   * 已废弃，返回未初始化内存标记值-1
   */
  @Deprecated
  public long getUsedMemory() {
    return UNINITIALIZED_MEMORY_VALUE;
  }

  /**
   * 获取所有黑名单TaskTracker的详细信息集合
   * 
   * @return 黑名单TaskTracker信息对象集合
   */
  public Collection<BlackListInfo> getBlackListedTrackersInfo() {
    return blacklistedTrackersInfo;
  }

  /**
   * 获取JobTracker状态（兼容旧版API）
   *
   * M/R 2.x已不再使用该状态，此方法仅为兼容旧版M/R 1.x应用保留
   *
   * @return 始终返回RUNNING状态
   */
  @Deprecated
  public JobTracker.State getJobTrackerState() {
    return JobTracker.State.RUNNING;
  }

  /**
   * 序列化集群状态对象到输出流
   * @param out 输出流
   * @throws IOException 序列化过程IO异常
   */
  public void write(DataOutput out) throws IOException {
    // 序列化活跃节点信息