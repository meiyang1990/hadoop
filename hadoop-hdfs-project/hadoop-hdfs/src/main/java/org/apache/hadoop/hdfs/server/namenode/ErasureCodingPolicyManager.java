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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyState;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * 文件：hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/ErasureCodingPolicyManager.java
 * 所属模块：HDFS NameNode 核心服务
 * 核心职责：管理HDFS系统中所有纠删码策略，包括内置系统策略和用户自定义策略，
 *          负责策略的加载、启用、禁用、删除，并同步持久化状态到NameNode镜像中。
 *          被FSNamesystem实例化，是NameNode中纠删码功能的核心管理组件。
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public final class ErasureCodingPolicyManager {

  public static Logger LOG = LoggerFactory.getLogger(
      ErasureCodingPolicyManager.class);
  private int maxCellSize =
      DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_MAX_CELLSIZE_DEFAULT;

  private boolean userDefinedAllowed =
      DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_USERPOLICIES_ALLOWED_KEY_DEFAULT;

  // 条带化纠删码文件支持的存储策略列表
  private static final byte[] SUITABLE_STORAGE_POLICIES_FOR_EC_STRIPED_MODE =
      new byte[]{
          HdfsConstants.HOT_STORAGE_POLICY_ID,
          HdfsConstants.COLD_STORAGE_POLICY_ID,
          HdfsConstants.ALLSSD_STORAGE_POLICY_ID};

  /**
   * 按名称排序存储所有策略（包含内置、用户自定义、已移除策略），用于快速查询
   */
  private Map<String, ErasureCodingPolicyInfo> policiesByName;

  /**
   * 按ID排序存储所有策略（包含内置、用户自定义、已移除策略），用于快速查询
   */
  private Map<Byte, ErasureCodingPolicyInfo> policiesByID;

  /**
   * 预缓存所有策略数组，优化全量查询性能
   */
  private ErasureCodingPolicyInfo[] allPolicies;

  /**
   * 存储将持久化到fsimage中的所有策略及其状态
   * 与所有策略的区别：仅在启动时启用的默认策略，在持久化列表和fsimage中会标记为禁用
   */
  private Map<Byte, ErasureCodingPolicyInfo> allPersistedPolicies;

  /**
   * 按名称存储所有已启用策略（包含内置和用户自定义策略），用于快速查询
   */
  private Map<String, ErasureCodingPolicy> enabledPoliciesByName;
  /**
   * 预缓存所有已启用策略数组，优化全量查询性能
   */
  private ErasureCodingPolicy[] enabledPolicies;

  private String defaultPolicyName;

  private volatile static ErasureCodingPolicyManager instance = null;

  /**
   * 获取ErasureCodingPolicyManager单例实例
   * @return 单例对象
   */
  public static ErasureCodingPolicyManager getInstance() {
    if (instance == null) {
      instance = new ErasureCodingPolicyManager();
    }
    return instance;
  }

  private ErasureCodingPolicyManager() {}

  /**
   * 初始化纠删码策略管理器，加载系统内置策略，从配置中读取参数并启用默认策略
   * @param conf Hadoop配置对象
   * @throws IOException 初始化失败时抛出异常
   */
  public void init(Configuration conf) throws IOException {
    this.policiesByName = new TreeMap<>();
    this.policiesByID = new TreeMap<>();
    this.enabledPoliciesByName = new TreeMap<>();
    this.allPersistedPolicies = new TreeMap<>();

    /**
     * TODO: 从fsImage加载用户自定义纠删码策略 HDFS-7859
     * 在NameNode启动阶段一次性从镜像和编辑日志加载持久化策略，可在此方法或单独方法中完成
     */

    /*
     * 将所有系统内置策略添加到策略映射表
     */
    for (ErasureCodingPolicy policy :
        SystemErasureCodingPolicies.getPolicies()) {
      final ErasureCodingPolicyInfo info = new ErasureCodingPolicyInfo(policy);
      policiesByName.put(policy.getName(), info);
      policiesByID.put(policy.getId(), info);
      allPersistedPolicies.put(policy.getId(),
          new ErasureCodingPolicyInfo(policy));
    }

    enableDefaultPolicy(conf);
    updatePolicies();
    maxCellSize = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_MAX_CELLSIZE_KEY,
        DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_MAX_CELLSIZE_DEFAULT);

    userDefinedAllowed = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_USERPOLICIES_ALLOWED_KEY,
        DFSConfigKeys.
            DFS_NAMENODE_EC_POLICIES_USERPOLICIES_ALLOWED_KEY_DEFAULT);
  }

  /**
   * 获取所有已启用的纠删码策略
   * @return 已启用策略数组
   */
  public ErasureCodingPolicy[] getEnabledPolicies() {
    return enabledPolicies;
  }

  /**
   * 根据策略名称获取已启用的纠删码策略
   * @param name 策略名称
   * @return 匹配的已启用策略，不存在则返回null
   */
  public ErasureCodingPolicy getEnabledPolicyByName(String name) {
    ErasureCodingPolicy ecPolicy = enabledPoliciesByName.get(name);
    if (ecPolicy == null) {
      if (name.equalsIgnoreCase(ErasureCodeConstants.REPLICATION_POLICY_NAME)) {
        ecPolicy = SystemErasureCodingPolicies.getReplicationPolicy();
      }
    }
    return ecPolicy;
  }

  /**
   * 检查指定存储策略是否适合条带化纠删码文件
   * @param storagePolicyID 存储策略ID
   * @return 适合返回true，否则返回false
   */
  public static boolean checkStoragePolicySuitableForECStripedMode(
      byte storagePolicyID) {
    boolean isPolicySuitable = false;
    for (byte suitablePolicy : SUITABLE_STORAGE_POLICIES_FOR_EC_STRIPED_MODE) {
      if (storagePolicyID == suitablePolicy) {
        isPolicySuitable = true;
        break;
      }
    }
    return isPolicySuitable;
  }

  /**
   * 获取所有策略（包含系统内置和用户自定义策略）
   * @return 所有策略信息数组
   */
  public ErasureCodingPolicyInfo[] getPolicies() {
    return allPolicies;
  }

  /**
   * 获取将持久化到fsimage中的所有策略及其状态
   * 与所有策略的区别：仅在启动时启用的默认策略，在持久化列表和fsimage中会标记为禁用
   * @return 持久化策略数组
   */
  public ErasureCodingPolicyInfo[] getPersistedPolicies() {
    return allPersistedPolicies.values()
        .toArray(new ErasureCodingPolicyInfo[0]);
  }

  /**
   * 获取已启用策略数组的拷贝，保证线程安全
   * @return 已启用策略数组拷贝
   */
  public ErasureCodingPolicy[] getCopyOfEnabledPolicies() {
    ErasureCodingPolicy[] copy;
    synchronized (this) {
      copy = Arrays.copyOf(enabledPolicies, enabledPolicies.length);
    }
    return copy;
  }

  /**
   * 根据策略ID获取纠删码策略（包含系统和用户自定义策略）
   * @param id 策略ID
   * @return 匹配的策略，不存在则返回null
   */
  public ErasureCodingPolicy getByID(byte id) {
    final ErasureCodingPolicyInfo ecpi = getPolicyInfoByID(id);
    if (ecpi == null) {
      return null;
    }
    return ecpi.getPolicy();
  }

  /**
   * 根据策略ID获取纠删码策略信息（包含系统和用户自定义策略）
   * @param id 策略ID
   * @return 匹配的策略信息，不存在则返回null
   */
  private ErasureCodingPolicyInfo getPolicyInfoByID(final byte id) {
    return this.policiesByID.get(id);
  }

  /**
   * 根据策略名称获取纠删码策略（包含系统和用户自定义策略）
   * @param name 策略名称
   * @return 匹配的策略，不存在则返回null
   */
  public ErasureCodingPolicy getByName(String name) {
    final ErasureCodingPolicyInfo ecpi = getPolicyInfoByName(name);
    if (ecpi == null) {
      return null;
    }
    return ecpi.getPolicy();
  }

  /**
   * 根据策略名称获取纠删码策略（包含系统、用户自定义策略和副本策略）
   * @param name 策略名称
   * @return 匹配的策略，不存在则返回null
   */
  public ErasureCodingPolicy getErasureCodingPolicyByName(String name) {
    final ErasureCodingPolicyInfo ecpi = getPolicyInfoByName(name);
    if (ecpi == null) {
      if (name.equalsIgnoreCase(ErasureCodeConstants.REPLICATION_POLICY_NAME)) {
        return SystemErasureCodingPolicies.getReplicationPolicy();
      }
      return null;
    }
    return ecpi.getPolicy();
  }

  /**
   * 根据策略名称获取纠删码策略信息（包含系统和用户自定义策略）
   * @param name 策略名称
   * @return 匹配的策略信息，不存在则返回null
   */
  private ErasureCodingPolicyInfo getPolicyInfoByName(final String name) {
    return this.policiesByName.get(name);
  }

  /**
   * 清空清理策略管理器，占位方法待实现
   */
  public void clear() {
    // TODO: we should only clear policies loaded from NN metadata.
    // This is a placeholder for HDFS-7337.
  }

  /**
   * 添加用户自定义纠删码策略，会进行合法性校验和冲突检查
   * @param policy 待添加的策略
   * @return 已添加的策略（如果已存在相同策略则直接返回已有策略）
   */
  public synchronized ErasureCodingPolicy addPolicy(
      ErasureCodingPolicy policy) {
    if (!userDefinedAllowed) {
      throw new HadoopIllegalArgumentException(
          "Addition of user defined erasure coding policy is disabled.");
    }

    if (!CodecUtil.hasCodec(policy.getCodecName())) {
      throw new HadoopIllegalArgumentException("Codec name "
          + policy.getCodecName() + " is not supported");
    }

    int blocksInGroup = policy.getNumDataUnits() + policy.getNumParityUnits();
    if (blocksInGroup > HdfsServerConstants.MAX_BLOCKS_IN_GROUP) {
      throw new HadoopIllegalArgumentException("Number of data and parity blocks in an EC group " +
          blocksInGroup + " should not exceed maximum " + HdfsServerConstants.MAX_BLOCKS_IN_GROUP);
    }

    if (policy.getCellSize() > maxCellSize) {
      throw new HadoopIllegalArgumentException("Cell size " +
          policy.getCellSize() + " should not exceed maximum " +
          maxCellSize + " bytes");
    }

    String assignedNewName = ErasureCodingPolicy.composePolicyName(
        policy.getSchema(), policy.getCellSize());
    for (ErasureCodingPolicyInfo info : getPolicies()) {
      final ErasureCodingPolicy p = info.getPolicy();
      if (p.getName().equals(assignedNewName)) {
        LOG.info("The policy name " + assignedNewName + " already exists");
        return p;
      }
      if (p.getSchema().equals(policy.getSchema()) &&
          p.getCellSize() == policy.getCellSize()) {
        LOG.info("A policy with same schema "
            + policy.getSchema().toString() + " and cell size "
            + p.getCellSize() + " already exists");
        return p;
      }
    }

    if (getCurrentMaxPolicyID() == ErasureCodeConstants.MAX_POLICY_ID) {
      throw new HadoopIllegalArgumentException("Adding erasure coding " +
          "policy failed because the number of policies stored in the " +
          "system already reached the threshold, which is " +
          ErasureCodeConstants.MAX_POLICY_ID);
    }

    policy = new ErasureCodingPolicy(assignedNewName, policy.getSchema(),
        policy.getCellSize(), getNextAvailablePolicyID());
    final ErasureCodingPolicyInfo pi = new ErasureCodingPolicyInfo(policy);
    this.policiesByName.put(policy.getName(), pi);
    this.policiesByID.put(policy.getId(), pi);
    allPolicies =
        policiesByName.values().toArray(new ErasureCodingPolicyInfo[0]);
    allPersistedPolicies.put(policy.getId(),
        new ErasureCodingPolicyInfo(policy));
    LOG.info("Added erasure coding policy " + policy);
    return policy;
  }

  /**
   * 获取当前所有策略中的最大ID值
   * @return 最大策略ID
   */
  private byte getCurrentMaxPolicyID() {
    return policiesByID.keySet().stream().max(Byte::compareTo).orElse((byte)0);
  }

  /**
   * 获取下一个可用的用户自定义策略ID
   * @return 下一个可用策略ID
   */
  private byte getNextAvailablePolicyID() {
    byte nextPolicyID = (byte)(getCurrentMaxPolicyID() + 1);
    return nextPolicyID > ErasureCodeConstants.USER_DEFINED_POLICY_START_ID ?
        nextPolicyID : ErasureCodeConstants.USER_DEFINED_POLICY_START_ID;
  }

  /**
   * 根据名称删除用户自定义纠删码策略，系统策略不允许删除
   * @param name 待删除策略名称
   */
  public synchronized void removePolicy(String name) {
    final ErasureCodingPolicyInfo info = policiesByName.get(name);
    if (info == null) {
      throw new HadoopIllegalArgumentException("The policy name " +
          name + " does not exist");
    }

    final ErasureCodingPolicy ecPolicy = info.getPolicy();
    if (ecPolicy.isSystemPolicy()) {
      throw new HadoopIllegalArgumentException("System erasure coding policy " +
          name + " cannot be removed");
    }

    if (enabledPoliciesByName.containsKey(name)) {
      enabledPoliciesByName.remove(name);
      enabledPolicies =
          enabledPoliciesByName.values().toArray(new ErasureCodingPolicy[0]);
    }
    info.setState(ErasureCodingPolicyState.REMOVED);
    LOG.info("Remove erasure coding policy " + name);
    allPersistedPolicies.put(ecPolicy.getId(),
        createPolicyInfo(ecPolicy, ErasureCodingPolicyState.REMOVED));
    /*
     * TODO HDFS-12405 将删除已移除策略推迟到NameNode重启时执行
     * */
  }

  /**
   * 获取所有已移除的策略列表，仅用于测试
   * @return 已移除策略列表
   */
  @VisibleForTesting
  public List<ErasureCodingPolicy> getRemovedPolicies() {
    ArrayList<ErasureCodingPolicy> removedPolicies = new ArrayList<>();
    for (ErasureCodingPolicyInfo info : policiesByName.values()) {
      final ErasureCodingPolicy ecPolicy = info.getPolicy();
      if (info.isRemoved()) {
        removedPolicies.add(ecPolicy);
      }