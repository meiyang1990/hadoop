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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.commons.io.serialization.ValidatingObjectInputStream;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 基于Zookeeper实现的容量调度器配置存储，是{@link YarnConfigurationStore}的实现类
 * 用于在HA模式下共享调度器配置，保证多个RM节点配置一致
 */
public class ZKConfigurationStore extends YarnConfigurationStore {

  public static final Logger LOG =
      LoggerFactory.getLogger(ZKConfigurationStore.class);

  private long maxLogs;

  @VisibleForTesting
  protected static final Version CURRENT_VERSION_INFO = Version
      .newInstance(0, 1);
  private Configuration conf;

  private static final String ZK_VERSION_PATH = "VERSION";
  private static final String LOGS_PATH = "LOGS";
  private static final String CONF_STORE_PATH = "CONF_STORE";
  private static final String FENCING_PATH = "FENCING";
  private static final String CONF_VERSION_PATH = "CONF_VERSION";
  private static final String NODEEXISTS_MSG = "Encountered NodeExists error."
      + " Skipping znode creation since another RM has already created it";
  private String znodeParentPath;
  private String zkVersionPath;
  private String logsPath;
  private String confStorePath;
  private String fencingNodePath;
  private String confVersionPath;

  private ZKCuratorManager zkManager;
  private List<ACL> zkAcl;

  @Override
  public void initialize(Configuration config, Configuration schedConf,
      RMContext rmContext) throws Exception {
    this.conf = config;

    // 从配置中读取Zookeeper父节点路径，使用默认值兜底
    this.znodeParentPath = conf.get(
        YarnConfiguration.RM_SCHEDCONF_STORE_ZK_PARENT_PATH,
        YarnConfiguration.DEFAULT_RM_SCHEDCONF_STORE_ZK_PARENT_PATH);

    // 读取配置变更日志最大保留条数
    this.maxLogs = conf.getLong(YarnConfiguration.RM_SCHEDCONF_MAX_LOGS,
        YarnConfiguration.DEFAULT_RM_SCHEDCONF_ZK_MAX_LOGS);
    // 创建并启动Zookeeper Curator管理器
    this.zkManager =
        rmContext.getResourceManager().createAndStartZKManager(conf);
    // 获取Zookeeper节点ACL权限配置
    this.zkAcl = ZKCuratorManager.getZKAcls(conf);

    // 拼接各ZNode完整路径
    this.zkVersionPath = getNodePath(znodeParentPath, ZK_VERSION_PATH);
    this.logsPath = getNodePath(znodeParentPath, LOGS_PATH);
    this.confStorePath = getNodePath(znodeParentPath, CONF_STORE_PATH);
    this.fencingNodePath = getNodePath(znodeParentPath, FENCING_PATH);
    this.confVersionPath = getNodePath(znodeParentPath, CONF_VERSION_PATH);

    try {
      // 递归创建父节点
      zkManager.createRootDirRecursively(znodeParentPath, zkAcl);
    } catch(NodeExistsException e) {
      // 节点已存在说明其他RM已经创建，直接忽略警告
      LOG.warn(NODEEXISTS_MSG, e);
    }
    // 删除 fencing节点，用于隔离旧的写入操作
    zkManager.delete(fencingNodePath);

    // 日志路径不存在则创建，初始化空日志列表
    if (createNewZkPath(logsPath)) {
      setZkData(logsPath, new LinkedList<LogMutation>());
    }

    // 配置版本路径不存在则创建，初始化为0
    if (createNewZkPath(confVersionPath)) {
      setZkData(confVersionPath, String.valueOf(0));
    }

    // 配置存储路径不存在则创建，使用初始调度配置初始化
    if (createNewZkPath(confStorePath)) {
      HashMap<String, String> mapSchedConf = new HashMap<>();
      for (Map.Entry<String, String> entry : schedConf) {
        mapSchedConf.put(entry.getKey(), entry.getValue());
      }
      setZkData(confStorePath, mapSchedConf);
      // 版本号自增
      long configVersion = getConfigVersion() + 1L;
      setZkData(confVersionPath, String.valueOf(configVersion));
    }
  }

  @VisibleForTesting
  @Override
  protected LinkedList<LogMutation> getLogs() throws Exception {
    return unsafeCast(deserializeObject(getZkData(logsPath)));
  }

  @Override
  public Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  @Override
  public Version getConfStoreVersion() throws Exception {
    // 版本节点存在则读取并解析版本信息
    if (zkManager.exists(zkVersionPath)) {
      byte[] data = getZkData(zkVersionPath);
      return new VersionPBImpl(YarnServerCommonProtos.VersionProto
          .parseFrom(data));
    }

    // 不存在返回null
    return null;
  }

  @Override
  public void format() throws Exception {
    // 删除整个配置存储父节点，清空所有数据
    zkManager.delete(znodeParentPath);
  }

  @Override
  public synchronized void storeVersion() throws Exception {
    // 将当前版本序列化
    byte[] data =
        ((VersionPBImpl) CURRENT_VERSION_INFO).getProto().toByteArray();

    // 节点存在则更新，不存在则创建
    if (zkManager.exists(zkVersionPath)) {
      safeSetZkData(zkVersionPath, data);
    } else {
      safeCreateZkData(zkVersionPath, data);
    }
  }

  @Override
  public void logMutation(LogMutation logMutation) throws Exception {
    // 只有开启日志保留才记录变更
    if (maxLogs > 0) {
      // 读取已有变更日志
      byte[] storedLogs = getZkData(logsPath);
      LinkedList<LogMutation> logs = new LinkedList<>();
      if (storedLogs != null) {
        logs = unsafeCast(deserializeObject(storedLogs));
      }
      // 添加新变更
      logs.add(logMutation);
      // 超过最大保留条数，删除最旧的一条
      if (logs.size() > maxLogs) {
        logs.remove(logs.removeFirst());
      }
      // 安全写回Zookeeper
      safeSetZkData(logsPath, logs);
    }
  }

  @Override
  public void confirmMutation(LogMutation pendingMutation,
      boolean isValid) throws Exception {
    // 变更有效则应用到存储配置
    if (isValid) {
      // 读取当前已存储配置
      Configuration storedConfigs = retrieve();
      Map<String, String> mapConf = new HashMap<>();
      for (Map.Entry<String, String> storedConf : storedConfigs) {
        mapConf.put(storedConf.getKey(), storedConf.getValue());
      }
      // 遍历变更，应用修改：空值删除配置，非空值更新配置
      for (Map.Entry<String, String> confChange :
          pendingMutation.getUpdates().entrySet()) {
        if (confChange.getValue() == null || confChange.getValue().isEmpty()) {
          mapConf.remove(confChange.getKey());
        } else {
          mapConf.put(confChange.getKey(), confChange.getValue());
        }
      }
      // 安全写回更新后的配置
      safeSetZkData(confStorePath, mapConf);
      // 版本号自增
      long configVersion = getConfigVersion() + 1L;
      setZkData(confVersionPath, String.valueOf(configVersion));

    }
  }

  @Override
  public synchronized Configuration retrieve() {
    byte[] serializedSchedConf;
    try {
      // 从Zookeeper读取序列化后的配置
      serializedSchedConf = getZkData(confStorePath);
    } catch (Exception e) {
      LOG.error("Failed to retrieve configuration from zookeeper store", e);
      return null;
    }
    try {
      // 反序列化配置并封装为Configuration对象返回
      Map<String, String> map =
          unsafeCast(deserializeObject(serializedSchedConf));
      Configuration c = new Configuration(false);
      for (Map.Entry<String, String> e : map.entrySet()) {
        c.set(e.getKey(), e.getValue());
      }
      return c;
    } catch (Exception e) {
      LOG.error("Exception while deserializing scheduler configuration " +
          "from store", e);
    }
    return null;
  }

  @Override
  public long getConfigVersion() throws Exception {
    // 从Zookeeper读取当前配置版本
    String version = zkManager.getStringData(confVersionPath);
    if (version == null) {
      throw new IllegalStateException("Config version can not be properly " +
          "serialized. Check Zookeeper config version path to locate " +
          "the error!");
    }

    return Long.parseLong(version);
  }

  @Override
  public List<LogMutation> getConfirmedConfHistory(long fromId) {
    return null; // unimplemented
  }

  /**
   * 仅当路径不存在时创建新ZNode路径
   *
   * @param path Zookeeper路径
   * @return 创建成功返回true，已存在或创建失败返回false
   * @throws Exception 异常
   */
  private boolean createNewZkPath(String path) throws Exception {
    if (!zkManager.exists(path)) {
      try {
        zkManager.create(path, zkAcl);
      } catch(NodeExistsException e) {
        LOG.warn(NODEEXISTS_MSG, e);
        return false;
      }
      return true;
    } else {
      return false;
    }
  }

  @VisibleForTesting
  protected byte[] getZkData(String path) throws Exception {
    // 重试机制：防止格式化或写入过程中读取空数据导致RM启动失败
    int maxRetries = 6;
    int attempt = 1;
    int sleepBetweenRetries = conf.getInt(
        YarnConfiguration.RM_SCHEDCONF_STORE_ZK_READ_RETRY_SECS,
        YarnConfiguration.DEFAULT_RM_SCHEDCONF_STORE_ZK_READ_RETRY_SECS);

    // 重试循环，最多重试maxRetries次
    while (attempt < maxRetries) {
      if(zkManager.exists(path)) {
        LOG.debug("zkManager.exists(path) {} exists.", path);
        byte[] zkData = zkManager.getData(path);
        // 数据非空则返回
        if (zkData != null && zkData.length > 0) {
          LOG.debug("We are returning the zkData OK!");
          return zkData;
        }
      }
      // 路径不存在或数据为空，等待重试
      LOG.warn("The ZK CONFSTORE path or the ZkData was null. Retrying in {} "
              + "seconds... (Attempt {})", sleepBetweenRetries, attempt);
      TimeUnit.SECONDS.sleep(sleepBetweenRetries);
      attempt++;
    }
    // 重试耗尽仍失败，返回空数组
    LOG.error("The ZK CONFSTORE path or the ZkData was null. Giving up.");
    return new byte[0];
  }


  @VisibleForTesting
  protected void setZkData(String path, byte[] data) throws Exception {
    zkManager.setData(path, data, -1);
  }

  private void setZkData(String path, Object data) throws Exception {
    setZkData(path, serializeObject(data));
  }

  private void setZkData(String path, String data) throws Exception {
    zkManager.setData(path, data, -1);
  }

  private void safeSetZkData(String path, byte[] data) throws Exception {
    // 带 fencing 保护的安全写入，防止脑裂
    zkManager.safeSetData(path, data, -1, zkAcl, fencingNodePath);
  }

  private void safeSetZkData(String path, Object data) throws Exception {
    safeSetZkData(path, serializeObject(data));
  }

  @VisibleForTesting
  protected void safeCreateZkData(String path, byte[] data) throws Exception {
    try {
      // 带 fencing 保护的安全创建持久节点
      zkManager.safeCreate(path, data, zkAcl, CreateMode.PERSISTENT,
          zkAcl, fencingNodePath);
    } catch(NodeExistsException e) {
      LOG.warn(NODEEXISTS_MSG, e);
    }
  }

  private static String getNodePath(String root, String nodeName) {
    return ZKCuratorManager.getNodePath(root, nodeName);
  }

  /**
   * 将Java对象序列化为字节数组
   * @param o 待序列化对象
   * @return 序列化后的字节数组
   * @throws Exception 序列化异常
   */
  private static byte[] serializeObject(Object o) throws Exception {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);) {
      oos.writeObject(o);
      oos.flush();
      baos.flush();
      return baos.toByteArray();
    }
  }

  /**
   * 将字节数组反序列化为Java对象，白名单限制反序列化类保证安全
   * @param bytes 待反序列化字节数组
   * @return 反序列化后的对象
   * @throws Exception 反序列化异常
   */
  private static Object deserializeObject(byte[] bytes) throws Exception {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         ValidatingObjectInputStream ois = new ValidatingObjectInputStream(bais);) {
      // 仅允许白名单中的类反序列化
      ois.accept(LinkedList.class, LogMutation.class, HashMap.class, String.class);
      return ois.readObject();
    }
  }

  /**
   * 不安全的强制类型转换，用于泛型转换
   * @param o 待转换对象
   * @param <T> 目标泛型类型
   * @return 转换后的对象
   * @throws ClassCastException 类型转换异常
   */
  @SuppressWarnings("unchecked")
  private static <T> T unsafeCast(Object o) throws ClassCastException {
    return (T)o;
  }

  @Override
  public void close() throws IOException {
    // 关闭Zookeeper连接，释放资源
    if (zkManager  != null) {
      zkManager.close();
    }
  }
}