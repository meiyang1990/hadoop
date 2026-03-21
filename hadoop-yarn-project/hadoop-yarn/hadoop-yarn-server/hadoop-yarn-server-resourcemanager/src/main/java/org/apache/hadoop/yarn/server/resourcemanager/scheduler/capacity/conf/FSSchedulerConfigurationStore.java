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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.VisibleForTesting;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;


/**
 * 基于Hadoop文件系统实现的容量调度器配置存储，将调度器配置持久化存储在文件系统中
 */
public class FSSchedulerConfigurationStore extends YarnConfigurationStore {
  public static final Logger LOG = LoggerFactory.getLogger(
      FSSchedulerConfigurationStore.class);

  @VisibleForTesting
  protected static final Version CURRENT_VERSION_INFO
      = Version.newInstance(0, 1);

  private static final String TMP = ".tmp";

  private int maxVersion;
  private Path schedulerConfDir;
  private FileSystem fileSystem;
  private PathFilter configFilePathFilter;
  private volatile Configuration schedConf;
  private volatile Configuration oldConf;
  private Path tempConfigPath;
  private Path configVersionFile;

  @Override
  public void initialize(Configuration fsConf, Configuration vSchedConf,
      RMContext rmContext) throws Exception {
    // 创建配置文件路径过滤器，筛选合法配置文件
    this.configFilePathFilter = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        if (path == null) {
          return false;
        }
        String pathName = path.getName();
        // 匹配配置文件前缀，过滤掉临时文件
        return pathName.startsWith(YarnConfiguration.CS_CONFIGURATION_FILE)
            && !pathName.endsWith(TMP);
      }
    };

    Configuration conf = new Configuration(fsConf);
    // 从配置中读取存储目录路径
    String schedulerConfPathStr = conf.get(
        YarnConfiguration.SCHEDULER_CONFIGURATION_FS_PATH);
    if (schedulerConfPathStr == null || schedulerConfPathStr.isEmpty()) {
      throw new IOException(
          YarnConfiguration.SCHEDULER_CONFIGURATION_FS_PATH
              + " must be set");
    }
    this.schedulerConfDir = new Path(schedulerConfPathStr);
    String scheme = schedulerConfDir.toUri().getScheme();
    if (scheme == null) {
      scheme = FileSystem.getDefaultUri(conf).getScheme();
    }
    if (scheme != null) {
      // 禁用文件系统缓存，确保获取最新文件系统实例
      String disableCacheName = String.format("fs.%s.impl.disable.cache",
          scheme);
      conf.setBoolean(disableCacheName, true);
    }
    // 获取文件系统实例
    this.fileSystem = this.schedulerConfDir.getFileSystem(conf);
    // 读取允许保留的最大配置文件版本数
    this.maxVersion = conf.getInt(
        YarnConfiguration.SCHEDULER_CONFIGURATION_FS_MAX_VERSION,
        YarnConfiguration.DEFAULT_SCHEDULER_CONFIGURATION_FS_MAX_VERSION);
    LOG.info("schedulerConfDir=" + schedulerConfPathStr);
    LOG.info("capacity scheduler file max version = " + maxVersion);

    // 存储目录不存在则创建
    if (!fileSystem.exists(schedulerConfDir)) {
      if (!fileSystem.mkdirs(schedulerConfDir)) {
        throw new IOException("mkdir " + schedulerConfPathStr + " failed");
      }
    }

    // 初始化配置版本文件
    this.configVersionFile = new Path(schedulerConfPathStr, "ConfigVersion");
    if (!fileSystem.exists(configVersionFile)) {
      fileSystem.createNewFile(configVersionFile);
      writeConfigVersion(0L);
    }

    // 如果没有已存在的配置文件，写入初始配置
    if (this.getConfigFileInputStream() == null) {
      writeConfigurationToFileSystem(vSchedConf);
      long configVersion = getConfigVersion() + 1L;
      writeConfigVersion(configVersion);
    }

    // 从文件系统加载最新配置到内存
    this.schedConf = this.getConfigurationFromFileSystem();
  }

  /**
   * 记录配置变更，写入临时配置文件预提交
   * @param logMutation 需要持久化的配置变更
   * @throws IOException 写入临时配置文件失败时抛出
   */
  @Override
  public void logMutation(LogMutation logMutation) throws IOException {
    LOG.info(new GsonBuilder().serializeNulls().create().toJson(logMutation);
    // 保存变更前的配置用于回滚
    oldConf = new Configuration(schedConf);
    Map<String, String> mutations = logMutation.getUpdates();
    // 遍历应用所有配置变更
    for (Map.Entry<String, String> kv : mutations.entrySet()) {
      if (kv.getValue() == null) {
        this.schedConf.unset(kv.getKey());
      } else {
        this.schedConf.set(kv.getKey(), kv.getValue());
      }
    }
    // 将变更后的配置写入临时文件
    tempConfigPath = writeTmpConfig(schedConf);
  }

  /**
   * 确认配置变更，提交或回滚预写入的临时配置
   * @param pendingMutation 待确认的配置变更
   * @param isValid 变更是否有效，true则正式提交，false则回滚
   * @throws Exception 处理过程IO失败时抛出
   */
  @Override
  public void confirmMutation(LogMutation pendingMutation,
      boolean isValid) throws Exception {
    if (pendingMutation == null || tempConfigPath == null) {
      LOG.warn("pendingMutation or tempConfigPath is null, do nothing");
      return;
    }
    if (isValid) {
      // 变更有效，将临时文件转为正式配置文件
      finalizeFileSystemFile();
      // 递增并保存配置版本号
      long configVersion = getConfigVersion() + 1L;
      writeConfigVersion(configVersion);
    } else {
      // 变更无效，回滚到变更前配置，删除临时文件
      schedConf = oldConf;
      removeTmpConfigFile();
    }
    tempConfigPath = null;
  }

  private void finalizeFileSystemFile() throws IOException {
    // 由confirmMutation保证tempConfigPath非空
    Path finalConfigPath = getFinalConfigPath(tempConfigPath);
    // 重命名临时文件为正式配置文件
    fileSystem.rename(tempConfigPath, finalConfigPath);
    LOG.info("finalize temp configuration file successfully, finalConfigPath="
        + finalConfigPath);
  }

  @Override
  public void format() throws Exception {
    // 列出所有正式配置文件
    FileStatus[] fileStatuses = fileSystem.listStatus(this.schedulerConfDir,
        this.configFilePathFilter);
    if (fileStatuses == null) {
      return;
    }
    // 删除所有配置文件，格式化存储
    for (int i = 0; i < fileStatuses.length; i++) {
      fileSystem.delete(fileStatuses[i].getPath(), false);
      LOG.info("delete config file " + fileStatuses[i].getPath());
    }
  }

  private Path getFinalConfigPath(Path tempPath) {
    String tempConfigPathStr = tempPath.getName();
    if (!tempConfigPathStr.endsWith(TMP)) {
      LOG.warn(tempPath + " does not end with '"
          + TMP + "' return null");
      return null;
    }
    // 去除临时文件后缀，得到正式文件名
    String finalConfigPathStr = tempConfigPathStr.substring(0,
        (tempConfigPathStr.length() - TMP.length()));
    return new Path(tempPath.getParent(), finalConfigPathStr);
  }

  private void removeTmpConfigFile() throws IOException {
    // 由confirmMutation保证tempConfigPath非空
    fileSystem.delete(tempConfigPath, true);
    LOG.info("delete temp configuration file: " + tempConfigPath);
  }

  private Configuration getConfigurationFromFileSystem() throws IOException {
    long start = Time.monotonicNow();

    Configuration conf = new Configuration(false);
    // 获取最新配置文件输入流
    InputStream configInputStream = getConfigFileInputStream();
    if (configInputStream == null) {
      throw new IOException(
          "no capacity scheduler file in " + this.schedulerConfDir);
    }

    // 加载配置
    conf.addResource(configInputStream);
    // 复制配置项到新配置对象
    Configuration result = new Configuration(false);
    for (Map.Entry<String, String> entry : conf) {
      result.set(entry.getKey(), entry.getValue());
    }
    LOG.info("upload conf from fileSystem took "
            + (Time.monotonicNow() - start) + " ms");

    // 更新内存中的配置，用于HA切换后刷新
    this.schedConf = result;
    return result;
  }

  private InputStream getConfigFileInputStream() throws IOException {
    // 获取最新版本配置文件路径
    Path lastestConfigPath = getLatestConfigPath();
    if (lastestConfigPath == null) {
      return null;
    }
    // 打开并返回输入流
    return fileSystem.open(lastestConfigPath);
  }

  private Path getLatestConfigPath() throws IOException {
    // 列出所有正式配置文件
    FileStatus[] fileStatuses = fileSystem.listStatus(this.schedulerConfDir,
        this.configFilePathFilter);

    if (fileStatuses == null || fileStatuses.length == 0) {
      return null;
    }
    // 按路径排序，最后一个就是最新版本
    Arrays.sort(fileStatuses);

    return fileStatuses[fileStatuses.length - 1].getPath();
  }

  private void writeConfigVersion(long configVersion) throws IOException {
    // 覆盖写入配置版本号到版本文件
    try (FSDataOutputStream out = fileSystem.create(configVersionFile, true)) {
      out.writeLong(configVersion);
    } catch (IOException e) {
      LOG.info("Failed to write config version at {}", configVersionFile, e);
      throw e;
    }
  }

  @Override
  public long getConfigVersion() throws Exception {
    // 从版本文件读取当前配置版本号
    try (FSDataInputStream in = fileSystem.open(configVersionFile)) {
      return in.readLong();
    } catch (IOException e) {
      LOG.info("Failed to read config version at {}", configVersionFile, e);
      throw e;
    }
  }



  @VisibleForTesting
  private Path writeTmpConfig(Configuration vSchedConf) throws IOException {
    long start = Time.monotonicNow();
    // 生成带时间戳的临时文件名，保证唯一
    String tempSchedulerConfigFile = YarnConfiguration.CS_CONFIGURATION_FILE
        + "." + System.currentTimeMillis() + TMP;

    Path tempSchedulerConfigPath = new Path(this.schedulerConfDir,
        tempSchedulerConfigFile);

    try (FSDataOutputStream outputStream = fileSystem.create(
        tempSchedulerConfigPath)) {
      // 超过最大版本数时清理旧配置文件
      cleanConfigurationFile();

      // 将配置写入XML格式输出流
      vSchedConf.writeXml(outputStream);
      LOG.info(
          "write temp capacity configuration successfully, schedulerConfigFile="
              + tempSchedulerConfigPath);
    } catch (IOException e) {
      LOG.info("write temp capacity configuration fail, schedulerConfigFile="
          + tempSchedulerConfigPath, e);
      throw e;
    }
    LOG.info("write temp configuration to fileSystem took "
        + (Time.monotonicNow() - start) + " ms");
    return tempSchedulerConfigPath;
  }

  @VisibleForTesting
  void writeConfigurationToFileSystem(Configuration vSchedConf)
      throws IOException {
    // 先写入临时文件，再确认提交为正式文件
    tempConfigPath = writeTmpConfig(vSchedConf);
    finalizeFileSystemFile();
  }

  private void cleanConfigurationFile() throws IOException {
    // 列出所有正式配置文件
    FileStatus[] fileStatuses = fileSystem.listStatus(this.schedulerConfDir,
        this.configFilePathFilter);

    // 不超过最大保留版本数则无需清理
    if (fileStatuses == null || fileStatuses.length <= this.maxVersion) {
      return;
    }
    // 按版本排序，删除最旧的多余配置文件
    Arrays.sort(fileStatuses);
    int configFileNum = fileStatuses.length;
    if (fileStatuses.length > this.maxVersion) {
      for (int i = 0; i < configFileNum - this.maxVersion; i++) {
        fileSystem.delete(fileStatuses[i].getPath(), false);
        LOG.info("delete config file " + fileStatuses[i].getPath());
      }
    }
  }

  @Override
  public Configuration retrieve() throws IOException {
    // 从文件系统加载并返回最新配置
    return getConfigurationFromFileSystem();
  }

  @Override
  public List<LogMutation> getConfirmedConfHistory(long fromId) {
    // 本实现不支持该功能
    return null;
  }

  @Override
  protected LinkedList<LogMutation> getLogs() {
    // 本实现不支持该功能
    return null;
  }

  @Override
  protected Version getConfStoreVersion() throws Exception {
    return null;
  }

  @Override
  protected void storeVersion() throws Exception {

  }

  @Override
  protected Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  @Override
  public void close() throws IOException {
    // 关闭文件系统实例
    if (fileSystem != null) {
      fileSystem.close();
    }
  }
}