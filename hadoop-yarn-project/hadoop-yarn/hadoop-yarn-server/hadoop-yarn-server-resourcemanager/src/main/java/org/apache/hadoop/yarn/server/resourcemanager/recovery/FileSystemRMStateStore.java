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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.EpochProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.Epoch;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.RMDelegationTokenIdentifierData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.AMRMTokenSecretManagerStatePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.EpochPBImpl;

import org.apache.hadoop.classification.VisibleForTesting;

@Private
@Unstable
/**
 * 基于文件系统实现的ResourceManager状态存储，用于RM重启/故障转移时恢复状态。
 * 可兼容任何实现了Hadoop FileSystem接口的存储系统，不依赖特殊目录结构，支持简单键值存储。
 * 版本变更记录：
 * 1.1 -> 1.2：新增独立存储AMRMTokenSecretManager状态，保存当前和下一个主密钥，从ApplicationAttemptState中移除AMRMToken
 * 1.2 -> 1.3：新增ReservationSystem状态存储
 */
public class FileSystemRMStateStore extends RMStateStore {

  public static final Logger LOG =
      LoggerFactory.getLogger(FileSystemRMStateStore.class);

  protected static final String ROOT_DIR_NAME = "FSRMStateRoot";
  protected static final Version CURRENT_VERSION_INFO = Version
    .newInstance(1, 3);
  protected static final String AMRMTOKEN_SECRET_MANAGER_NODE =
      "AMRMTokenSecretManagerNode";

  private static final String UNREADABLE_BY_SUPERUSER_XATTRIB =
          "security.hdfs.unreadable.by.superuser";
  protected FileSystem fs;
  @VisibleForTesting
  protected Configuration fsConf;

  private Path rootDirPath;
  @Private
  @VisibleForTesting
  Path rmDTSecretManagerRoot;
  private Path rmAppRoot;
  private Path dtSequenceNumberPath = null;
  private int fsNumRetries;
  private long fsRetryInterval;
  private boolean intermediateEncryptionEnabled =
      YarnConfiguration.DEFAULT_YARN_INTERMEDIATE_DATA_ENCRYPTION;

  @VisibleForTesting
  Path fsWorkingPath;

  Path amrmTokenSecretManagerRoot;
  private Path reservationRoot;
  private Path proxyCARoot;

  @Override
  public synchronized void initInternal(Configuration conf)
      throws Exception{
    // 从配置获取状态存储根路径
    fsWorkingPath = new Path(conf.get(YarnConfiguration.FS_RM_STATE_STORE_URI));
    // 拼接根目录路径
    rootDirPath = new Path(fsWorkingPath, ROOT_DIR_NAME);
    // 初始化各模块状态存储路径
    rmDTSecretManagerRoot = new Path(rootDirPath, RM_DT_SECRET_MANAGER_ROOT);
    rmAppRoot = new Path(rootDirPath, RM_APP_ROOT);
    amrmTokenSecretManagerRoot =
        new Path(rootDirPath, AMRMTOKEN_SECRET_MANAGER_ROOT);
    reservationRoot = new Path(rootDirPath, RESERVATION_SYSTEM_ROOT);
    proxyCARoot = new Path(rootDirPath, PROXY_CA_ROOT);
    // 读取文件系统操作重试配置
    fsNumRetries =
        conf.getInt(YarnConfiguration.FS_RM_STATE_STORE_NUM_RETRIES,
            YarnConfiguration.DEFAULT_FS_RM_STATE_STORE_NUM_RETRIES);
    fsRetryInterval =
        conf.getLong(YarnConfiguration.FS_RM_STATE_STORE_RETRY_INTERVAL_MS,
                YarnConfiguration.DEFAULT_FS_RM_STATE_STORE_RETRY_INTERVAL_MS);
    // 读取中间数据加密配置
    intermediateEncryptionEnabled =
        conf.getBoolean(YarnConfiguration.YARN_INTERMEDIATE_DATA_ENCRYPTION,
          YarnConfiguration.DEFAULT_YARN_INTERMEDIATE_DATA_ENCRYPTION);
  }

  @Override
  protected synchronized void startInternal() throws Exception {
    // 延迟到服务启动阶段创建文件系统，此时RM已完成Kerberos认证，可以获取合法凭证
    fsConf = new Configuration(getConfig());

    String scheme = fsWorkingPath.toUri().getScheme();
    if (scheme == null) {
      scheme = FileSystem.getDefaultUri(fsConf).getScheme();
    }
    if (scheme != null) {
      // 禁用文件系统缓存，确保每次获取全新的文件系统实例
      String disableCacheName = String.format("fs.%s.impl.disable.cache", scheme);
      fsConf.setBoolean(disableCacheName, true);
    }

    // 获取文件系统实例
    fs = fsWorkingPath.getFileSystem(fsConf);
    // 重试创建所有模块目录
    mkdirsWithRetries(rmDTSecretManagerRoot);
    mkdirsWithRetries(rmAppRoot);
    mkdirsWithRetries(amrmTokenSecretManagerRoot);
    mkdirsWithRetries(reservationRoot);
    mkdirsWithRetries(proxyCARoot);
  }

  @Override
  protected synchronized void closeInternal() throws Exception {
    closeWithRetries();
  }

  @Override
  protected Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  @Override
  protected synchronized Version loadVersion() throws Exception {
    // 获取版本文件路径
    Path versionNodePath = getNodePath(rootDirPath, VERSION_NODE);
    FileStatus status = getFileStatusWithRetries(versionNodePath);
    if (status != null) {
      // 读取版本数据并反序列化
      byte[] data = readFileWithRetries(versionNodePath, status.getLen());
      Version version =
          new VersionPBImpl(VersionProto.parseFrom(data));
      return version;
    }
    // 版本文件不存在返回null
    return null;
  }

  @Override
  protected synchronized void storeVersion() throws Exception {
    // 序列化当前版本信息
    Path versionNodePath = getNodePath(rootDirPath, VERSION_NODE);
    byte[] data =
        ((VersionPBImpl) CURRENT_VERSION_INFO).getProto().toByteArray();
    // 已存在则更新，否则新建
    if (existsWithRetries(versionNodePath)) {
      updateFile(versionNodePath, data, false);
    } else {
      writeFileWithRetries(versionNodePath, data, false);
    }
  }
  
  @Override
  public synchronized long getAndIncrementEpoch() throws Exception {
    // 获取epoch文件路径
    Path epochNodePath = getNodePath(rootDirPath, EPOCH_NODE);
    long currentEpoch = baseEpoch;
    FileStatus status = getFileStatusWithRetries(epochNodePath);
    if (status != null) {
      // 加载当前epoch值
      byte[] data = readFileWithRetries(epochNodePath, status.getLen());
      Epoch epoch = new EpochPBImpl(EpochProto.parseFrom(data));
      currentEpoch = epoch.getEpoch();
      // 自增后存储新epoch值
      byte[] storeData = Epoch.newInstance(nextEpoch(currentEpoch)).getProto()
          .toByteArray();
      updateFile(epochNodePath, storeData, false);
    } else {
      // 初始化epoch文件，第一个epoch为baseEpoch + 1
      byte[] storeData = Epoch.newInstance(nextEpoch(currentEpoch)).getProto()
          .toByteArray();
      writeFileWithRetries(epochNodePath, storeData, false);
    }
    return currentEpoch;
  }
  
  @Override
  public synchronized RMState loadState() throws Exception {
    RMState rmState = new RMState();
    // 恢复DelegationTokenSecretManager状态
    loadRMDTSecretManagerState(rmState);
    // 恢复所有应用状态
    loadRMAppState(rmState);
    // 恢复AMRMTokenSecretManager状态
    loadAMRMTokenSecretManagerState(rmState);
    // 恢复资源预约系统状态
    loadReservationSystemState(rmState);
    // 恢复ProxyCAManager状态
    loadProxyCAManagerState(rmState);
    return rmState;
  }

  private void loadReservationSystemState(RMState rmState) throws Exception {
    try {
      // 创建预约状态处理器，遍历处理所有预约文件
      final ReservationStateFileProcessor fileProcessor = new
          ReservationStateFileProcessor(rmState);
      final Path rootDirectory = this.reservationRoot;

      processDirectoriesOfFiles(fileProcessor, rootDirectory);
    } catch (Exception e) {
      LOG.error("Failed to load state.", e);
      throw e;
    }
  }

  private void loadAMRMTokenSecretManagerState(RMState rmState)
      throws Exception {
    // 检查并修复未完成的更新操作
    checkAndResumeUpdateOperation(amrmTokenSecretManagerRoot);
    Path amrmTokenSecretManagerStateDataDir =
        new Path(amrmTokenSecretManagerRoot, AMRMTOKEN_SECRET_MANAGER_NODE);
    FileStatus status = getFileStatusWithRetries(
        amrmTokenSecretManagerStateDataDir);
    if (status == null) {
      return;
    }
    assert status.isFile();
    // 读取并反序列化状态数据
    byte[] data = readFileWithRetries(amrmTokenSecretManagerStateDataDir,
            status.getLen());
    AMRMTokenSecretManagerStatePBImpl stateData =
        new AMRMTokenSecretManagerStatePBImpl(
          AMRMTokenSecretManagerStateProto.parseFrom(data));
    // 将状态存入RMState对象
    rmState.amrmTokenSecretManagerState =
        AMRMTokenSecretManagerState.newInstance(
          stateData.getCurrentMasterKey(), stateData.getNextMasterKey());
  }

  private void loadRMAppState(RMState rmState) throws Exception {
    try {
      List<ApplicationAttemptStateData> attempts = new ArrayList<>();
      // 创建应用状态处理器，遍历处理所有应用文件
      final RMAppStateFileProcessor rmAppStateFileProcessor =
          new RMAppStateFileProcessor(rmState, attempts);
      final Path rootDirectory = this.rmAppRoot;

      processDirectoriesOfFiles(rmAppStateFileProcessor, rootDirectory);

      // 将所有加载的尝试关联到对应应用
      // 理论上每个尝试都应有对应应用，因为删除操作会同时删除应用和尝试目录
      for (ApplicationAttemptStateData attemptState : attempts) {
        ApplicationId appId = attemptState.getAttemptId().getApplicationId();
        ApplicationStateData appState = rmState.appState.get(appId);
        assert appState != null;
        appState.attempts.put(attemptState.getAttemptId(), attemptState);
      }
      LOG.info("Done loading applications from FS state store");
    } catch (Exception e) {
      LOG.error("Failed to load state.", e);
      throw e;
    }
  }

  private void processDirectoriesOfFiles(
      RMStateFileProcessor rmAppStateFileProcessor, Path rootDirectory)
    throws Exception {
    // 遍历根目录下所有一级子目录
    for (FileStatus dir : listStatusWithRetries(rootDirectory)) {
      // 检查并修复当前目录下未完成的更新操作
      checkAndResumeUpdateOperation(dir.getPath());
      String dirName = dir.getPath().getName();
      // 遍历目录下所有文件
      for (FileStatus fileNodeStatus : listStatusWithRetries(dir.getPath())) {
        assert fileNodeStatus.isFile();
        String fileName = fileNodeStatus.getPath().getName();
        // 清理未完成写入的临时文件，跳过处理
        if (checkAndRemovePartialRecordWithRetries(fileNodeStatus.getPath())) {
          continue;
        }
        // 读取文件内容
        byte[] fileData = readFileWithRetries(fileNodeStatus.getPath(),
                fileNodeStatus.getLen());
        // 设置超级用户不可读扩展属性（如果启用加密且未设置）
        setUnreadableBySuperuserXattrib(fileNodeStatus.getPath());

        // 调用处理器处理当前文件
        rmAppStateFileProcessor.processChildNode(dirName, fileName,
            fileData);
      }
    }
  }

  private boolean checkAndRemovePartialRecord(Path record) throws IOException {
    // 如果文件以.tmp结尾，说明写入过程中断，需要删除该不完整文件
    if (record.getName().endsWith(".tmp")) {
      LOG.error("incomplete rm state store entry found :"
          + record);
      fs.delete(record, false);
      return true;
    }
    return false;
  }

  private void checkAndResumeUpdateOperation(Path path) throws Exception {
    // 加载状态前检查是否存在.new结尾的文件，.new文件说明更新过程中断，需要完成替换旧文件
    FileStatus[] newChildNodes =
        listStatusWithRetries(path, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(".new");
      }
    });
    // 遍历所有未完成更新，完成替换操作
    for(FileStatus newChildNodeStatus : newChildNodes) {
      assert newChildNodeStatus.isFile();
      String newChildNodeName = newChildNodeStatus.getPath().getName();
      String childNodeName = newChildNodeName.substring(
              0, newChildNodeName.length() - ".new".length());
      Path childNodePath =
          new Path(newChildNodeStatus.getPath().getParent(), childNodeName);
      // 用.new文件替换原文件，完成更新
      replaceFile(newChildNodeStatus.getPath(), childNodePath);
    }
  }
  private void loadRMDTSecretManagerState(RMState rmState) throws Exception {
    // 检查并修复未完成的更新操作
    checkAndResumeUpdateOperation(rmDTSecretManagerRoot);
    FileStatus[] childNodes = listStatusWithRetries(rmDTSecretManagerRoot);

    // 遍历所有文件恢复状态
    for(FileStatus childNodeStatus : childNodes) {
      assert childNodeStatus.isFile();
      String childNodeName = childNodeStatus.getPath().getName();
      // 删除不完整文件，跳过处理
      if (checkAndRemovePartialRecordWithRetries(childNodeStatus.getPath())) {
        continue;
      }
      // 处理令牌序列号文件，更新当前最大序列号
      if(childNodeName.startsWith(DELEGATION_TOKEN_SEQUENCE_NUMBER_PREFIX)) {
        rmState.rmSecretManagerState.dtSequenceNumber =
            Integer.parseInt(childNodeName.split("_")[1]);
        continue;
      }

      // 读取文件内容
      Path childNodePath = getNodePath(rmDTSecretManagerRoot, childNodeName);
      byte[] childData = readFileWithRetries(childNodePath,
          childNode