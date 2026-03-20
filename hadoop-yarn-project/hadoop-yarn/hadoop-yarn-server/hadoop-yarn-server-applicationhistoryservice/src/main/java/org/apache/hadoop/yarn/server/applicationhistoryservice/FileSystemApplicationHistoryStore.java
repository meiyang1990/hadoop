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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProto;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ApplicationAttemptStartDataProto;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ApplicationFinishDataProto;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ApplicationStartDataProto;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ContainerFinishDataProto;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ContainerStartDataProto;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.impl.pb.ApplicationAttemptFinishDataPBImpl;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.impl.pb.ApplicationAttemptStartDataPBImpl;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.impl.pb.ApplicationFinishDataPBImpl;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.impl.pb.ApplicationStartDataPBImpl;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.impl.pb.ContainerFinishDataPBImpl;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.impl.pb.ContainerStartDataPBImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;

import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于文件系统的应用历史存储实现。
 * 
 * 每个应用对应一个历史文件，包含该应用及其尝试和容器的所有历史数据。
 * 写入时，{@link #applicationStarted(ApplicationStartData)} 首先被调用并打开文件，
 * {@link #applicationFinished(ApplicationFinishData)} 最后被调用并关闭文件。
 */
@Public
@Unstable
public class FileSystemApplicationHistoryStore extends AbstractService
    implements ApplicationHistoryStore {

  private static final Logger LOG = LoggerFactory
      .getLogger(FileSystemApplicationHistoryStore.class);

  // 根目录名称
  private static final String ROOT_DIR_NAME = "ApplicationHistoryDataRoot";
  // TFile 最小块大小
  private static final int MIN_BLOCK_SIZE = 256 * 1024;
  // 启动数据后缀
  private static final String START_DATA_SUFFIX = "_start";
  // 结束数据后缀
  private static final String FINISH_DATA_SUFFIX = "_finish";
  // 根目录权限
  private static final FsPermission ROOT_DIR_UMASK = FsPermission
    .createImmutable((short) 0740);
  // 历史文件权限
  private static final FsPermission HISTORY_FILE_UMASK = FsPermission
    .createImmutable((short) 0640);

  // 文件系统实例
  private FileSystem fs;
  // 根目录路径
  private Path rootDirPath;

  // 正在写入的历史文件映射（应用 ID -> 写入器）
  private ConcurrentMap<ApplicationId, HistoryFileWriter> outstandingWriters =
      new ConcurrentHashMap<ApplicationId, HistoryFileWriter>();

  public FileSystemApplicationHistoryStore() {
    super(FileSystemApplicationHistoryStore.class.getName());
  }

  protected FileSystem getFileSystem(Path path, Configuration conf) throws Exception {
    return path.getFileSystem(conf);
  }

  /**
   * 启动服务：初始化文件系统和根目录
   */
  @Override
  public void serviceStart() throws Exception {
    Configuration conf = getConfig();
    Path fsWorkingPath =
        new Path(conf.get(YarnConfiguration.FS_APPLICATION_HISTORY_STORE_URI,
            conf.get("hadoop.tmp.dir") + "/yarn/timeline/generic-history"));
    rootDirPath = new Path(fsWorkingPath, ROOT_DIR_NAME);
    try {
      fs = getFileSystem(fsWorkingPath, conf);
      fs.mkdirs(rootDirPath, ROOT_DIR_UMASK);
    } catch (IOException e) {
      LOG.error("Error when initializing FileSystemHistoryStorage", e);
      throw e;
    }
    super.serviceStart();
  }

  /**
   * 停止服务：关闭所有打开的写入器和文件系统
   */
  @Override
  public void serviceStop() throws Exception {
    try {
      for (Entry<ApplicationId, HistoryFileWriter> entry : outstandingWriters
        .entrySet()) {
        entry.getValue().close();
      }
      outstandingWriters.clear();
    } finally {
      IOUtils.cleanupWithLogger(LOG, fs);
    }
    super.serviceStop();
  }

  /**
   * 获取指定应用的历史数据
   */
  @Override
  public ApplicationHistoryData getApplication(ApplicationId appId)
      throws IOException {
    HistoryFileReader hfReader = getHistoryFileReader(appId);
    try {
      boolean readStartData = false;
      boolean readFinishData = false;
      ApplicationHistoryData historyData =
          ApplicationHistoryData.newInstance(appId, null, null, null, null,
            Long.MIN_VALUE, Long.MIN_VALUE, Long.MAX_VALUE, null,
            FinalApplicationStatus.UNDEFINED, null);
      // 遍历历史文件，查找启动和结束数据
      while ((!readStartData || !readFinishData) && hfReader.hasNext()) {
        HistoryFileReader.Entry entry = hfReader.next();
        if (entry.key.id.equals(appId.toString())) {
          if (entry.key.suffix.equals(START_DATA_SUFFIX)) {
            ApplicationStartData startData =
                parseApplicationStartData(entry.value);
            mergeApplicationHistoryData(historyData, startData);
            readStartData = true;
          } else if (entry.key.suffix.equals(FINISH_DATA_SUFFIX)) {
            ApplicationFinishData finishData =
                parseApplicationFinishData(entry.value);
            mergeApplicationHistoryData(historyData, finishData);
            readFinishData = true;
          }
        }
      }
      if (!readStartData && !readFinishData) {
        return null;
      }
      if (!readStartData) {
        LOG.warn("Start information is missing for application " + appId);
      }
      if (!readFinishData) {
        LOG.warn("Finish information is missing for application " + appId);
      }
      LOG.info("Completed reading history information of application " + appId);
      return historyData;
    } catch (IOException e) {
      LOG.error("Error when reading history file of application " + appId, e);
      throw e;
    } finally {
      hfReader.close();
    }
  }

  /**
   * 获取所有应用的历史数据
   */
  @Override
  public Map<ApplicationId, ApplicationHistoryData> getAllApplications()
      throws IOException {
    Map<ApplicationId, ApplicationHistoryData> historyDataMap =
        new HashMap<ApplicationId, ApplicationHistoryData>();
    FileStatus[] files = fs.listStatus(rootDirPath);
    for (FileStatus file : files) {
      ApplicationId appId =
          ApplicationId.fromString(file.getPath().getName());
      try {
        ApplicationHistoryData historyData = getApplication(appId);
        if (historyData != null) {
          historyDataMap.put(appId, historyData);
        }
      } catch (IOException e) {
        // 忽略异常，继续处理下一个应用
        LOG.error("History information of application " + appId
            + " is not included into the result due to the exception", e);
      }
    }
    return historyDataMap;
  }

  /**
   * 获取指定应用的所有尝试历史数据
   */
  @Override
  public Map<ApplicationAttemptId, ApplicationAttemptHistoryData>
      getApplicationAttempts(ApplicationId appId) throws IOException {
    Map<ApplicationAttemptId, ApplicationAttemptHistoryData> historyDataMap =
        new HashMap<ApplicationAttemptId, ApplicationAttemptHistoryData>();
    HistoryFileReader hfReader = getHistoryFileReader(appId);
    try {
      while (hfReader.hasNext()) {
        HistoryFileReader.Entry entry = hfReader.next();
        if (entry.key.id.startsWith(
            ConverterUtils.APPLICATION_ATTEMPT_PREFIX)) {
          ApplicationAttemptId appAttemptId = ApplicationAttemptId.fromString(
              entry.key.id);
          if (appAttemptId.getApplicationId().equals(appId)) {
            ApplicationAttemptHistoryData historyData = 
                historyDataMap.get(appAttemptId);
            if (historyData == null) {
              historyData = ApplicationAttemptHistoryData.newInstance(
                  appAttemptId, null, -1, null, null, null,
                  FinalApplicationStatus.UNDEFINED, null);
              historyDataMap.put(appAttemptId, historyData);
            }
            if (entry.key.suffix.equals(START_DATA_SUFFIX)) {
              mergeApplicationAttemptHistoryData(historyData,
                  parseApplicationAttemptStartData(entry.value));
            } else if (entry.key.suffix.equals(FINISH_DATA_SUFFIX)) {
              mergeApplicationAttemptHistoryData(historyData,
                  parseApplicationAttemptFinishData(entry.value));
            }
          }
        }
      }
      LOG.info("Completed reading history information of all application"
          + " attempts of application " + appId);
    } catch (IOException e) {
      LOG.info("Error when reading history information of some application"
          + " attempts of application " + appId);
    } finally {
      hfReader.close();
    }
    return historyDataMap;
  }

  /**
   * 获取指定应用尝试的历史数据
   */
  @Override
  public ApplicationAttemptHistoryData getApplicationAttempt(
      ApplicationAttemptId appAttemptId) throws IOException {
    HistoryFileReader hfReader =
        getHistoryFileReader(appAttemptId.getApplicationId());
    try {
      boolean readStartData = false;
      boolean readFinishData = false;
      ApplicationAttemptHistoryData historyData =
          ApplicationAttemptHistoryData.newInstance(appAttemptId, null, -1,
            null, null, null, FinalApplicationStatus.UNDEFINED, null);
      while ((!readStartData || !readFinishData) && hfReader.hasNext()) {
        HistoryFileReader.Entry entry = hfReader.next();
        if (entry.key.id.equals(appAttemptId.toString())) {
          if (entry.key.suffix.equals(START_DATA_SUFFIX)) {
            ApplicationAttemptStartData startData =
                parseApplicationAttemptStartData(entry.value);
            mergeApplicationAttemptHistoryData(historyData, startData);
            readStartData = true;
          } else if (entry.key.suffix.equals(FINISH_DATA_SUFFIX)) {
            ApplicationAttemptFinishData finishData =
                parseApplicationAttemptFinishData(entry.value);
            mergeApplicationAttemptHistoryData(historyData, finishData);
            readFinishData = true;
          }
        }
      }
      if (!readStartData && !readFinishData) {
        return null;
      }
      if (!readStartData) {
        LOG.warn("Start information is missing for application attempt "
            + appAttemptId);
      }
      if (!readFinishData) {
        LOG.warn("Finish information is missing for application attempt "
            + appAttemptId);
      }
      LOG.info("Completed reading history information of application attempt "
          + appAttemptId);
      return historyData;
    } catch (IOException e) {
      LOG.error("Error when reading history file of application attempt"
          + appAttemptId, e);
      throw e;
    } finally {
      hfReader.close();
    }
  }

  /**
   * 获取指定容器的历史数据
   */
  @Override
  public ContainerHistoryData getContainer(ContainerId containerId)
      throws IOException {
    HistoryFileReader hfReader =
        getHistoryFileReader(containerId.getApplicationAttemptId()
          .getApplicationId());
    try {
      boolean readStartData = false;
      boolean readFinishData = false;
      ContainerHistoryData historyData =
          ContainerHistoryData
            .newInstance(containerId, null, null, null, Long.MIN_VALUE,
              Long.MAX_VALUE, null, Integer.MAX_VALUE, null);
      while ((!readStartData || !readFinishData) && hfReader.hasNext()) {
        HistoryFileReader.Entry entry = hfReader.next();
        if (entry.key.id.equals(containerId.toString())) {
          if (entry.key.suffix.equals(START_DATA_SUFFIX)) {
            ContainerStartData startData = parseContainerStartData(entry.value);
            mergeContainerHistoryData(historyData, startData);
            readStartData = true;
          } else if (entry.key.suffix.equals(FINISH_DATA_SUFFIX)) {
            ContainerFinishData finishData =
                parseContainerFinishData(entry.value);
            mergeContainerHistoryData(historyData, finishData);
            readFinishData = true;
          }
        }
      }
      if (!readStartData && !readFinishData) {
        return null;
      }
      if (!readStartData) {
        LOG.warn("Start information is missing for container " + containerId);
      }
      if (!readFinishData) {
        LOG.warn("Finish information is missing for container " + containerId);
      }
      LOG.info("Completed reading history information of container "
          + containerId);
      return historyData;
    } catch (IOException e) {
      LOG.error("Error when reading history file of container " + containerId, e);
      throw e;
    } finally {
      hfReader.close();
    }
  }

  /**
   * 获取指定应用尝试的 AM 容器历史数据
   */
  @Override
  public ContainerHistoryData getAMContainer(ApplicationAttemptId appAttemptId)
      throws IOException {
    ApplicationAttemptHistoryData attemptHistoryData =
        getApplicationAttempt(appAttemptId);
    if (attemptHistoryData == null
        || attemptHistoryData.getMasterContainerId() == null) {
      return null;
    }
    return getContainer(attemptHistoryData.getMasterContainerId());
  }

  /**
   * 获取指定应用尝试的所有容器历史数据
   */
  @Override
  public Map<ContainerId, ContainerHistoryData> getContainers(
      ApplicationAttemptId appAttemptId) throws IOException {
    Map<ContainerId, ContainerHistoryData> historyDataMap =
        new HashMap<ContainerId, ContainerHistoryData>();
    HistoryFileReader hfReader =
        getHistoryFileReader(appAttemptId.getApplicationId());
    try {
      while (hfReader.hasNext()) {
        HistoryFileReader.Entry entry = hfReader.next();
        if (entry.key.id.startsWith(ConverterUtils.CONTAINER_PREFIX)) {
          ContainerId containerId =
              ContainerId.fromString(entry.key.id);
          if (containerId.getApplicationAttemptId().equals(appAttemptId)) {
            ContainerHistoryData historyData =
                historyDataMap.get(containerId);
            if (historyData == null) {
              historyData = ContainerHistoryData.newInstance(
                  containerId, null, null, null, Long.MIN_VALUE,
                  Long.MAX_VALUE, null, Integer.MAX_VALUE, null);
              historyDataMap.put(containerId, historyData);
            }
            if (entry.key.suffix.equals(START_DATA_SUFFIX)) {
              mergeContainerHistoryData(historyData,
                  parseContainerStartData(entry.value));
            } else if (entry.key.suffix.equals(FINISH_DATA_SUFFIX)) {
              mergeContainerHistoryData(historyData,
                  parseContainerFinishData(entry.value));
            }
          }
        }
      }
      LOG.info("Completed reading history information of all containers"
          + " of application attempt " + appAttemptId);
    } catch (IOException e) {
      LOG.info("Error when reading history information of some containers"
          + " of application attempt " + appAttemptId);
    } finally {
      hfReader.close();
    }
    return historyDataMap;
  }

  /**
   * 写入应用启动数据，同时打开历史文件
   */
  @Override
  public void applicationStarted(ApplicationStartData appStart)
      throws IOException {
    HistoryFileWriter hfWriter =
        outstandingWriters.get(appStart.getApplicationId());
    if (hfWriter == null) {
      Path applicationHistoryFile =
          new Path(rootDirPath, appStart.getApplicationId().toString());
      try {
        hfWriter = new HistoryFileWriter(applicationHistoryFile);
        LOG.info("Opened history file of application "
            + appStart.getApplicationId());
      } catch (IOException e) {
        LOG.error("Error when openning history file of application "
            + appStart.getApplicationId(), e);
        throw e;
      }
      outstandingWriters.put(appStart.getApplicationId(), hfWriter);
    } else {
      throw new IOException("History file of application "
          + appStart.getApplicationId() + " is already opened");
    }
    assert appStart instanceof ApplicationStartDataPBImpl;
    try {
      hfWriter.writeHistoryData(new HistoryDataKey(appStart.getApplicationId()
        .toString(), START_DATA_SUFFIX),
        ((ApplicationStartDataPBImpl) appStart).getProto().toByteArray());
      LOG.info("Start information of application "
          + appStart.getApplicationId() + " is written");
    } catch (IOException e) {
      LOG.error("Error when writing start information of application "
          + appStart.getApplicationId(), e);
      throw e;
    }
  }

  /**
   * 写入应用结束数据，同时关闭历史文件
   */
  @Override
  public void applicationFinished(ApplicationFinishData appFinish)
      throws IOException {
    HistoryFileWriter hfWriter =
        getHistoryFileWriter(appFinish.getApplicationId());
    assert appFinish instanceof ApplicationFinishDataPBImpl;
    try {
      hfWriter.writeHistoryData(new HistoryDataKey(appFinish.getApplicationId()
        .toString(), FINISH_DATA_SUFFIX),
        ((ApplicationFinishDataPBImpl) appFinish).getProto().toByteArray());
      LOG.info("Finish information of application "
          + appFinish.getApplicationId() + " is written");
    } catch (IOException e) {
      LOG.error("Error when writing finish information of application "
          + appFinish.getApplicationId(), e);
      throw e;
    } finally {
      hfWriter.close();
      outstandingWriters.remove(appFinish.getApplicationId());
    }
  }

  /**
   * 写入应用尝试启动数据
   */
  @Override
  public void applicationAttemptStarted(
      ApplicationAttemptStartData appAttemptStart) throws IOException {
    HistoryFileWriter hfWriter =
        getHistoryFileWriter(appAttemptStart.getApplicationAttemptId()
          .getApplicationId());
    assert appAttemptStart instanceof ApplicationAttemptStartDataPBImpl;
    try {
      hfWriter.writeHistoryData(new HistoryDataKey(appAttemptStart
        .getApplicationAttemptId().toString(), START_DATA_SUFFIX),
        ((ApplicationAttemptStartDataPBImpl) appAttemptStart).getProto()
          .toByteArray());
      LOG.info("Start information of application attempt "
          + appAttemptStart.getApplicationAttemptId() + " is written");
    } catch (IOException e) {
      LOG.error("Error when writing start information of application attempt "
          + appAttemptStart.getApplicationAttemptId(), e);
      throw e;
    }
  }

  /**
   * 写入应用尝试结束数据
   */
  @Override
  public void applicationAttemptFinished(
      ApplicationAttemptFinishData appAttemptFinish) throws IOException {
    HistoryFileWriter hfWriter =
        getHistoryFileWriter(appAttemptFinish.getApplicationAttemptId()
          .getApplicationId());
    assert appAttemptFinish instanceof ApplicationAttemptFinishDataPBImpl;
    try {
      hfWriter.writeHistoryData(new HistoryDataKey(appAttemptFinish
        .getApplicationAttemptId().toString(), FINISH_DATA_SUFFIX),
        ((ApplicationAttemptFinishDataPBImpl) appAttemptFinish).getProto()
          .toByteArray());
      LOG.info("Finish information of application attempt "
          + appAttemptFinish.getApplicationAttemptId() + " is written");
    } catch (IOException e) {
      LOG.error("Error when writing finish information of application attempt "
          + appAttemptFinish.getApplicationAttemptId(), e);
      throw e;
    }
  }

  /**
   * 写入容器启动数据
   */
  @Override
  public void containerStarted(ContainerStartData containerStart)
      throws IOException {
    HistoryFileWriter hfWriter =
        getHistoryFileWriter(containerStart.getContainerId()
          .getApplicationAttemptId().getApplicationId());
    assert containerStart instanceof ContainerStartDataPBImpl;
    try {
      hfWriter.writeHistoryData(new HistoryDataKey(containerStart
        .getContainerId().toString(), START_DATA_SUFFIX),
        ((ContainerStartDataPBImpl) containerStart).getProto().toByteArray());
      LOG.info("Start information of container "
          + containerStart.getContainerId() + " is written");
    } catch (IOException e) {
      LOG.error("Error when writing start information of container "
          + containerStart.getContainerId(), e);
      throw e;
    }
  }

  /**
   * 写入容器结束数据
   */
  @Override
  public void containerFinished(ContainerFinishData containerFinish)
      throws IOException {
    HistoryFileWriter hfWriter =
        getHistoryFileWriter(containerFinish.getContainerId()
          .getApplicationAttemptId().getApplicationId());
    assert containerFinish instanceof ContainerFinishDataPBImpl;
    try {
      hfWriter.writeHistoryData(new HistoryDataKey(containerFinish
        .getContainerId().toString(), FINISH_DATA_SUFFIX),
        ((ContainerFinishDataPBImpl) containerFinish).getProto().toByteArray());
      LOG.info("Finish information of container "
          + containerFinish.getContainerId() + " is written");
    } catch (IOException e) {
      LOG.error("Error when writing finish information of container "
          + containerFinish.getContainerId(), e);
    }
  }

  // ========== Protocol Buffer 解析方法 ==========

  private static ApplicationStartData parseApplicationStartData(byte[] value)
      throws InvalidProtocolBufferException {
    return new ApplicationStartDataPBImpl(
      ApplicationStartDataProto.parseFrom(value));
  }

  private static ApplicationFinishData parseApplicationFinishData(byte[] value)
      throws InvalidProtocolBufferException {
    return new ApplicationFinishDataPBImpl(
      ApplicationFinishDataProto.parseFrom(value));
  }

  private static ApplicationAttemptStartData parseApplicationAttemptStartData(
      byte[] value) throws InvalidProtocolBufferException {
    return new ApplicationAttemptStartDataPBImpl(
      ApplicationAttemptStartDataProto.parseFrom(value));
  }

  private static ApplicationAttemptFinishData
      parseApplicationAttemptFinishData(byte[] value)
          throws InvalidProtocolBufferException {
    return new ApplicationAttemptFinishDataPBImpl(
      ApplicationAttemptFinishDataProto.parseFrom(value));
  }

  private static ContainerStartData parseContainerStartData(byte[] value)
      throws InvalidProtocolBufferException {
    return new ContainerStartDataPBImpl(
      ContainerStartDataProto.parseFrom(value));
  }

  private static ContainerFinishData parseContainerFinishData(byte[] value)
      throws InvalidProtocolBufferException {
    return new ContainerFinishDataPBImpl(
      ContainerFinishDataProto.parseFrom(value));
  }

  // ========== 数据合并方法 ==========

  private static void mergeApplicationHistoryData(
      ApplicationHistoryData historyData, ApplicationStartData startData) {
    historyData.setApplicationName(startData.getApplicationName());
    historyData.setApplicationType(startData.getApplicationType());
    historyData.setQueue(startData.getQueue());
    historyData.setUser(startData.getUser());
    historyData.setSubmitTime(startData.getSubmitTime());
    historyData.setStartTime(startData.getStartTime());
  }

  private static void mergeApplicationHistoryData(
      ApplicationHistoryData historyData, ApplicationFinishData finishData) {
    historyData.setFinishTime(finishData.getFinishTime());
    historyData.setDiagnosticsInfo(finishData.getDiagnosticsInfo());
    historyData.setFinalApplicationStatus(finishData
      .getFinalApplicationStatus());
    historyData.setYarnApplicationState(finishData.getYarnApplicationState());
  }

  private static void mergeApplicationAttemptHistoryData(
      ApplicationAttemptHistoryData historyData,
      ApplicationAttemptStartData startData) {
    historyData.setHost(startData.getHost());
    historyData.setRPCPort(startData.getRPCPort());
    historyData.setMasterContainerId(startData.getMasterContainerId());
  }

  private static void mergeApplicationAttemptHistoryData(
      ApplicationAttemptHistoryData historyData,
      ApplicationAttemptFinishData finishData) {
    historyData.setDiagnosticsInfo(finishData.getDiagnosticsInfo());
    historyData.setTrackingURL(finishData.getTrackingURL());
    historyData.setFinalApplicationStatus(finishData
      .getFinalApplicationStatus());
    historyData.setYarnApplicationAttemptState(finishData
      .getYarnApplicationAttemptState());
  }

  private static void mergeContainerHistoryData(
      ContainerHistoryData historyData, ContainerStartData startData) {
    historyData.setAllocatedResource(startData.getAllocatedResource());
    historyData.setAssignedNode(startData.getAssignedNode());
    historyData.setPriority(startData.getPriority());
    historyData.setStartTime(startData.getStartTime());
  }

  private static void mergeContainerHistoryData(
      ContainerHistoryData historyData, ContainerFinishData finishData) {
    historyData.setFinishTime(finishData.getFinishTime());
    historyData.setDiagnosticsInfo(finishData.getDiagnosticsInfo());
    historyData.setContainerExitStatus(finishData.getContainerExitStatus());
    historyData.setContainerState(finishData.getContainerState());
  }

  /**
   * 获取指定应用的历史文件写入器
   */
  private HistoryFileWriter getHistoryFileWriter(ApplicationId appId)
      throws IOException {
    HistoryFileWriter hfWriter = outstandingWriters.get(appId);
    if (hfWriter == null) {
      throw new IOException("History file of application " + appId
          + " is not opened");
    }
    return hfWriter;
  }

  /**
   * 获取指定应用的历史文件读取器
   */
  private HistoryFileReader getHistoryFileReader(ApplicationId appId)
      throws IOException {
    Path applicationHistoryFile = new Path(rootDirPath, appId.toString());
    try {
      fs.getFileStatus(applicationHistoryFile);
    } catch (FileNotFoundException e) {
      throw (FileNotFoundException) new FileNotFoundException("History file for"
          + " application " + appId + " is not found: " + e).initCause(e);
    }
    // 如果文件正在写入，则不允许读取
    if (outstandingWriters.containsKey(appId)) {
      throw new IOException("History file for application " + appId
          + " is under writing");
    }
    return new HistoryFileReader(applicationHistoryFile);
  }

  /**
   * 历史文件读取器，基于 TFile 格式
   */
  private class HistoryFileReader {

    /** 历史文件条目 */
    private class Entry {
      private HistoryDataKey key;
      private byte[] value;

      public Entry(HistoryDataKey key, byte[] value) {
        this.key = key;
        this.value = value;
      }
    }

    private TFile.Reader reader;
    private TFile.Reader.Scanner scanner;
    FSDataInputStream fsdis;

    public HistoryFileReader(Path historyFile) throws IOException {
      fsdis = fs.open(historyFile);
      reader =
          new TFile.Reader(fsdis, fs.getFileStatus(historyFile).getLen(),
            getConfig());
      reset();
    }

    public boolean hasNext() {
      return !scanner.atEnd();
    }

    public Entry next() throws IOException {
      TFile.Reader.Scanner.Entry entry = scanner.entry();
      DataInputStream dis = entry.getKeyStream();
      HistoryDataKey key = new HistoryDataKey();
      key.readFields(dis);
      dis = entry.getValueStream();
      byte[] value = new byte[entry.getValueLength()];
      dis.read(value);
      scanner.advance();
      return new Entry(key, value);
    }

    public void reset() throws IOException {
      IOUtils.cleanupWithLogger(LOG, scanner);
      scanner = reader.createScanner();
    }

    public void close() {
      IOUtils.cleanupWithLogger(LOG, scanner, reader, fsdis);
    }

  }

  /**
   * 历史文件写入器，基于 TFile 格式
   */
  private class HistoryFileWriter {

    private FSDataOutputStream fsdos;
    private TFile.Writer writer;

    public HistoryFileWriter(Path historyFile) throws IOException {
      if (fs.exists(historyFile)) {
        fsdos = fs.append(historyFile);
      } else {
        fsdos = fs.create(historyFile);
      }
      try {
        fs.setPermission(historyFile, HISTORY_FILE_UMASK);
        writer =
            new TFile.Writer(fsdos, MIN_BLOCK_SIZE, getConfig().get(
                YarnConfiguration.FS_APPLICATION_HISTORY_STORE_COMPRESSION_TYPE,
                YarnConfiguration.DEFAULT_FS_APPLICATION_HISTORY_STORE_COMPRESSION_TYPE), null,
                getConfig());
      } catch (IOException e) {
        IOUtils.cleanupWithLogger(LOG, fsdos);
        throw e;
      }
    }

    public synchronized void close() {
      IOUtils.cleanupWithLogger(LOG, writer, fsdos);
    }

    /**
     * 写入历史数据条目
     */
    public synchronized void writeHistoryData(HistoryDataKey key, byte[] value)
        throws IOException {
      DataOutputStream dos = null;
      try {
        dos = writer.prepareAppendKey(-1);
        key.write(dos);
      } finally {
        IOUtils.cleanupWithLogger(LOG, dos);
      }
      try {
        dos = writer.prepareAppendValue(value.length);
        dos.write(value);
      } finally {
        IOUtils.cleanupWithLogger(LOG, dos);
      }
    }

  }

  /**
   * 历史数据键，包含实体 ID 和后缀（标识启动或结束数据）
   */
  private static class HistoryDataKey implements Writable {

    private String id;
    private String suffix;

    public HistoryDataKey() {
      this(null, null);
    }

    public HistoryDataKey(String id, String suffix) {
      this.id = id;
      this.suffix = suffix;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(id);
      out.writeUTF(suffix);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      id = in.readUTF();
      suffix = in.readUTF();
    }
  }
}
