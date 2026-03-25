// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.files;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.util.JsonSerialization;

/**
 * Task任务尝试生成的输出文件清单，记录该任务尝试产生的所有待提交文件和需要创建的目录信息。
 * 用于基于清单的输出提交器流程，在作业提交阶段统一处理所有任务输出。
 * 
 * 版本兼容说明：
 * 集群滚动升级时，新版本节点生成的清单需要被旧版本作业提交器处理，
 * 向后兼容的修改可以直接保留JSON反序列化能力；不兼容修改必须更新VERSION常量，
 * 避免加载错误格式的清单导致作业失败。
 */
@SuppressWarnings("unused")
@InterfaceAudience.Private
@InterfaceStability.Unstable
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TaskManifest extends AbstractManifestData<TaskManifest> {

  /**
   * 当前清单格式版本，不兼容修改时必须更新。
   */
  public static final int VERSION = 1;

  /**
   * 清单类型标识，包含完整类名和版本号，用于反序列化时校验。
   */
  public static final String TYPE =
      "org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest/"
      + VERSION;

  private static final Logger LOG =
      LoggerFactory.getLogger(TaskManifest.class);

  /**
   * 序列化版本ID，随版本变化更新。
   */
  private static final long serialVersionUID = 7090285511966046094L + VERSION;

  /**
   * 清单类型，用于反序列化校验。
   */
  @JsonProperty("type")
  private String type = TYPE;

  /**
   * 版本标记，用于反序列化兼容校验。
   */
  @JsonProperty("version")
  private int version = VERSION;

  /**
   * 作业ID，同一次作业的所有任务尝试共享该ID。
   */
  @JsonProperty("jobId")
  private String jobId;

  /**
   * 作业尝试编号，从0开始计数。
   */
  @JsonProperty("jobAttemptNumber")
  private int jobAttemptNumber;

  /**
   * 任务ID。
   */
  @JsonProperty("taskID")
  private String taskID;

  /**
   * 任务尝试ID。
   */
  @JsonProperty("taskAttemptID")
  private String taskAttemptID;

  /**
   * 任务尝试的工作目录路径。
   */
  @JsonProperty("taskAttemptDir")
  private String taskAttemptDir;

  /**
   * 待提交文件列表，每个条目包含源路径、目标路径、文件大小等信息。
   */
  @JsonProperty("files")
  private final List<FileEntry> filesToCommit = new ArrayList<>();

  /**
   * 需要在输出路径创建的目录列表，所有目录必须在文件提交前创建完成。
   */
  @JsonProperty("directories")
  private final List<DirEntry> destDirectories = new ArrayList<>();

  /**
   * 提交器可扩展自定义数据的预留字段。
   */
  private final Map<String, String> extraData = new HashMap<>(0);

  /**
   * IO操作统计信息快照。
   */
  @JsonProperty("iostatistics")
  private IOStatisticsSnapshot iostatistics = new IOStatisticsSnapshot();

  /**
   * 空构造函数，供Jackson反序列化和业务代码调用。
   */
  public TaskManifest() {
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  @Override
  public IOStatisticsSnapshot getIOStatistics() {
    return iostatistics;
  }

  public void setIOStatistics(
      @Nullable final IOStatisticsSnapshot ioStatistics) {
    this.iostatistics = ioStatistics;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(final String jobId) {
    this.jobId = jobId;
  }

  public int getJobAttemptNumber() {
    return jobAttemptNumber;
  }

  public void setJobAttemptNumber(final int jobAttemptNumber) {
    this.jobAttemptNumber = jobAttemptNumber;
  }

  public String getTaskID() {
    return taskID;
  }

  public void setTaskID(final String taskID) {
    this.taskID = taskID;
  }

  public String getTaskAttemptID() {
    return taskAttemptID;
  }

  public void setTaskAttemptID(final String taskAttemptID) {
    this.taskAttemptID = taskAttemptID;
  }

  public String getTaskAttemptDir() {
    return taskAttemptDir;
  }

  public void setTaskAttemptDir(final String taskAttemptDir) {
    this.taskAttemptDir = taskAttemptDir;
  }

  /**
   * 添加一个待提交文件到清单。
   * @param entry 待添加的文件条目
   */
  public void addFileToCommit(FileEntry entry) {
    filesToCommit.add(entry);
  }

  public List<FileEntry> getFilesToCommit() {
    return filesToCommit;
  }

  /**
   * 计算所有待提交文件的总大小。
   * @return 总字节数
   */
  @JsonIgnore
  public long getTotalFileSize() {
    return filesToCommit.stream().mapToLong(FileEntry::getSize).sum();
  }

  /**
   * 获取所有需要创建的目标目录列表。
   * @return 目录条目列表
   */
  public List<DirEntry> getDestDirectories() {
    return destDirectories;
  }

  /**
   * 添加一个需要创建的目标目录到清单。
   * @param entry 待添加的目录条目
   */
  public void addDirectory(DirEntry entry) {
    destDirectories.add(entry);
  }

  public Map<String, String> getExtraData() {
    return extraData;
  }

  @Override
  public byte[] toBytes() throws IOException {
    return serializer().toBytes(this);
  }

  /**
   * 将清单序列化为JSON字符串。
   * @return JSON字符串
   * @throws IOException 序列化失败
   */
  public String toJson() throws IOException {
    return serializer().toJson(this);
  }

  @Override
  public void save(FileSystem fs, Path path, boolean overwrite)
      throws IOException {
    serializer().save(fs, path, this, overwrite);
  }

  /**
   * 校验清单数据的完整性和合法性，包括类型版本校验、条目类型校验、路径冲突检测。
   * @return 校验通过的本实例
   * @throws IOException 数据校验失败
   */
  public TaskManifest validate() throws IOException {
    verify(TYPE.equals(type), "Wrong type: %s", type);
    verify(version == VERSION, "Wrong version: %s", version);
    validateCollectionClass(extraData.keySet(), String.class);
    validateCollectionClass(extraData.values(), String.class);
    Set<String> destinations = new HashSet<>(filesToCommit.size());
    validateCollectionClass(filesToCommit, FileEntry.class);
    for (FileEntry c : filesToCommit) {
      c.validate();
      verify(!destinations.contains(c.getDest()),
          "Destination %s is written to by more than one pending commit",
          c.getDest());
      destinations.add(c.getDest());
    }
    return this;
  }

  /**
   * 创建当前类的JSON序列化器。
   * @return JSON序列化器实例
   */
  @Override
  public JsonSerialization<TaskManifest> createSerializer() {
    return serializer();
  }

  /**
   * 创建TaskManifest的JSON序列化器。
   * @return JSON序列化器实例
   */
  public static JsonSerialization<TaskManifest> serializer() {
    return new JsonSerialization<>(TaskManifest.class, false, true);
  }

  /**
   * 从文件加载TaskManifest并校验。
   * @param fs 文件系统
   * @param path 清单文件路径
   * @return 加载并校验完成的清单实例
   * @throws IOException IO错误或数据校验失败
   */
  public static TaskManifest load(FileSystem fs, Path path)
      throws IOException {
    LOG.debug("Reading Manifest in file {}", path);
    return serializer().load(fs, path).validate();
  }

  /**
   * 从文件加载TaskManifest并校验，支持传入预获取的FileStatus减少IO调用。
   * @param serializer 序列化器实例
   * @param fs 文件系统
   * @param path 清单文件路径
   * @param status 预获取的文件状态信息
   * @return 加载并校验完成的清单实例
   * @throws IOException IO错误或数据校验失败
   */
  public static TaskManifest load(
      JsonSerialization<TaskManifest> serializer,
      FileSystem fs,
      Path path,
      FileStatus status)
      throws IOException {
    LOG.debug("Reading Manifest in file {}", path);
    return serializer.load(fs, path, status)
        .validate();
  }

}