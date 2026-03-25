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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSupport;
import org.apache.hadoop.util.JsonSerialization;

/**
 * 文件级注释：
 * Manifest提交器作业成功标记文件_SUCCESS的数据模型，保存作业提交的结果摘要信息。
 * 兼容S3A提交器的成功数据格式，便于下游模块统一解析不同提交器生成的成功标记文件。
 * JSON格式保持跨版本兼容性，Java序列化仅保证相同二进制版本间的兼容性。
 */
@SuppressWarnings({"unused", "CollectionDeclaredAsConcreteClass"})
@InterfaceAudience.Public
@InterfaceStability.Unstable
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ManifestSuccessData
    extends AbstractManifestData<ManifestSuccessData> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ManifestSuccessData.class);

  /**
   * 当前支持的数据格式版本，版本变更会同步更新序列化ID避免反序列化错误。
   */
  public static final int VERSION = 1;

  /**
   * 序列化ID，与版本号绑定保证兼容性。
   */
  private static final long serialVersionUID = 4755993198698104084L + VERSION;

  /**
   * 持久化数据中的类型标识，用于区分其他类型的清单文件。
   */
  public static final String NAME
      = "org.apache.hadoop.fs.s3a.commit.files.SuccessData/" + VERSION;

  /** 数据标识名称。 */
  private String name;

  /** 文件创建时间戳。 */
  private long timestamp;

  /** 
   * 作业是否成功标记，_SUCCESS文件中默认为true，
   * 保存到日志目录时可根据实际结果修改。
   */
  private boolean success = true;

  /** 创建时间的日期字符串，不保证可解析。 */
  private String date;

  /** 创建该文件的主机名，即提交作业的节点。 */
  private String hostname;

  /** 提交器名称。 */
  private String committer;

  /** 描述文本。 */
  private String description;

  /** 作业ID，已知时保存。 */
  private String jobId = "";

  /** 作业ID来源。 */
  private String jobIdSource = "";

  /**
   * 指标数据，使用TreeMap保证序列化稳定性。
   */
  private TreeMap<String, Long> metrics = new TreeMap<>();

  /**
   * 诊断信息，使用TreeMap保证序列化稳定性。
   */
  private TreeMap<String, String> diagnostics = new TreeMap<>();

  /**
   * 本次提交涉及的所有文件路径。
   */
  private ArrayList<String> filenames = new ArrayList<>(0);

  /**
   * IO统计信息快照。
   */
  @JsonProperty("iostatistics")
  private IOStatisticsSnapshot iostatistics = new IOStatisticsSnapshot();

  /** 作业状态：已提交、已中止等。 */
  private String state;

  /** 最后执行的提交阶段。 */
  private String stage;

  /**
   * 验证加载的成功数据格式兼容性。
   * @return 验证通过的当前实例
   * @throws IOException 格式不兼容时抛出
   */
  @Override
  public ManifestSuccessData validate() throws IOException {
    verify(name != null,
        "Incompatible file format: no 'name' field");
    verify(NAME.equals(name),
        "Incompatible file format: " + name);
    return this;
  }

  /**
   * 创建当前类的JSON序列化器。
   * @return JSON序列化器实例
   */
  @Override
  public JsonSerialization<ManifestSuccessData> createSerializer() {
    return serializer();
  }

  /**
   * 将当前实例序列化为字节数组。
   * @return 序列化后的字节数组
   * @throws IOException 序列化失败时抛出
   */
  @Override
  public byte[] toBytes() throws IOException {
    return serializer().toBytes(this);
  }

  /**
   * 将当前实例序列化为JSON字符串。
   * @return JSON字符串
   * @throws IOException 序列化失败时抛出
   */
  public String toJson() throws IOException {
    return serializer().toJson(this);
  }

  /**
   * 将当前成功数据保存到指定路径。
   * @param fs 文件系统
   * @param path 保存路径
   * @param overwrite 是否覆盖已有文件
   * @throws IOException 保存失败时抛出
   */
  @Override
  public void save(FileSystem fs, Path path, boolean overwrite)
      throws IOException {
    // 保存前设置正确的名称标识
    name = NAME;
    serializer().save(fs, path, this, overwrite);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "ManifestSuccessData{");
    sb.append("committer='").append(committer).append('\'');
    sb.append(", hostname='").append(hostname).append('\'');
    sb.append(", description='").append(description).append('\'');
    sb.append(", date='").append(date).append('\'');
    sb.append(", filenames=[").append(
        StringUtils.join(filenames, ", "))
        .append("]");
    sb.append('}');
    return sb.toString();
  }

  /**
   * 将指标信息格式化输出为字符串，按键排序。
   * @param prefix 每个条目前缀
   * @param middle 键值之间分隔符
   * @param suffix 每个条目后缀
   * @return 格式化后的指标字符串
   */
  public String dumpMetrics(String prefix, String middle, String suffix) {
    return joinMap(metrics, prefix, middle, suffix);
  }

  /**
   * 将诊断信息格式化输出为字符串，按键排序。
   * @param prefix 每个条目前缀
   * @param middle 键值之间分隔符
   * @param suffix 每个条目后缀
   * @return 格式化后的诊断信息字符串
   */
  public String dumpDiagnostics(String prefix, String middle, String suffix) {
    return joinMap(diagnostics, prefix, middle, suffix);
  }

  /**
   * 将键值对映射按key排序后拼接为字符串，用于日志输出。
   * @param map 待拼接的映射表
   * @param prefix 每个条目前缀
   * @param middle 键值之间分隔符
   * @param suffix 每个条目后缀
   * @return 拼接后的字符串
   */
  protected static String joinMap(Map<String, ?> map,
      String prefix,
      String middle, String suffix) {
    if (map == null) {
      return "";
    }
    List<String> list = new ArrayList<>(map.keySet());
    Collections.sort(list);
    StringBuilder sb = new StringBuilder(list.size() * 32);
    for (String k : list) {
      sb.append(prefix)
          .append(k)
          .append(middle)
          .append(map.get(k))
          .append(suffix);
    }
    return sb.toString();
  }

  /**
   * 从文件加载成功数据并验证格式兼容性。
   * @param fs 文件系统
   * @param path 文件路径
   * @return 加载验证后的实例
   * @throws IOException IO错误或格式不兼容时抛出
   */
  public static ManifestSuccessData load(FileSystem fs, Path path)
      throws IOException {
    LOG.debug("Reading success data from {}", path);
    ManifestSuccessData instance = serializer().load(fs, path);
    instance.validate();
    return instance;
  }

  /**
   * 获取当前类的JSON序列化器实例。
   * @return JSON序列化器
   */
  public static JsonSerialization<ManifestSuccessData> serializer() {
    return new JsonSerialization<>(ManifestSuccessData.class, false, true);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  /** @return 创建时间戳。 */
  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  /** @return 创建日期字符串，不保证可解析。 */
  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  /**
   * @return 创建该文件的主机名，即提交作业的节点。
   */
  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  /**
   * @return 提交器名称。
   */
  public String getCommitter() {
    return committer;
  }

  public void setCommitter(String committer) {
    this.committer = committer;
  }

  /**
   * @return 描述文本。
   */
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * @return 指标映射表。
   */
  public Map<String, Long> getMetrics() {
    return metrics;
  }

  public void setMetrics(TreeMap<String, Long> metrics) {
    this.metrics = metrics;
  }

  /**
   * @return 本次提交的文件路径列表。
   */
  public List<String> getFilenames() {
    return filenames;
  }

  /**
   * 获取文件路径列表，转换为Path对象。
   * @return 转换后的Path列表
   */
  @JsonIgnore
  public List<Path> getFilenamePaths() {
    return getFilenames().stream()
        .map(AbstractManifestData::unmarshallPath)
        .collect(Collectors.toList());
  }

  /**
   * 设置文件路径列表，从Path对象转换为字符串保存。
   */
  @JsonIgnore
  public void setFilenamePaths(List<Path> paths) {
    setFilenames(new ArrayList<>(
        paths.stream()
            .map(AbstractManifestData::marshallPath)
            .collect(Collectors.toList())));
  }

  public void setFilenames(ArrayList<String> filenames) {
    this.filenames = filenames;
  }

  public Map<String, String> getDiagnostics() {
    return diagnostics;
  }

  public void setDiagnostics(TreeMap<String, String> diagnostics) {
    this.diagnostics = diagnostics;
  }

  /**
   * 添加一条诊断信息。
   * @param key 诊断键名
   * @param value 诊断值
   */
  public void putDiagnostic(String key, String value) {
    diagnostics.put(key, value);
  }

  /** @return 作业ID，未知时返回空字符串。 */
  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public String getJobIdSource() {
    return jobIdSource;
  }

  public void setJobIdSource(final String jobIdSource) {
    this.jobIdSource = jobIdSource;
  }

  @Override
  public IOStatisticsSnapshot getIOStatistics() {
    return iostatistics;
  }

  public void setIOStatistics(final IOStatisticsSnapshot ioStatistics) {
    this.iostatistics = ioStatistics;
  }

  /**
   * 将传入的IO统计信息生成快照保存。
   * @param iostats 源IO统计信息，可为null
   */
  public void snapshotIOStatistics(IOStatistics iostats) {
    setIOStatistics(IOStatisticsSupport.snapshotIOStatistics(iostats));
  }

  /**
   * 设置作业成功标记。
   * @param success 作业是否成功
   */
  public void setSuccess(boolean success) {
    this.success = success;
  }

  /**
   * 获取作业成功标记。
   * @return 作业是否成功
   */
  public boolean getSuccess() {
    return success;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getStage() {
    return stage;
  }

  /**
   * 记录作业失败信息，设置成功标记为false，并将异常信息保存到诊断信息中。
   * @param thrown 失败抛出的异常
   */
  public void recordJobFailure(Throwable thrown) {
    setSuccess(false);
    String stacktrace = ExceptionUtils.getStackTrace(thrown);
    diagnostics.put(DiagnosticKeys.EXCEPTION, thrown.toString());
    diagnostics.put(DiagnosticKeys.STACKTRACE, stacktrace;
  }
}