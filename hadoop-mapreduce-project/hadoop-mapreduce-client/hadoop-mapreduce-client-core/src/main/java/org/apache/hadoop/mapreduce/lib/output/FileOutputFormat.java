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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;
import java.text.NumberFormat;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件输出格式的抽象基类，为所有基于HDFS文件系统的输出格式提供通用基础能力
 * 负责输出目录管理、输出压缩配置、临时输出路径生成等公共功能
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileOutputFormat<K, V> extends OutputFormat<K, V> {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileOutputFormat.class);

  /** 
   * 分区编号格式化工具，保证字典序排序后分区顺序和实际编号一致
   */
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  protected static final String BASE_OUTPUT_NAME = "mapreduce.output.basename";
  protected static final String PART = "part";
  static {
    // 格式化时最少保留5位数字，不足补零
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    // 关闭千分位分组
    NUMBER_FORMAT.setGroupingUsed(false);
  }
  private PathOutputCommitter committer = null;

  /** 配置项：是否开启输出压缩 */
  public static final String COMPRESS =
      "mapreduce.output.fileoutputformat.compress";

  /** 配置项：压缩编码器类名 */
  public static final String COMPRESS_CODEC =
      "mapreduce.output.fileoutputformat.compress.codec";
  /**
   * 配置项：压缩类型，值可为NONE、RECORD、BLOCK，主要用于SequenceFileOutputFormat
   */
  public static final String COMPRESS_TYPE =
      "mapreduce.output.fileoutputformat.compress.type";

  /** 配置项：作业输出目录配置项名称 */
  public static final String OUTDIR =
      "mapreduce.output.fileoutputformat.outputdir";

  @Deprecated
  public enum Counter {
    BYTES_WRITTEN
  }

  /**
   * 设置作业输出是否开启压缩
   * @param job 目标作业对象
   * @param compress 是否开启压缩
   */
  public static void setCompressOutput(Job job, boolean compress) {
    job.getConfiguration().setBoolean(FileOutputFormat.COMPRESS, compress);
  }
  
  /**
   * 获取作业输出是否开启压缩的配置
   * @param job 作业上下文对象
   * @return true表示开启压缩，false表示不开启
   */
  public static boolean getCompressOutput(JobContext job) {
    return job.getConfiguration().getBoolean(
      FileOutputFormat.COMPRESS, false);
  }
  
  /**
   * 设置作业输出压缩所使用的编码解码器
   * @param job 目标作业对象
   * @param codecClass 压缩编码解码器类
   */
  public static void 
  setOutputCompressorClass(Job job, 
                           Class<? extends CompressionCodec> codecClass) {
    setCompressOutput(job, true);
    job.getConfiguration().setClass(FileOutputFormat.COMPRESS_CODEC, 
                                    codecClass, 
                                    CompressionCodec.class);
  }
  
  /**
   * 获取作业输出压缩所使用的编码解码器
   * @param job 作业上下文对象
   * @param defaultValue 未配置时返回的默认编码器
   * @return 配置的压缩编码解码器类
   * @throws IllegalArgumentException 如果配置的类找不到则抛出异常
   */
  public static Class<? extends CompressionCodec> 
  getOutputCompressorClass(JobContext job, 
                       Class<? extends CompressionCodec> defaultValue) {
    Class<? extends CompressionCodec> codecClass = defaultValue;
    Configuration conf = job.getConfiguration();
    String name = conf.get(FileOutputFormat.COMPRESS_CODEC);
    if (name != null) {
      try {
        codecClass =
            conf.getClassByName(name).asSubclass(CompressionCodec.class);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Compression codec " + name + 
                                           " was not found.", e);
      }
    }
    return codecClass;
  }
  
  public abstract RecordWriter<K, V> 
     getRecordWriter(TaskAttemptContext job
                     ) throws IOException, InterruptedException;

  /**
   * 检查作业输出规范，验证输出目录合法性并获取委派令牌
   * @param job 作业上下文对象
   * @throws FileAlreadyExistsException 如果输出目录已存在抛出
   * @throws IOException 其他IO异常
   */
  public void checkOutputSpecs(JobContext job
                               ) throws FileAlreadyExistsException, IOException{
    // 获取输出目录，验证是否已配置
    Path outDir = getOutputPath(job);
    if (outDir == null) {
      throw new InvalidJobConfException("Output directory not set.");
    }

    // 获取输出目录对应文件系统的委派令牌，用于安全认证
    TokenCache.obtainTokensForNamenodes(job.getCredentials(),
        new Path[] { outDir }, job.getConfiguration());

    // 检查输出目录是否已存在，存在则抛出异常避免覆盖
    if (outDir.getFileSystem(job.getConfiguration()).exists(outDir)) {
      throw new FileAlreadyExistsException("Output directory " + outDir + 
                                           " already exists");
    }
  }

  /**
   * 设置MapReduce作业的输出根目录
   * @param job 目标作业对象
   * @param outputDir 输出根目录路径
   */
  public static void setOutputPath(Job job, Path outputDir) {
    try {
      // 将路径转换为合格的绝对路径
      outputDir = outputDir.getFileSystem(job.getConfiguration()).makeQualified(
          outputDir);
    } catch (IOException e) {
        // 兼容MR1，将IO异常转换为运行时异常抛出
        throw new RuntimeException(e);
    }
    job.getConfiguration().set(FileOutputFormat.OUTDIR, outputDir.toString());
  }

  /**
   * 获取MapReduce作业的输出根目录
   * @param job 作业上下文对象
   * @return 输出根目录路径
   */
  public static Path getOutputPath(JobContext job) {
    String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
    return name == null ? null: new Path(name);
  }
  
  /**
   * 获取当前任务尝试的临时输出工作目录，用于解决推测执行等场景避免文件名冲突
   * 框架会在任务成功完成后自动将临时目录中的文件提升到输出根目录，失败则删除临时目录
   * @param context 任务输入输出上下文
   * @return 当前任务尝试的临时输出工作目录路径
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public static Path getWorkOutputPath(TaskInputOutputContext<?,?,?,?> context
                                       ) throws IOException, 
                                                InterruptedException {
    PathOutputCommitter committer = (PathOutputCommitter)
      context.getOutputCommitter();
    Path workPath = committer.getWorkPath();
    LOG.debug("Work path is {}", workPath);
    return workPath;
  }

  /**
   * 为当前任务在工作目录生成唯一的输出文件路径
   * @param context 任务上下文
   * @param name 文件名基础名称
   * @param extension 文件扩展名
   * @return 唯一的输出文件路径
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public 
  static Path getPathForWorkFile(TaskInputOutputContext<?,?,?,?> context, 
                                 String name,
                                 String extension
                                ) throws IOException, InterruptedException {
    return new Path(getWorkOutputPath(context),
                    getUniqueFile(context, name, extension));
  }

  /**
   * 根据任务ID生成唯一的文件名，保证不同任务输出文件名不冲突
   * @param context 任务尝试上下文
   * @param name 文件名基础名称
   * @param extension 文件扩展名
   * @return 生成的唯一文件名
   */
  public synchronized static String getUniqueFile(TaskAttemptContext context,
                                                  String name,
                                                  String extension) {
    TaskID taskId = context.getTaskAttemptID().getTaskID();
    int partition = taskId.getId();
    StringBuilder result = new StringBuilder();
    result.append(name);
    result.append('-');
    // 添加任务类型标识（m表示map，r表示reduce等）
    result.append(
        TaskID.getRepresentingCharacter(taskId.getTaskType()));
    result.append('-');
    // 格式化分区编号，保证字典序正确
    result.append(NUMBER_FORMAT.format(partition));
    result.append(extension);
    return result.toString();
  }

  /**
   * 获取当前任务默认的默认工作输出文件路径
   * @param context 任务尝试上下文
   * @param extension 文件扩展名
   * @return 默认工作输出文件路径
   * @throws IOException IO异常
   */
  public Path getDefaultWorkFile(TaskAttemptContext context,
                                 String extension) throws IOException{
    OutputCommitter c = getOutputCommitter(context);
    // 验证提交器必须是PathOutputCommitter类型
    Preconditions.checkState(c instanceof PathOutputCommitter,
        "Committer %s is not a PathOutputCommitter", c);
    Path workPath = ((PathOutputCommitter) c).getWorkPath();
    Preconditions.checkNotNull(workPath,
        "Null workPath returned by committer %s", c);
    // 生成唯一文件名并拼接工作路径
    Path workFile = new Path(workPath,
        getUniqueFile(context, getOutputName(context), extension));
    LOG.debug("Work file for {} extension '{}' is {}",
        context, extension, workFile);
    return workFile;
  }

  /**
   * 获取输出文件的基础名称
   * @param job 作业上下文
   * @return 输出文件基础名称
   */
  protected static String getOutputName(JobContext job) {
    return job.getConfiguration().get(BASE_OUTPUT_NAME, PART);
  }

  /**
   * 设置输出文件的基础名称
   * @param job 作业上下文
   * @param name 输出文件基础名称
   */
  protected static void setOutputName(JobContext job, String name) {
    job.getConfiguration().set(BASE_OUTPUT_NAME, name);
  }

  /**
   * 获取文件输出提交器，负责输出文件的提交和清理工作
   * @param context 任务尝试上下文
   * @return 文件输出提交器实例
   * @throws IOException IO异常
   */
  public synchronized
      OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
    if (committer == null) {
      // 从工厂创建输出提交器，使用单例模式缓存
      Path output = getOutputPath(context);
      committer = PathOutputCommitterFactory.getCommitterFactory(
          output,
          context.getConfiguration()).createOutputCommitter(output, context);
    }
    return committer;
  }
}