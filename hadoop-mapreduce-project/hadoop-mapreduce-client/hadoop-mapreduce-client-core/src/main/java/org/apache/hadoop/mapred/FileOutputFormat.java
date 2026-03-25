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

import java.io.IOException;
import java.text.NumberFormat;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.Progressable;

/**
 * 文件输出格式的抽象基类，为所有基于文件的OutputFormat提供公共基础能力，
 * 负责管理输出目录配置、输出压缩设置、输出路径检查等通用逻辑。
 * 是旧MapReduce API中所有文件类输出格式的父类。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileOutputFormat<K, V> implements OutputFormat<K, V> {

  @Deprecated
  public enum Counter {
    BYTES_WRITTEN
  }
  
  /**
   * 设置作业输出是否需要压缩
   * @param conf 作业配置对象
   * @param compress 是否压缩输出
   */
  public static void setCompressOutput(JobConf conf, boolean compress) {
    conf.setBoolean(org.apache.hadoop.mapreduce.lib.output.
      FileOutputFormat.COMPRESS, compress);
  }
  
  /**
   * 获取作业输出是否需要压缩的配置
   * @param conf 作业配置对象
   * @return true表示需要压缩，false表示不需要压缩
   */
  public static boolean getCompressOutput(JobConf conf) {
    return conf.getBoolean(org.apache.hadoop.mapreduce.lib.output.
      FileOutputFormat.COMPRESS, false);
  }
  
  /**
   * 设置用于压缩作业输出的压缩编解码器类
   * @param conf 作业配置对象
   * @param codecClass 压缩编解码器类
   */
  public static void 
  setOutputCompressorClass(JobConf conf, 
                           Class<? extends CompressionCodec> codecClass) {
    setCompressOutput(conf, true);
    conf.setClass(org.apache.hadoop.mapreduce.lib.output.
      FileOutputFormat.COMPRESS_CODEC, codecClass, 
                  CompressionCodec.class);
  }
  
  /**
   * 获取用于压缩作业输出的压缩编解码器类
   * @param conf 作业配置对象
   * @param defaultValue 如果未配置则返回的默认编解码器类
   * @return 要使用的压缩编解码器类
   * @throws IllegalArgumentException 配置了编解码器但找不到类时抛出
   */
  public static Class<? extends CompressionCodec> 
  getOutputCompressorClass(JobConf conf, 
		                       Class<? extends CompressionCodec> defaultValue) {
    Class<? extends CompressionCodec> codecClass = defaultValue;
    
    String name = conf.get(org.apache.hadoop.mapreduce.lib.output.
      FileOutputFormat.COMPRESS_CODEC);
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
  
  /**
   * 获取用于写入输出的RecordWriter实例，由子类实现具体的文件写入逻辑
   * @param ignored 文件系统对象（参数保留但未实际使用）
   * @param job 作业配置对象
   * @param name 输出文件名
   * @param progress 进度回调对象
   * @return 用于写入键值对的RecordWriter
   * @throws IOException 创建写入器失败时抛出
   */
  public abstract RecordWriter<K, V> getRecordWriter(FileSystem ignored,
                                               JobConf job, String name,
                                               Progressable progress)
    throws IOException;

  /**
   * 检查作业输出目录规格，验证输出目录是否合法
   * @param ignored 文件系统对象（参数保留但未实际使用）
   * @param job 作业配置对象
   * @throws FileAlreadyExistsException 输出目录已存在时抛出
   * @throws InvalidJobConfException 输出目录未配置时抛出
   * @throws IOException 文件系统操作失败时抛出
   */
  public void checkOutputSpecs(FileSystem ignored, JobConf job) 
    throws FileAlreadyExistsException, 
           InvalidJobConfException, IOException {
    // 获取作业输出路径
    Path outDir = getOutputPath(job);
    // 有Reduce任务但未配置输出目录，抛出异常
    if (outDir == null && job.getNumReduceTasks() != 0) {
      throw new InvalidJobConfException("Output directory not set in JobConf.");
    }
    if (outDir != null) {
      // 获取输出路径所在的文件系统
      FileSystem fs = outDir.getFileSystem(job);
      // 标准化输出路径格式
      outDir = fs.makeQualified(outDir);
      // 更新配置中的输出路径
      setOutputPath(job, outDir);
      
      // 为输出目录所在文件系统获取委托令牌，用于权限认证
      TokenCache.obtainTokensForNamenodes(job.getCredentials(), 
                                          new Path[] {outDir}, job);
      
      // 检查输出目录是否已存在，存在则抛出异常避免覆盖已有数据
      if (fs.exists(outDir)) {
        throw new FileAlreadyExistsException("Output directory " + outDir + 
                                             " already exists");
      }
    }
  }

  /**
   * 设置MapReduce作业的输出目录路径
   * @param conf 作业配置对象
   * @param outputDir 输出目录路径
   */
  public static void setOutputPath(JobConf conf, Path outputDir) {
    outputDir = new Path(conf.getWorkingDirectory(), outputDir);
    conf.set(org.apache.hadoop.mapreduce.lib.output.
      FileOutputFormat.OUTDIR, outputDir.toString());
  }

  /**
   * 设置任务临时输出目录路径，由MapReduce框架内部调用
   * @param conf 作业配置对象
   * @param outputDir 任务临时输出目录路径
   */
  @Private
  public static void setWorkOutputPath(JobConf conf, Path outputDir) {
    outputDir = new Path(conf.getWorkingDirectory(), outputDir);
    conf.set(JobContext.TASK_OUTPUT_DIR, outputDir.toString());
  }
  
  /**
   * 获取MapReduce作业的输出目录路径
   * @return 作业输出目录路径，未配置则返回null
   */
  public static Path getOutputPath(JobConf conf) {
    String name = conf.get(org.apache.hadoop.mapreduce.lib.output.
      FileOutputFormat.OUTDIR);
    return name == null ? null: new Path(name);
  }
  
  /**
   * 获取当前任务尝试的临时输出目录路径，用于存放任务侧输出文件，
   * 避免推测执行中同一任务的多个尝试同时写入同一个文件冲突。
   * <p>
   * 任务成功完成后，框架会自动将临时目录中的文件移动到最终输出目录，
   * 失败任务的临时目录会被直接丢弃，对应用户透明。
   * </p>
   * 
   * @param conf 作业配置对象
   * @return 当前任务尝试的临时输出目录路径，未配置则返回null
   */
  public static Path getWorkOutputPath(JobConf conf) {
    String name = conf.get(JobContext.TASK_OUTPUT_DIR);
    return name == null ? null: new Path(name);
  }

  /**
   * 生成任务临时输出文件路径，创建临时目录并返回最终文件路径
   * @param conf 作业配置对象
   * @param name 输出文件名
   * @return 任务临时输出文件的完整路径
   * @throws IOException 路径解析或生成失败时抛出
   */
  public static Path getTaskOutputPath(JobConf conf, String name) 
  throws IOException {
    // 获取作业最终输出目录
    Path outputPath = getOutputPath(conf);
    if (outputPath == null) {
      throw new IOException("Undefined job output-path");
    }

    OutputCommitter committer = conf.getOutputCommitter();
    Path workPath = outputPath;
    TaskAttemptContext context = 
      new TaskAttemptContextImpl(conf,
                                 TaskAttemptID.forName(conf.get(
                                     JobContext.TASK_ATTEMPT_ID)));
    // 如果输出提交器是FileOutputCommitter，使用其工作路径作为临时输出根目录
    if (committer instanceof FileOutputCommitter) {
      workPath = ((FileOutputCommitter)committer).getWorkPath(context,
                                                              outputPath);
    }
    
    // 拼接生成最终临时输出文件路径
    return new Path(workPath, name);
  } 

  /**
   * 生成任务唯一文件名，保证同一作业不同任务生成的文件名不冲突
   * @param conf 作业配置对象
   * @param name 基础文件名
   * @return 带任务分区和类型后缀的唯一文件名
   */
  public static String getUniqueName(JobConf conf, String name) {
    // 获取当前任务的分区编号
    int partition = conf.getInt(JobContext.TASK_PARTITION, -1);
    // 只能在任务执行上下文内调用
    if (partition == -1) {
      throw new IllegalArgumentException(
        "This method can only be called from within a Job");
    }

    // 根据任务类型（Map/Reduce）生成短标记
    String taskType = conf.getBoolean(JobContext.TASK_ISMAP,
        JobContext.DEFAULT_TASK_ISMAP) ? "m" : "r";

    // 格式化分区编号为5位固定长度，方便排序
    NumberFormat numberFormat = NumberFormat.getInstance();
    numberFormat.setMinimumIntegerDigits(5);
    numberFormat.setGroupingUsed(false);

    // 拼接生成唯一名称
    return name + "-" + taskType + "-" + numberFormat.format(partition);
  }

  /**
   * 生成任务唯一文件路径，路径位于任务临时输出目录下，保证不同任务不冲突
   * @param conf 作业配置对象
   * @param name 基础文件名
   * @return 唯一文件路径，可直接用于创建自定义输出文件
   */
  public static Path getPathForCustomFile(JobConf conf, String name) {
    return new Path(getWorkOutputPath(conf), getUniqueName(conf, name));
  }
}