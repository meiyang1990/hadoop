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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;

import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 文件输出提交器抽象基类，定义了将任务工作目录的数据提交到最终输出目录的核心接口
 * 
 * 核心职责：提供基于路径的输出提交协议，标准实现为{@link FileOutputCommitter}
 * 子类需要实现具体的目录操作和提交逻辑，支持自定义输出提交流程
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class PathOutputCommitter extends OutputCommitter {
  private static final Logger LOG =
      LoggerFactory.getLogger(PathOutputCommitter.class);

  private final JobContext context;

  /**
   * 任务尝试级构造函数，子类必须提供相同签名的公共构造函数
   * @param outputPath 输出路径，可为null
   * @param context 任务尝试上下文
   * @throws IOException 初始化IO异常
   */
  protected PathOutputCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    this.context = Preconditions.checkNotNull(context, "Null context");
    // 日志记录提交器实例化信息
    LOG.debug("Instantiating committer {} with output path {} and task context"
        + " {}", this, outputPath, context);
  }

  /**
   * 作业级构造函数，子类必须提供相同签名的公共构造函数
   * @param outputPath 输出路径，可为null
   * @param context 作业上下文
   * @throws IOException 初始化IO异常
   */
  protected PathOutputCommitter(Path outputPath,
      JobContext context) throws IOException {
    this.context = Preconditions.checkNotNull(context, "Null context");
    // 日志记录提交器实例化信息
    LOG.debug("Instantiating committer {} with output path {} and job context"
        + " {}", this, outputPath, context);
  }

  /**
   * 获取作业提交完成后最终输出的根目录
   * @return 作业最终输出路径，无输出路径时返回null
   */
  public abstract Path getOutputPath();

  /**
   * 判断是否配置了输出路径
   * @return true表示已配置输出路径，false表示未配置
   */
  public boolean hasOutputPath() {
    return getOutputPath() != null;
  }

  /**
   * 获取任务写入临时结果的工作目录
   * @return 任务工作目录，可能为null
   * @throws IOException 获取路径IO异常
   */
  public abstract Path getWorkPath() throws IOException;

  @Override
  public String toString() {
    return "PathOutputCommitter{context=" + context
        + "; " + super.toString()
        + '}';
  }
}