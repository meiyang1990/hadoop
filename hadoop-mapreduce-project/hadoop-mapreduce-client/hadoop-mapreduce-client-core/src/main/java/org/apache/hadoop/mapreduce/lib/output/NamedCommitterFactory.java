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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 文件路径：hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/lib/output/NamedCommitterFactory.java
 * <p>
 * 根据配置中指定的类名创建自定义输出提交器的工厂类
 * 通过 {@link PathOutputCommitterFactory#NAMED_COMMITTER_CLASS} 配置项加载自定义的输出提交器实现
 */
public final class NamedCommitterFactory extends
    PathOutputCommitterFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(NamedCommitterFactory.class);

  /**
   * 根据配置创建指定类名的路径输出提交器实例
   * @param outputPath 输出路径
   * @param context 任务尝试上下文
   * @return 输出提交器实例
   * @throws IOException 加载或实例化提交器失败时抛出
   */
  @SuppressWarnings("JavaReflectionMemberAccess")
  @Override
  public PathOutputCommitter createOutputCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    // 从配置中加载提交器类
    Class<? extends PathOutputCommitter> clazz = loadCommitterClass(context);
    LOG.debug("Using PathOutputCommitter implementation {}", clazz);
    try {
      // 获取构造函数：参数为(Path, TaskAttemptContext)
      Constructor<? extends PathOutputCommitter> ctor
          = clazz.getConstructor(Path.class, TaskAttemptContext.class);
      // 反射实例化提交器
      return ctor.newInstance(outputPath, context);
    } catch (NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | InvocationTargetException e) {
      // 实例化失败包装为IO异常抛出
      throw new IOException("Failed to create " + clazz
          + ":" + e, e);
    }
  }

  /**
   * 从任务上下文配置中加载指定类名的输出提交器类
   * @param context 作业或任务上下文
   * @return 加载完成的输出提交器类
   * @throws IOException 配置中未指定提交器类时抛出
   */
  private Class<? extends PathOutputCommitter> loadCommitterClass(
      JobContext context) throws IOException {
    // 检查上下文不为空
    Preconditions.checkNotNull(context, "null context");
    Configuration conf = context.getConfiguration();
    // 读取配置中指定的提交器类名
    String value = conf.get(NAMED_COMMITTER_CLASS, "");
    if (value.isEmpty()) {
      // 未配置类名抛出异常
      throw new IOException("No committer defined in " + NAMED_COMMITTER_CLASS);
    }
    // 从配置中加载提交器类，默认回退到FileOutputCommitter
    return conf.getClass(NAMED_COMMITTER_CLASS,
        FileOutputCommitter.class, PathOutputCommitter.class);
  }
}