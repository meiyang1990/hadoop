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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.InternalConstants.UNSUPPORTED_FS_SCHEMAS;

/**
 * 文件级注释：Manifest提交器工厂，用于为指定输出路径和任务尝试创建ManifestCommitter实例，
 * 作为Manifest输出提交器的工厂实现，可按文件系统 schema 注册绑定，为MapReduce输出提交提供工厂支持。
 * 核心职责是创建ManifestCommitter，并对不支持的文件系统提前做快速失败校验。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ManifestCommitterFactory extends PathOutputCommitterFactory {

  /**
   * 工厂名称，用于注册和标识当前工厂实现。
   */
  public static final String NAME = ManifestCommitterFactory.class.getName();

  /**
   * 创建Manifest输出提交器实例，校验输出路径文件系统是否支持，不支持则快速失败，支持则返回ManifestCommitter实例。
   * @param outputPath 作业输出路径
   * @param context 任务尝试上下文
   * @return 初始化完成的ManifestCommitter实例
   * @throws IOException 当文件系统不支持或IO操作异常时抛出
   */
  @Override
  public ManifestCommitter createOutputCommitter(final Path outputPath,
      final TaskAttemptContext context) throws IOException {
    // 安全校验：获取输出路径的文件系统scheme
    final String scheme = outputPath.toUri().getScheme();
    // 校验当前scheme是否属于不支持的文件系统
    if (UNSUPPORTED_FS_SCHEMAS.contains(scheme)) {
      // 不支持则立即抛出异常快速失败
      throw new PathIOException(outputPath.toString(),
          "This committer does not work with the filesystem of type " + scheme);
    }
    // 创建并返回ManifestCommitter实例
    return new ManifestCommitter(outputPath, context);
  }

}