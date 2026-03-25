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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 文件输出提交器工厂，始终创建标准FileOutputCommitter实例。
 * 作为PathOutputCommitterFactory的具体实现，用于MapReduce任务输出文件的提交管理
 */
public final class FileOutputCommitterFactory
    extends PathOutputCommitterFactory {

  /**
   * 创建标准文件输出提交器实例
   * @param outputPath 任务输出根路径
   * @param context 任务尝试上下文
   * @return 标准FileOutputCommitter实例
   * @throws IOException 创建过程中IO异常
   */
  @Override
  public PathOutputCommitter createOutputCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    return createFileOutputCommitter(outputPath, context);
  }

}