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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * 输出目录日志过滤器，用于过滤掉输出目录中的日志文件夹
 * <p>
 * 核心功能是过滤掉路径中包含_logs的目录，在列出输出目录结果文件时排除日志目录，
 * 可用于筛选MapReduce作业输出目录中的最终结果文件，排除日志文件夹。
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class OutputLogFilter implements PathFilter {
  /** 委托内部公共实现类完成实际过滤逻辑 */
  private static final PathFilter LOG_FILTER = 
    new Utils.OutputFileUtils.OutputLogFilter();

  /**
   * 判断给定路径是否通过过滤，排除日志目录
   * @param path 待检查的HDFS路径
   * @return true表示保留该路径，false表示过滤掉该路径（日志路径）
   */
  public boolean accept(Path path) {
    return LOG_FILTER.accept(path);
  }
}