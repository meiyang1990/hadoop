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
 * MapReduce 工具类，提供输出目录文件过滤相关工具能力，用于从输出目录中过滤结果分片文件。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Utils {
  /**
   * MapReduce 输出文件工具类，提供不同粒度的输出目录文件过滤实现。
   */
  public static class OutputFileUtils {
    /**
     * 输出结果文件过滤器，继承OutputLogFilter后额外过滤掉_SUCCESS文件，只保留真正的结果分片文件。
     * 可用于列出输出目录中的所有结果文件，使用示例：
     * <pre>
     * Path[] fileList = FileUtil.stat2Paths(fs.listStatus(outDir, new OutputFilesFilter()));
     * </pre>
     */
    public static class OutputFilesFilter extends OutputLogFilter {
      @Override
      public boolean accept(Path path) {
        // 先过滤日志目录，再排除成功标记文件
        return super.accept(path) 
               && !FileOutputCommitter.SUCCEEDED_FILE_NAME
                   .equals(path.getName());
      }
    }
    
    /**
     * 输出日志目录过滤器，过滤掉输出目录中的_logs日志目录，保留其他文件和目录。
     * 可用于列出输出目录中的非日志文件，使用示例：
     * <pre>
     * Path[] fileList = FileUtil.stat2Paths(fs.listStatus(outDir, new OutputLogFilter()));
     * </pre>
     */
    public static class OutputLogFilter implements PathFilter {
      @Override
      public boolean accept(Path path) {
        // 过滤掉名称为_logs的目录
        return !"_logs".equals(path.getName());
      }
    }
  }
}