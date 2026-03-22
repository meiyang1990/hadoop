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
package org.apache.hadoop.hdfs.tools.snapshot;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 文件级注释：HDFS快照工具类，用于列出当前用户拥有的所有可快照目录。
 * 超级用户执行该命令时会返回集群中所有可快照目录。
 * 实现了Hadoop Tool接口，可作为独立命令行工具运行。
 *
 * A tool used to list all snapshottable directories that are owned by the 
 * current user. The tool returns all the snapshottable directories if the user
 * is a super user.
 */
@InterfaceAudience.Private
public class LsSnapshottableDir extends Configured implements Tool {

  /**
   * 执行lsSnapshottableDir命令，列出可快照目录
   * @param argv 命令行参数，本工具不需要额外参数
   * @return 执行结果，0表示成功，非0表示失败
   * @throws Exception 执行过程中可能抛出异常
   */
  @Override
  public int run(String[] argv) throws Exception {
    String description = "hdfs lsSnapshottableDir: \n" +
        "\tGet the list of snapshottable directories that are owned by the current user.\n" +
        "\tReturn all the snapshottable directories if the current user is a super user.\n";
    // 检查参数个数，本工具不需要额外参数
    if(argv.length != 0) {
      System.err.println("Usage: \n" + description);
      return 1;
    }
    
    // 获取当前配置对应的文件系统实例
    FileSystem fs = FileSystem.get(getConf());
    // 检查当前文件系统是否为HDFS分布式文件系统
    if (! (fs instanceof DistributedFileSystem)) {
      System.err.println(
          "LsSnapshottableDir can only be used in DistributedFileSystem");
      return 1;
    }
    // 转换为DistributedFileSystem实例以调用HDFS专属接口
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    
    try {
      // 从NameNode获取可快照目录列表
      SnapshottableDirectoryStatus[] stats = dfs.getSnapshottableDirListing();
      // 格式化输出结果到标准输出
      SnapshottableDirectoryStatus.print(stats, System.out);
    } catch (IOException e) {
      // 提取异常第一行信息输出到标准错误
      String[] content = e.getLocalizedMessage().split("\n");
      System.err.println("lsSnapshottableDir: " + content[0]);
      return 1;
    }
    // 执行成功返回0
    return 0;
  }

  /**
   * 工具主入口，通过ToolRunner运行命令
   * @param argv 命令行参数
   * @throws Exception 运行过程中可能抛出异常
   */
  public static void main(String[] argv) throws Exception {
    int rc = ToolRunner.run(new LsSnapshottableDir(), argv);
    System.exit(rc);
  }
}