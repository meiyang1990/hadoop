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


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.tools.AdminHelper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 文件级注释：HDFS 快照列表查询命令行工具，用于列出可快照目录下的所有快照
 * 
 * A tool used to list all snapshottable directories that are owned by the
 * current user. The tool returns all the snapshottable directories if the user
 * is a super user.
 */
@InterfaceAudience.Private
/**
 * LsSnapshot 类：实现 `hdfs lsSnapshot` 命令行工具，用于查询指定可快照目录下的所有快照
 */
@InterfaceAudience.Private
public class LsSnapshot extends Configured implements Tool {
  /**
   * 工具执行入口，处理命令行参数并执行快照列表查询
   * @param argv 命令行参数数组
   * @return 执行结果码，0表示成功，非0表示失败
   * @throws Exception 执行过程中可能抛出的异常
   */
  @Override
  public int run(String[] argv) throws Exception {
    String description = "hdfs lsSnapshot <snapshotDir>: \n" +
        "\tGet the list of snapshots for a snapshottable directory.\n";

    // 检查参数个数是否合法
    if(argv.length != 1) {
      System.err.println("Invalid no of arguments");
      System.err.println("Usage: \n" + description);
      return 1;
    }
    // 解析命令行传入的可快照目录路径
    Path snapshotRoot = new Path(argv[0]);
    try {
      // 获取 DistributedFileSystem 实例
      DistributedFileSystem dfs = AdminHelper.getDFS(getConf());
      // 从HDFS获取指定目录的快照列表
      SnapshotStatus[] stats = dfs.getSnapshotListing(snapshotRoot);
      // 打印快照列表到标准输出
      SnapshotStatus.print(stats, System.out);
    } catch (Exception e) {
      // 提取异常第一行信息输出到错误日志
      String[] content = e.getLocalizedMessage().split("\n");
      System.err.println("lsSnapshot: " + content[0]);
      return 1;
    }
    return 0;
  }

  /**
   * 工具主方法，通过 ToolRunner 启动命令行工具
   * @param argv 命令行参数数组
   * @throws Exception 启动执行过程中可能抛出的异常
   */
  public static void main(String[] argv) throws Exception {
    int rc = ToolRunner.run(new LsSnapshot(), argv);
    System.exit(rc);
  }
}