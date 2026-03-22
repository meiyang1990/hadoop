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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 文件级注释：HDFS快照差异对比命令行工具，用于对比两个快照之间，或者快照与目录当前状态之间的差异，输出差异报告。
 *
 * A tool used to get the difference report between two snapshots, or between
 * a snapshot and the current status of a directory. 
 * <pre>
 * Usage: SnapshotDiff snapshotDir from to
 * For from/to, users can use "." to present the current status, and use 
 * ".snapshot/snapshot_name" to present a snapshot, where ".snapshot/" can be 
 * omitted.
 * </pre>
 */
@InterfaceAudience.Private
/**
 * 快照差异对比工具类，实现Hadoop Tool接口，作为命令行工具运行。
 * 核心功能是获取并打印HDFS快照之间或者快照与当前目录的差异报告。
 */
@InterfaceAudience.Private
public class SnapshotDiff extends Configured implements Tool {
  /**
   * 无参构造方法，使用默认HDFS配置初始化工具对象。
   */
  public SnapshotDiff() {
    this(new HdfsConfiguration());
  }

  /**
   * 带配置的构造方法，使用指定配置初始化工具对象。
   * @param conf Hadoop配置对象
   */
  public SnapshotDiff(Configuration conf) {
    super(conf);
  }

  /**
   * 从用户输入中解析提取快照名称，处理用户输入的省略写法和路径前缀。
   * 当前目录使用.表示，返回空字符串代表当前状态；
   * 支持用户省略.snapshot/前缀，自动提取实际快照名称。
   * @param name 用户输入的快照名称/路径
   * @return 解析后的标准快照名称，空字符串代表当前目录状态
   */
  private static String getSnapshotName(String name) {
    if (Path.CUR_DIR.equals(name)) { // current directory
      return "";
    }
    final int i;
    if (name.startsWith(HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR)) {
      i = 0;
    } else if (name.startsWith(
        HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR + Path.SEPARATOR)) {
      i = 1;
    } else {
      return name;
    }

    // 提取并返回去掉.snapshot前缀后的实际快照名称
    return name.substring(i + HdfsConstants.DOT_SNAPSHOT_DIR.length() + 1);
  }
  
  @Override
  /**
   * 工具执行入口，解析命令行参数，调用HDFS获取差异报告并输出结果。
   * @param argv 命令行参数，长度必须为3：<快照目录> <起始快照> <结束快照>
   * @return 执行结果码，0代表成功，非0代表失败
   * @throws Exception 执行过程中可能抛出的异常
   */
  public int run(String[] argv) throws Exception {
    String description = "hdfs snapshotDiff <snapshotDir> <from> <to>:\n" +
    "\tGet the difference between two snapshots, \n" + 
    "\tor between a snapshot and the current tree of a directory.\n" +
    "\tFor <from>/<to>, users can use \".\" to present the current status,\n" +
    "\tand use \".snapshot/snapshot_name\" to present a snapshot,\n" +
    "\twhere \".snapshot/\" can be omitted\n";
    
    // 参数个数校验
    if(argv.length != 3) {
      System.err.println("Usage: \n" + description);
      return 1;
    }

    // 获取指定路径对应的文件系统实例
    FileSystem fs = FileSystem.get(new Path(argv[0]).toUri(), getConf());
    // 校验是否为分布式文件系统，快照仅支持HDFS
    if (! (fs instanceof DistributedFileSystem)) {
      System.err.println(
          "SnapshotDiff can only be used in DistributedFileSystem");
      return 1;
    }
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    
    // 解析命令行参数
    Path snapshotRoot = new Path(argv[0]);
    String fromSnapshot = getSnapshotName(argv[1]);
    String toSnapshot = getSnapshotName(argv[2]);
    try {
      // 调用HDFS接口获取快照差异报告
      SnapshotDiffReport diffReport = dfs.getSnapshotDiffReport(snapshotRoot,
          fromSnapshot, toSnapshot);
      // 打印差异报告到控制台
      System.out.println(diffReport.toString());
    } catch (IOException e) {
      // 异常处理，打印错误信息和堆栈
      String[] content = e.getLocalizedMessage().split("\n");
      System.err.println("snapshotDiff: " + content[0]);
      e.printStackTrace(System.err);
      return 1;
    }
    return 0;
  }

  /**
   * 命令行入口方法，通过ToolRunner启动快照差异对比工具。
   * @param argv 命令行参数
   * @throws Exception 启动或执行过程中抛出的异常
   */
  public static void main(String[] argv) throws Exception {
    int rc = ToolRunner.run(new SnapshotDiff(), argv);
    System.exit(rc);
  }

}