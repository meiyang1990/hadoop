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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.files;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;

/**
 * Hadoop命令行工具，用于解析并打印MapReduce作业输出提交清单文件内容，支持查看作业提交成功标记、元数据和IO统计信息。
 * 属于Manifest提交协议的调试辅助工具。
 */
public class ManifestPrinter extends Configured implements Tool {

  private static final String USAGE = "successfile <success-file>";

  /**
   * 输出流，用于打印清单内容。
   */
  private final PrintStream out;

  /**
   * 默认构造器，默认输出到System.out。
   */
  public ManifestPrinter() {
    this(null, System.out);
  }

  /**
   * 构造器，指定配置和输出流。
   * @param conf Hadoop配置
   * @param out 输出流
   */
  public ManifestPrinter(Configuration conf, PrintStream out) {
    super(conf);
    this.out = out;
  }

  @Override
  public int run(String[] args) throws Exception {
    // 检查参数数量是否正确
    if (args.length != 1) {
      printUsage();
      return -1;
    }
    // 解析输入文件路径
    Path path = new Path(args[0]);
    // 加载并打印清单文件内容
    loadAndPrintManifest(path.getFileSystem(getConf()), path);
    return 0;
  }

  /**
   * 从指定文件系统加载清单文件并打印其内容。
   * @param fs 目标文件系统
   * @param path 清单文件路径
   * @throws IOException 加载失败时抛出异常
   * @return 加载后的作业成功数据对象
   */
  public ManifestSuccessData loadAndPrintManifest(FileSystem fs, Path path)
      throws IOException {
    // 加载清单文件
    println("Manifest file: %s", path);
    final ManifestSuccessData success = ManifestSuccessData.load(fs, path);

    printManifest(success);
    return success;
  }

  /**
   * 按格式打印作业成功清单的所有字段信息。
   * @param success 加载完成的作业成功数据对象
   */
  public void printManifest(ManifestSuccessData success) {
    field("succeeded", success.getSuccess());
    field("created", success.getDate());
    field("committer", success.getCommitter());
    field("hostname", success.getHostname());
    field("description", success.getDescription());
    field("jobId", success.getJobId());
    field("jobIdSource", success.getJobIdSource());
    field("stage", success.getStage());
    // 打印诊断信息
    println("Diagnostics\n%s",
        success.dumpDiagnostics("  ", " = ", "\n"));
    // 打印IO统计信息
    println("Statistics:\n%s",
        ioStatisticsToPrettyString(success.getIOStatistics()));
    out.flush();
  }

  private void printUsage() {
    println(USAGE);
  }

  /**
   * 格式化输出一行文本到输出流。
   * @param format 格式化字符串
   * @param args 格式化参数
   */
  private void println(String format, Object... args) {
    out.format(format, args);
    out.println();
  }

  /**
   * 如果字段值非空，则打印该字段。
   * @param name 字段名称
   * @param value 字段值
   */
  private void field(String name, Object value) {
    if (value != null) {
      println("%s: %s", name, value);
    }
  }

  /**
   * 命令行入口方法，通过ToolRunner启动工具。
   */
  public static void main(String[] argv) throws Exception {

    try {
      int res = ToolRunner.run(new ManifestPrinter(), argv);
      System.exit(res);
    } catch (ExitUtil.ExitException e) {
      ExitUtil.terminate(e);
    }
  }
}