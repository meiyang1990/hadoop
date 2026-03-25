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
package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;

/**
 * 文件级注释：BinaryEditsVisitor是离线编辑日志查看工具的二进制格式输出访问器，
 * 核心职责是将解析后的HDFS编辑日志操作重新写出为标准二进制格式的编辑日志文件，
 * 用于编辑日志的格式转换或重新生成。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class BinaryEditsVisitor implements OfflineEditsVisitor {
  final private EditLogFileOutputStream elfos;

  /**
   * 构造函数，初始化输出到指定文件的二进制编辑日志访问器
   * @param outputName 输出二进制编辑日志文件的路径名
   * @throws IOException 初始化文件输出流失败时抛出IO异常
   */
  public BinaryEditsVisitor(String outputName) throws IOException {
    this.elfos = new EditLogFileOutputStream(new Configuration(),
      new File(outputName), 0);
    // 创建新的编辑日志文件，使用当前HDFS磁盘布局版本
    elfos.create(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
  }

  /**
   * 访问器启动方法，此处无需额外初始化逻辑
   * @param version 输入编辑日志的版本号
   * @throws IOException 不抛出IO异常
   */
  @Override
  public void start(int version) throws IOException {
  }

  /**
   * 关闭访问器，完成输出并刷新同步数据到磁盘
   * @param error 访问过程中抛出的异常，如果不为null则需要处理错误
   * @throws IOException 关闭输出流或刷盘失败时抛出IO异常
   */
  @Override
  public void close(Throwable error) throws IOException {
    // 设置输出流可刷新状态
    elfos.setReadyToFlush();
    // 刷新所有缓冲数据并同步到磁盘
    elfos.flushAndSync(true);
    // 关闭输出流
    elfos.close();
  }

  /**
   * 处理单个编辑日志操作，将操作写出到二进制文件
   * @param op 待输出的编辑日志操作对象
   * @throws IOException 写出操作失败时抛出IO异常
   */
  @Override
  public void visitOp(FSEditLogOp op) throws IOException {
    elfos.write(op);
  }
}