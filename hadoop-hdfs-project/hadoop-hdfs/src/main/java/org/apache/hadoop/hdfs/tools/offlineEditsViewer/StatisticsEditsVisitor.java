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

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;

/**
 * 离线编辑日志查看工具的统计访问者实现，负责统计编辑日志中各类操作码出现的次数并输出统计结果
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class StatisticsEditsVisitor implements OfflineEditsVisitor {
  final private PrintWriter out;

  private int version = -1;
  private final Map<FSEditLogOpCodes, Long> opCodeCount =
    new HashMap<FSEditLogOpCodes, Long>();

  /**
   * 构造统计访问者，指定统计结果输出流
   * @param out 统计结果输出流
   */
  public StatisticsEditsVisitor(OutputStream out) throws IOException {
    this.out = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
  }

  /**
   * 访问者开始处理入口，保存编辑日志版本号
   * @param version 编辑日志版本
   */
  @Override
  public void start(int version) throws IOException {
    this.version = version;
  }
  
  /**
   * 关闭访问者，输出统计结果并关闭输出流
   * @param error 处理过程中产生的异常，如果为null则表示无错误
   */
  @Override
  public void close(Throwable error) throws IOException {
    out.print(getStatisticsString());
    if (error != null) {
      out.print("EXITING ON ERROR: " + error.toString() + "\n");
    }
    out.close();
  }

  @Override
  public void visitOp(FSEditLogOp op) throws IOException {
    incrementOpCodeCount(op.opCode);
  }

  /**
   * 对指定操作码的计数加1
   * @param opCode 需要增加计数的操作码
   */
  private void incrementOpCodeCount(FSEditLogOpCodes opCode) {
    if(!opCodeCount.containsKey(opCode)) {
      opCodeCount.put(opCode, 0L);
    }
    Long newValue = opCodeCount.get(opCode) + 1;
    opCodeCount.put(opCode, newValue);
  }

  /**
   * 获取所有操作码的统计结果
   * @return 操作码到对应计数的映射表
   */
  public Map<FSEditLogOpCodes, Long> getStatistics() {
    return opCodeCount;
  }

  /**
   * 将统计结果格式化为可打印的字符串形式
   * @return 格式化后的统计结果字符串
   */
  public String getStatisticsString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(
        "    %-30.30s      : %d%n",
        "VERSION", version));
    for(FSEditLogOpCodes opCode : FSEditLogOpCodes.values()) {
      Long count = opCodeCount.get(opCode);
      sb.append(String.format(
          "    %-30.30s (%3d): %d%n",
          opCode.toString(),
          opCode.getOpCode(),
          count == null ? Long.valueOf(0L) : count));
    }
    return sb.toString();
  }
}