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
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * 文件级别注释：离线编辑日志查看工具的访问者工厂，根据用户指定的输出格式创建对应类型的EditsVisitor实例
 * 支持二进制、XML、统计信息三种不同的输出格式
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OfflineEditsVisitorFactory {
  /**
   * 根据输出格式类型创建对应EditsVisitor实例，负责将解析后的编辑日志转换为指定格式输出
   * @param filename 输出结果的文件路径
   * @param processor 需要创建的访问者类型，支持xml/binary/stats
   * @param printToScreen 是否同时将结果输出到控制台
   * @return 对应格式的EditsVisitor实例
   * @throws IOException 当创建访问者或打开输出文件出错时抛出异常
   */
  static public OfflineEditsVisitor getEditsVisitor(String filename,
    String processor, boolean printToScreen) throws IOException {
    // 匹配二进制格式输出
    if(StringUtils.equalsIgnoreCase("binary", processor)) {
      return new BinaryEditsVisitor(filename);
    }
    OfflineEditsVisitor vis;
    // 创建输出文件的输出流
    OutputStream fout = Files.newOutputStream(Paths.get(filename));
    OutputStream out = null;
    try {
      // 不需要输出到屏幕，只输出到文件
      if (!printToScreen) {
        out = fout;
      }
      else {
        // 需要同时输出到文件和屏幕，创建Tee流同时写两个输出流
        OutputStream outs[] = new OutputStream[2];
        outs[0] = fout;
        outs[1] = System.out;
        out = new TeeOutputStream(outs);
      }
      // 根据访问者类型创建对应实例
      if(StringUtils.equalsIgnoreCase("xml", processor)) {
        vis = new XmlEditsVisitor(out);
      } else if(StringUtils.equalsIgnoreCase("stats", processor)) {
        vis = new StatisticsEditsVisitor(out);
      } else {
        throw new IOException("Unknown processor " + processor +
          " (valid processors: xml, binary, stats)");
      }
      out = fout = null;
      return vis;
    } finally {
      // 确保资源关闭，避免泄漏
      IOUtils.closeStream(fout);
      IOUtils.closeStream(out);
    }
  }
}