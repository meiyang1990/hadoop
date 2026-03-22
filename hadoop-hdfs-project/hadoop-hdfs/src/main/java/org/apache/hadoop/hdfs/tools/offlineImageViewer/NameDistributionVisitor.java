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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 文件名称分布统计访问器，用于离线fsimage分析
 * <p>
 * 分析fsimage中的文件名，输出以下统计信息：
 * <ul>
 * <li>唯一文件名的总数量</li> 
 * <li>不同使用频次区间的文件名数量对应总文件数分布</li>
 * <li>如果复用文件名对象可以节省的堆内存大小</li>
 * </ul>
 * 本类是离线fsimage查看器的一个组成部分，用于分析文件名重复分布，为对象复用优化提供数据支撑
 */
@InterfaceAudience.Private
public class NameDistributionVisitor extends TextWriterImageVisitor {
  // 存储文件名 -> 出现次数的统计映射
  HashMap<String, Integer> counts = new HashMap<String, Integer>();

  /**
   * 构造文件名分布统计访问器
   * @param filename 输出结果文件名
   * @param printToScreen 是否同时输出到控制台
   * @throws IOException 初始化输出流时出错
   */
  public NameDistributionVisitor(String filename, boolean printToScreen)
      throws IOException {
    super(filename, printToScreen);
  }

  /**
   * 完成所有节点访问后，统计并输出最终结果
   * @throws IOException 写入输出结果时出错
   */
  @Override
  void finish() throws IOException {
    // 定义String底层byte数组对象头占用的内存大小
    final int BYTEARRAY_OVERHEAD = 24;

    write("Total unique file names " + counts.size());
    // 统计分箱，每一行格式：[最低出现次数, 总节省内存, 总文件数, 该分箱文件名数]
    final long stats[][] = { { 100000, 0, 0, 0 },
                             { 10000, 0, 0, 0 },
                             { 1000, 0, 0, 0 },
                             { 100, 0, 0, 0 },
                             { 10, 0, 0, 0 },
                             { 5, 0, 0, 0 },
                             { 4, 0, 0, 0 },
                             { 3, 0, 0, 0 },
                             { 2, 0, 0, 0 }};

    int highbound = Integer.MIN_VALUE;
    // 遍历所有文件名统计，按出现次数划分到对应分箱
    for (Entry<String, Integer> entry : counts.entrySet()) {
      // 更新最大出现次数，用于后续输出区间描述
      highbound = Math.max(highbound, entry.getValue());
      // 找到当前文件名命中的分箱
      for (int i = 0; i < stats.length; i++) {
        if (entry.getValue() >= stats[i][0]) {
          // 累加该文件名复用可节省的堆内存：(数组头大小 + 字符长度) * (重复次数 - 1)
          stats[i][1] += (BYTEARRAY_OVERHEAD + entry.getKey().length())
              * (entry.getValue() - 1);
          // 累加该分箱对应的总文件数
          stats[i][2] += entry.getValue();
          // 累加该分箱对应的文件名数量
          stats[i][3]++;
          break;
        }
      }
    }

    long lowbound = 0;
    long totalsavings = 0;
    // 按区间从大到小输出统计结果
    for (long[] stat : stats) {
      lowbound = stat[0];
      // 累加全局总节省内存
      totalsavings += stat[1];
      // 生成区间描述文本
      String range = lowbound == highbound ? " " + lowbound :
          " between " + lowbound + "-" + highbound;
      // 输出当前分箱统计结果
      write("\n" + stat[3] + " names are used by " + stat[2] + " files"
          + range + " times. Heap savings ~" + stat[1] + " bytes.");
      // 更新下一个分箱的区间上界
      highbound = (int) stat[0] - 1;
    }
    // 输出全局总节省堆内存
    write("\n\nTotal saved heap ~" + totalsavings + "bytes.\n");
    super.finish();
  }

  /**
   * 处理访问到的fsimage元素，收集INode路径中的文件名信息并统计
   * @param element 访问到的图像元素类型
   * @param value 元素对应的值
   * @throws IOException 写入过程出错
   */
  @Override
  void visit(ImageElement element, String value) throws IOException {
    // 仅处理INode路径元素
    if (element == ImageElement.INODE_PATH) {
      // 从完整路径中提取出文件名（最后一个/后的部分）
      String filename = value.substring(value.lastIndexOf("/") + 1);
      // 更新文件名出现次数统计
      if (counts.containsKey(filename)) {
        counts.put(filename, counts.get(filename) + 1);
      } else {
        counts.put(filename, 1);
      }
    }
  }

  @Override
  void leaveEnclosingElement() throws IOException {
  }

  @Override
  void start() throws IOException {
  }

  @Override
  void visitEnclosingElement(ImageElement element) throws IOException {
  }

  @Override
  void visitEnclosingElement(ImageElement element, ImageElement key,
      String value) throws IOException {
  }
}