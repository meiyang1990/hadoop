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
import java.util.Date;

/**
 * 用于离线FSImage查看器，通过缩进层级展示FSImage结构的访问者实现
 * 将FSImage的树形结构以缩进文本形式格式化输出，便于人工阅读
 */
class IndentedImageVisitor extends TextWriterImageVisitor {
  
  /**
   * 构造函数，指定输出文件名
   * @param filename 输出文件路径
   * @throws IOException 文件创建/写入异常
   */
  public IndentedImageVisitor(String filename) throws IOException {
    super(filename);
  }

  /**
   * 构造函数，指定输出文件名和是否输出到控制台
   * @param filename 输出文件路径
   * @param printToScreen 是否同时将结果输出到控制台
   * @throws IOException 文件创建/写入异常
   */
  public IndentedImageVisitor(String filename, boolean printToScreen) throws IOException {
    super(filename, printToScreen);
  }

  // 深度计数器，用于跟踪当前缩进层级
  final private DepthCounter dc = new DepthCounter();

  @Override
  void start() throws IOException {}

  @Override
  void finish() throws IOException { super.finish(); }

  /**
   * 处理异常终止的FSImage处理流程
   * @throws IOException 写入异常
   */
  @Override
  void finishAbnormally() throws IOException {
    System.out.println("*** Image processing finished abnormally.  Ending ***");
    super.finishAbnormally();
  }

  /**
   * 离开闭合元素时，减少缩进层级
   * @throws IOException 写入异常
   */
  @Override
  void leaveEnclosingElement() throws IOException {
    dc.decLevel();
  }

  /**
   * 访问字符串值类型的FSImage元素，输出缩进格式的元素
   * @param element FSImage元素类型
   * @param value 元素值
   * @throws IOException 写入异常
   */
  @Override
  void visit(ImageElement element, String value) throws IOException {
    printIndents();
    write(element + " = " + value + "\n");
  }

  /**
   * 访问长整型值类型的FSImage元素，对时间类型元素转换为可读日期格式
   * @param element FSImage元素类型
   * @param value 长整型元素值
   * @throws IOException 写入异常
   */
  @Override
  void visit(ImageElement element, long value) throws IOException {
    if ((element == ImageElement.DELEGATION_TOKEN_IDENTIFIER_EXPIRY_TIME) || 
        (element == ImageElement.DELEGATION_TOKEN_IDENTIFIER_ISSUE_DATE) || 
        (element == ImageElement.DELEGATION_TOKEN_IDENTIFIER_MAX_DATE)) {
      visit(element, new Date(value).toString());
    } else {
      visit(element, Long.toString(value));
    }
  }
  
  /**
   * 访问无额外属性的闭合元素，输出元素名称并增加缩进层级
   * @param element 闭合元素类型
   * @throws IOException 写入异常
   */
  @Override
  void visitEnclosingElement(ImageElement element) throws IOException {
    printIndents();
    write(element + "\n");
    dc.incLevel();
  }

  /**
   * 访问带键值属性的闭合元素，输出元素及属性并增加缩进层级
   * @param element 闭合元素类型
   * @param key 属性键
   * @param value 属性值
   * @throws IOException 写入异常
   */
  @Override
  void visitEnclosingElement(ImageElement element,
      ImageElement key, String value)
      throws IOException {
    printIndents();
    write(element + " [" + key + " = " + value + "]\n");
    dc.incLevel();
  }

  /**
   * 预缓存的缩进字符串，避免重复创建字符串提升输出性能
   * 缓存最深7层缩进，超过后动态生成
   */
  final private static String [] indents = { "",
                                             "  ",
                                             "    ",
                                             "      ",
                                             "        ",
                                             "          ",
                                             "            "};
  /**
   * 根据当前层级输出对应数量的缩进空格
   * @throws IOException 写入异常
   */
  private void printIndents() throws IOException {
    try {
      write(indents[dc.getLevel()]);
    } catch (IndexOutOfBoundsException e) {
      // 超出预缓存层级时，动态生成缩进
      for(int i = 0; i < dc.getLevel(); i++)
        write(" ");
    }
   }
}