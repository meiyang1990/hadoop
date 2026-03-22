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
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.hadoop.hdfs.util.XMLUtils;

/**
 * 文件级注释：HDFS离线fsimage查看器的XML格式输出访问器，将fsimage结构转换为等价的XML文档输出
 * 
 * 遍历HDFS fsimage结构，将fsimage中的所有组件转换为XML格式文档输出，用于离线分析fsimage
 */
public class XmlImageVisitor extends TextWriterImageVisitor {
  // 保存当前打开的XML标签栈，用于维护嵌套层级关系
  final private Deque<ImageElement> tagQ = new ArrayDeque<>();

  /**
   * 构造函数，通过文件路径创建XML格式fsimage访问器
   * @param filename 输出XML文件路径
   * @throws IOException 写入文件异常
   */
  public XmlImageVisitor(String filename) throws IOException {
    super(filename, false);
  }

  /**
   * 构造函数，通过文件路径创建XML格式fsimage访问器，支持输出到控制台
   * @param filename 输出XML文件路径
   * @param printToScreen 是否同时输出到控制台
   * @throws IOException 写入文件异常
   */
  public XmlImageVisitor(String filename, boolean printToScreen)
       throws IOException {
    super(filename, printToScreen);
  }

  @Override
  void finish() throws IOException {
    super.finish();
  }

  /**
   * 异常终止处理，输出错误注释后终止访问
   * @throws IOException 写入异常
   */
  @Override
  void finishAbnormally() throws IOException {
    // 在XML中输出处理错误注释
    write("\n<!-- Error processing image file.  Exiting -->\n");
    super.finishAbnormally();
  }

  /**
   * 离开闭合元素，关闭对应XML标签
   * @throws IOException 写入异常
   */
  @Override
  void leaveEnclosingElement() throws IOException {
    // 标签栈为空说明fsimage结构不合法，抛出异常
    if (tagQ.isEmpty()) {
      throw new IOException("Tried to exit non-existent enclosing element " +
          "in FSImage file");
    }

    ImageElement element = tagQ.pop();
    // 写入XML闭合标签
    write("</" + element.toString() + ">\n");
  }

  /**
   * 访问开始，输出XML版本声明
   * @throws IOException 写入异常
   */
  @Override
  void start() throws IOException {
    write("<?xml version=\"1.0\" ?>\n");
  }

  @Override
  void visit(ImageElement element, String value) throws IOException {
    writeTag(element.toString(), value);
  }

  /**
   * 访问开始处理嵌套元素，写入开放XML标签并压入栈
   * @param element 当前嵌套元素
   * @throws IOException 写入异常
   */
  @Override
  void visitEnclosingElement(ImageElement element) throws IOException {
    write('<' + element.toString() + ">\n");
    tagQ.push(element);
  }

  /**
   * 访问开始处理带属性的嵌套元素，写入带属性的开放XML标签并压入栈
   * @param element 当前嵌套元素
   * @param key 属性名
   * @param value 属性值
   * @throws IOException 写入异常
   */
  @Override
  void visitEnclosingElement(ImageElement element,
      ImageElement key, String value)
       throws IOException {
    write('<' + element.toString() + ' ' + key + "=\"" + value +"\">\n");
    tagQ.push(element);
  }

  /**
   * 写入单值XML标签，对特殊字符进行转义处理
   * @param tag 标签名
   * @param value 标签值
   * @throws IOException 写入异常
   */
  private void writeTag(String tag, String value) throws IOException {
    write('<' + tag + '>' +
        XMLUtils.mangleXmlString(value, true) + "</" + tag + ">\n");
  }
}