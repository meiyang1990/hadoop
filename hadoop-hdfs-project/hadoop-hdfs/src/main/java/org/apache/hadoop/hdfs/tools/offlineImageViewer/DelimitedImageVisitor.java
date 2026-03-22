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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * 文件级注释：HDFS fsimage离线查看工具的分隔符输出访问者，将fsimage转换为带分隔符的文本格式
 * 
 * 本类将fsimage中的inode信息输出为文本，每个字段用指定分隔符分隔，包含普通inode和构建中inode的公共字段。
 * 对于旧版本fsimage中不存在的字段（如访问时间），输出会保留对应列但留空。
 * 当前不输出每个文件的具体块信息。默认分隔符为制表符，避免与路径文本冲突，可通过构造函数自定义分隔符。
 */
class DelimitedImageVisitor extends TextWriterImageVisitor {
  private static final String defaultDelimiter = "\t"; 
  
  // 存储当前处理的嵌套元素栈，用于跟踪元素层级
  final private LinkedList<ImageElement> elemQ = new LinkedList<ImageElement>();
  // 累计当前文件的总大小（所有块大小之和）
  private long fileSize = 0l;
  // 需要采集并输出的fsimage元素列表，同时决定输出顺序
  private final Collection<ImageElement> elementsToTrack;
  // 存储当前inode各采集元素的值
  private final AbstractMap<ImageElement, String> elements = 
                                            new HashMap<ImageElement, String>();
  // 字段分隔符
  private final String delimiter;

  {
    elementsToTrack = new ArrayList<ImageElement>();
    
    // 该列表决定需要输出哪些字段，以及输出顺序
    Collections.addAll(elementsToTrack,  ImageElement.INODE_PATH,
                                         ImageElement.REPLICATION,
                                         ImageElement.MODIFICATION_TIME,
                                         ImageElement.ACCESS_TIME,
                                         ImageElement.BLOCK_SIZE,
                                         ImageElement.NUM_BLOCKS,
                                         ImageElement.NUM_BYTES,
                                         ImageElement.NS_QUOTA,
                                         ImageElement.DS_QUOTA,
                                         ImageElement.PERMISSION_STRING,
                                         ImageElement.USER_NAME,
                                         ImageElement.GROUP_NAME);
  }
  
  /**
   * 构造函数，使用默认分隔符输出到指定文件
   * @param filename 输出文件路径
   * @throws IOException 输出文件创建失败时抛出
   */
  public DelimitedImageVisitor(String filename) throws IOException {
    this(filename, false);
  }

  /**
   * 构造函数，使用默认分隔符，可选择输出到屏幕
   * @param outputFile 输出文件路径
   * @param printToScreen 是否打印到屏幕，true打印到屏幕，false输出到文件
   * @throws IOException 输出文件创建失败时抛出
   */
  public DelimitedImageVisitor(String outputFile, boolean printToScreen) 
                                                           throws IOException {
    this(outputFile, printToScreen, defaultDelimiter);
  }
  
  /**
   * 构造函数，可自定义分隔符和输出位置
   * @param outputFile 输出文件路径
   * @param printToScreen 是否打印到屏幕
   * @param delimiter 自定义字段分隔符
   * @throws IOException 输出文件创建失败时抛出
   */
  public DelimitedImageVisitor(String outputFile, boolean printToScreen, 
                               String delimiter) throws IOException {
    super(outputFile, printToScreen);
    this.delimiter = delimiter;
    reset();
  }

  /**
   * 重置当前采集的元素值和文件大小，准备处理下一个inode
   */
  private void reset() {
    elements.clear();
    for(ImageElement e : elementsToTrack) 
      elements.put(e, null);
    
    fileSize = 0l;
  }
  
  @Override
  void leaveEnclosingElement() throws IOException {
    // 弹出当前处理完的闭合元素
    ImageElement elem = elemQ.pop();

    // 处理完一个inode（普通或构建中），输出结果并重置状态
    if(elem == ImageElement.INODE || 
       elem == ImageElement.INODE_UNDER_CONSTRUCTION) {
      writeLine();
      write("\n");
      reset();
    }
  }

  /**
   * 将当前采集的所有字段按顺序输出，使用分隔符分隔
   * @throws IOException 写入失败时抛出
   */
  private void writeLine() throws IOException {
    Iterator<ImageElement> it = elementsToTrack.iterator();
    
    while(it.hasNext()) {
      ImageElement e = it.next();
      
      String v = null;
      // 文件大小使用累计计算的结果
      if(e == ImageElement.NUM_BYTES)
        v = String.valueOf(fileSize);
      else
        v = elements.get(e);
      
      // 输出非空值
      if(v != null)
        write(v);
      
      // 最后一个字段后不加分隔符
      if(it.hasNext())
        write(delimiter);
    }
  }

  @Override
  void visit(ImageElement element, String value) throws IOException {
    // 根inode路径为空，显式替换为/
    if(element == ImageElement.INODE_PATH && value.equals(""))
      value = "/";
    
    // 文件大小需要累加每个块的大小
    if(element == ImageElement.NUM_BYTES)
      fileSize += Long.parseLong(value);
    
    // 存储需要采集的元素值（NUM_BYTES单独处理不存储）
    if(elements.containsKey(element) && element != ImageElement.NUM_BYTES)
      elements.put(element, value);
    
  }

  @Override
  void visitEnclosingElement(ImageElement element) throws IOException {
    // 将嵌套元素压入栈
    elemQ.push(element);
  }

  @Override
  void visitEnclosingElement(ImageElement element, ImageElement key,
      String value) throws IOException {
    // 块数量作为blocks元素的属性需要单独处理
    if(key == ImageElement.NUM_BLOCKS 
        && elements.containsKey(ImageElement.NUM_BLOCKS))
      elements.put(key, value);
    
    // 将嵌套元素压入栈
    elemQ.push(element);
  }
  
  @Override
  void start() throws IOException { /* 启动阶段无需操作 */ }
}