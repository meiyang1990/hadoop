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
import java.util.Formatter;
import java.util.LinkedList;

/**
 * HDFS fsimage离线查看器的ls格式输出访问器，以类似ls/lsr命令的格式展示命名空间中的文件目录信息。
 * 输出包含条目类型（目录/文件）、权限、副本数、用户名、组名、文件大小、修改时间和完整路径。
 * 
 * 注意：本工具输出与实际lsr命令输出的一个重要差异是无法对条目进行排序，
 * 条目输出顺序与fsimage文件中的存储顺序一致，因此不能直接与在线lsr命令输出对比。
 */
class LsImageVisitor extends TextWriterImageVisitor {
  // 存储解析过程中的元素栈，跟踪当前在fsimage树中的位置
  final private LinkedList<ImageElement> elemQ = new LinkedList<ImageElement>();

  private int numBlocks;
  private String perms;
  private int replication;
  private String username;
  private String group;
  private long filesize;
  private String modTime;
  private String path;
  private String linkTarget;

  // 当前是否正在处理一个INode节点
  private boolean inInode = false;
  final private StringBuilder sb = new StringBuilder();
  final private Formatter formatter = new Formatter(sb);

  /**
   * 构造函数，将结果输出到指定文件
   * @param filename 输出文件名
   * @throws IOException 打开文件失败时抛出异常
   */
  public LsImageVisitor(String filename) throws IOException {
    super(filename);
  }

  /**
   * 构造函数，可指定是否输出到屏幕
   * @param filename 输出文件名
   * @param printToScreen 是否同时打印到屏幕
   * @throws IOException 打开文件失败时抛出异常
   */
  public LsImageVisitor(String filename, boolean printToScreen) throws IOException {
    super(filename, printToScreen);
  }

  /**
   * 初始化新一行输出，重置所有INode信息字段
   */
  private void newLine() {
    numBlocks = 0;
    perms = username = group = path = linkTarget = "";
    filesize = 0l;
    replication = 0;

    inInode = true;
  }

  // 格式化输出宽度常量
  private final static int widthRepl = 2;  
  private final static int widthUser = 8; 
  private final static int widthGroup = 10; 
  private final static int widthSize = 10;
  private final static int widthMod = 10;
  // ls格式输出模板
  private final static String lsStr = " %" + widthRepl + "s %" + widthUser + 
                                       "s %" + widthGroup + "s %" + widthSize +
                                       "d %" + widthMod + "s %s";
  /**
   * 收集完INode所有信息后，按照ls格式输出一行数据
   * @throws IOException 写入输出失败时抛出异常
   */
  private void printLine() throws IOException {
    // 添加条目类型标记：d表示目录，-表示文件
    sb.append(numBlocks < 0 ? "d" : "-");
    // 添加权限字符串
    sb.append(perms);

    // 如果是符号链接，拼接目标路径
    if (0 != linkTarget.length()) {
      path = path + " -> " + linkTarget; 
    }
    // 按照固定格式格式化输出各个字段
    formatter.format(lsStr, replication > 0 ? replication : "-",
                           username, group, filesize, modTime, path);
    sb.append("\n");

    // 写入结果并清空字符串Builder
    write(sb.toString());
    sb.setLength(0);

    // 标记INode处理完成
    inInode = false;
  }

  /**
   * 访问开始处理，初始化操作
   */
  @Override
  void start() throws IOException {}

  /**
   * 访问正常完成，执行收尾操作
   */
  @Override
  void finish() throws IOException {
    super.finish();
  }

  /**
   * 访问异常中断处理，输出提示信息
   */
  @Override
  void finishAbnormally() throws IOException {
    System.out.println("Input ended unexpectedly.");
    super.finishAbnormally();
  }

  /**
   * 离开闭合元素时的处理逻辑
   */
  @Override
  void leaveEnclosingElement() throws IOException {
    // 弹出当前处理的元素
    ImageElement elem = elemQ.pop();

    // 如果离开的是INode元素，则输出整行信息
    if(elem == ImageElement.INODE)
      printLine();
  }

  /**
   * 处理当前元素，维护解析状态并收集INode输出所需的字段信息
   */
  @Override
  void visit(ImageElement element, String value) throws IOException {
    if(inInode) {
      // 根据元素类型保存对应字段值
      switch(element) {
      case INODE_PATH:
        // 根路径特殊处理
        if(value.equals("")) path = "/";
        else path = value;
        break;
      case PERMISSION_STRING:
        perms = value;
        break;
      case REPLICATION:
        replication = Integer.parseInt(value);
        break;
      case USER_NAME:
        username = value;
        break;
      case GROUP_NAME:
        group = value;
        break;
      case NUM_BYTES:
        filesize += Long.parseLong(value);
        break;
      case MODIFICATION_TIME:
        modTime = value;
        break;
      case SYMLINK:
        linkTarget = value;
        break;
      default:
        // 不需要处理未关心的元素，跳过即可
        break;
      }
    }
  }

  /**
   * 进入闭合元素时的处理逻辑
   */
  @Override
  void visitEnclosingElement(ImageElement element) throws IOException {
    // 将元素压入栈，维护当前解析位置
    elemQ.push(element);
    // 如果进入INode元素，初始化新行准备收集信息
    if(element == ImageElement.INODE)
      newLine();
  }

  /**
   * 进入带键值的闭合元素时的处理逻辑
   */
  @Override
  void visitEnclosingElement(ImageElement element,
      ImageElement key, String value) throws IOException {
    // 将元素压入栈，维护当前解析位置
    elemQ.push(element);
    if(element == ImageElement.INODE)
      // 如果进入INode元素，初始化新行准备收集信息
      newLine();
    else if (element == ImageElement.BLOCKS)
      // 保存块数量信息，负数标识目录
      numBlocks = Integer.parseInt(value);
  }
}