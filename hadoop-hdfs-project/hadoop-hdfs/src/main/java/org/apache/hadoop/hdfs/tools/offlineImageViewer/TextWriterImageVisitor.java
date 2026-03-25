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
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;


/**
 * 离线FsImage镜像查看工具的文本写入抽象访问者基类，为具体实现类提供基础的文本文件输出能力
 * 核心职责：封装输出文件的打开、关闭和写入逻辑，子类只需负责按格式生成输出内容，无需处理IO
 * 子类必须正确调用父类的构造方法、finish和finishAbnormally方法，以保证文件正确打开和关闭
 * 本类不会自动在输出内容后添加换行符，换行处理由具体实现类负责
 */
abstract class TextWriterImageVisitor extends ImageVisitor {
  private boolean printToScreen = false;
  private boolean okToWrite = false;
  final private OutputStreamWriter fw;

  /**
   * 构造仅输出到指定文件的文本写入访问者
   *
   * @param filename 输出文件路径
   */
  public TextWriterImageVisitor(String filename) throws IOException {
    this(filename, false);
  }

  /**
   * 构造可同时输出到文件和屏幕的文本写入访问者
   *
   * @param filename 输出文件路径
   * @param printToScreen 是否同时将输出镜像到控制台打印
   */
  public TextWriterImageVisitor(String filename, boolean printToScreen)
         throws IOException {
    super();
    this.printToScreen = printToScreen;
    // 打开输出文件，使用UTF-8编码
    fw = new OutputStreamWriter(Files.newOutputStream(Paths.get(filename)),
        StandardCharsets.UTF_8);
    // 标记文件已打开，可以写入
    okToWrite = true;
  }
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.hdfs.tools.offlineImageViewer.ImageVisitor#finish()
   */
  @Override
  void finish() throws IOException {
    // 正常完成访问流程，关闭输出流
    close();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hdfs.tools.offlineImageViewer.ImageVisitor#finishAbnormally()
   */
  @Override
  void finishAbnormally() throws IOException {
    // 异常终止访问流程，依然关闭输出流保证资源释放
    close();
  }

  /**
   * 关闭输出流，禁止后续写入操作，释放IO资源
   */
  private void close() throws IOException {
    fw.close();
    okToWrite = false;
  }

  /**
   * 将指定文本写入输出文件，若开启屏幕镜像则同步打印到控制台
   *
   * @param toWrite 待写入的文本内容
   */
  protected void write(String toWrite) throws IOException  {
    // 检查文件是否处于可写入状态
    if(!okToWrite)
      throw new IOException("file not open for writing.");

    // 如果开启屏幕镜像，输出到控制台
    if(printToScreen)
      System.out.print(toWrite);

    try {
      // 写入文本到输出文件
      fw.write(toWrite);
    } catch (IOException e) {
      // 写入失败后标记不可再写入
      okToWrite = false;
      throw e;
    }
  }
}