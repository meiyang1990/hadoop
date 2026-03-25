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
package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.nativetask.util.ReadWriteBuffer;

/**
 * 文件: org.apache.hadoop.mapred.nativetask.INativeHandler
 * 模块: Hadoop MapReduce 本地任务模块
 * 职责: 定义Java层与C++本地代码交互的处理器接口，支持命令和数据在跨层之间传输
 */
/**
 * 本地处理器接口，可接收输入并产生输出，用于Java层与本地C++层之间传输命令和数据
 */
@InterfaceAudience.Private
public interface INativeHandler extends NativeDataTarget, NativeDataSource {

  /**
   * 获取当前处理器的名称
   * @return 处理器名称字符串
   */
  public String name();

  /**
   * 获取本地C++层对应处理器的内存地址指针
   * @return 本地处理器指针的长整型表示
   */
  public long getNativeHandler();

  /**
   * 初始化本地处理器，加载配置完成准备工作
   * @param conf Hadoop作业配置对象
   * @throws IOException 初始化过程中发生IO异常时抛出
   */
  public void init(Configuration conf) throws IOException;

  /**
   * 关闭本地处理器，释放占用的资源
   * @throws IOException 关闭过程中发生IO异常时抛出
   */
  public void close() throws IOException;

  /**
   * 向下游组件调用执行指定命令
   * @param command 待执行的命令对象
   * @param parameter 命令参数缓冲区
   * @return 命令执行结果缓冲区
   * @throws IOException 命令调用过程中发生IO异常时抛出
   */
  public ReadWriteBuffer call(Command command, ReadWriteBuffer parameter) throws IOException;

  /**
   * 设置命令分发器，用于处理来自本地层的命令请求
   * @param handler 命令分发器实例
   */
  void setCommandDispatcher(CommandDispatcher handler);

}