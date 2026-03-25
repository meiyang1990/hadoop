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

package org.apache.hadoop.mapred.pipes;

import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Hadoop Pipes框架中，从C++子任务进程向上层Java Task传递消息的上行协议接口
 * 所有调用都是异步的，会在消息处理完成前返回，不阻塞调用方
 * @param <K> 输出键类型
 * @param <V> 输出值类型
 */
interface UpwardProtocol<K extends WritableComparable, V extends Writable> {
  /**
   * 输出子任务处理后的键值对结果
   * @param key 输出记录的键
   * @param value 输出记录的值
   * @throws IOException 传输或IO异常
   */
  void output(K key, V value) throws IOException;
  
  /**
   * 输出带有自定义分区结果的键值对，用于用户自定义了分区函数的Map任务
   * @param reduce 该记录要发送到的Reduce编号
   * @param key 输出记录的键
   * @param value 输出记录的值
   * @throws IOException 传输或IO异常
   */
  void partitionedOutput(int reduce, K key, 
                         V value) throws IOException;
  
  /**
   * 更新任务状态信息，展示给用户
   * @param msg 要展示给用户的状态消息
   * @throws IOException 传输或IO异常
   */
  void status(String msg) throws IOException;
  
  /**
   * 汇报任务当前进度
   * @param progress 当前进度值，范围0.0到1.0
   * @throws IOException 传输或IO异常
   */
  void progress(float progress) throws IOException;
  
  /**
   * 汇报应用已成功完成所有输入处理
   * @throws IOException 传输或IO异常
   */
  void done() throws IOException;
  
  /**
   * 汇报应用执行或通信失败
   * @param e 失败异常
   */
  void failed(Throwable e);
  
  /**
   * 注册一个新的计量计数器
   * @param id 计数器唯一ID
   * @param group 计数器分组
   * @param name 计数器名称
   * @throws IOException 传输或IO异常
   */
  void registerCounter(int id, String group, String name) throws IOException;
  
  /**
   * 增加已注册计数器的数值
   * @param id 已注册计数器的ID
   * @param amount 要增加的数值
   * @throws IOException 传输或IO异常
   */
  void incrementCounter(int id, long amount) throws IOException;

  /**
   * 处理来自客户端的认证响应，通知等待认证结果的线程
   * @param digest 认证摘要
   * @return 认证成功返回true，否则返回false
   * @throws IOException 传输或IO异常
   */
  boolean authenticate(String digest) throws IOException;

}