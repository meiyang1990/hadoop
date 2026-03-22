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
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

/**
 * @file DownwardProtocol.java
 * @brief Pipes框架中从Java端到C++端的下行通信协议接口定义
 * 
 * Hadoop Pipes是Hadoop提供的C++ MapReduce编程支持，本接口定义了Java端
 * （父进程）向C++端（子进程）发送命令和数据的所有下行方法，所有调用均为异步，
 * 会在消息处理完成前返回。
 */
interface DownwardProtocol<K extends WritableComparable, V extends Writable> {
  /**
   * 发送身份认证请求
   * @param digest 认证摘要
   * @param challenge 挑战码
   * @throws IOException 通信IO异常
   */
  void authenticate(String digest, String challenge) throws IOException;
  
  /**
   * 启动通信，初始化C++端任务
   * @throws IOException 通信IO异常
   */
  void start() throws IOException;
  
  /**
   * 向C++端发送任务配置信息
   * @param conf 作业配置对象
   * @throws IOException 通信IO异常
   */
  void setJobConf(JobConf conf) throws IOException;
  
  /**
   * 配置Map任务的输入键值对类型
   * @param keyType 键类型的类全名称
   * @param valueType 值类型的类全名称
   * @throws IOException 通信IO异常
   */
  void setInputTypes(String keyType, String valueType) throws IOException;
  
  /**
   * 通知C++端启动Map任务
   * @param split 本次Map任务的输入分片
   * @param numReduces 作业的Reducer数量
   * @param pipedInput 输入是否由Java端管道传递（否则C++直接读取HDFS）
   * @throws IOException 通信IO异常
   */
  void runMap(InputSplit split, int numReduces, 
              boolean pipedInput) throws IOException;
  
  /**
   * 向管道输入模式的Map任务发送一条键值对记录
   * @param key 记录键
   * @param value 记录值
   * @throws IOException 通信IO异常
   */
  void mapItem(K key, V value) throws IOException;
  
  /**
   * 通知C++端启动Reduce任务
   * @param reduce 当前Reduce任务的索引（0 ~ numReduces - 1）
   * @param pipedOutput 输出是否通过管道发送回Java端（否则C++直接写入HDFS）
   * @throws IOException 通信IO异常
   */
  void runReduce(int reduce, boolean pipedOutput) throws IOException;
  
  /**
   * 向Reduce任务发送一个分组的新键
   * @param key 分组键
   * @throws IOException 通信IO异常
   */
  void reduceKey(K key) throws IOException;
  
  /**
   * 向当前分组发送一个新值
   * @param value 分组值
   * @throws IOException 通信IO异常
   */
  void reduceValue(V value) throws IOException;
  
  /**
   * 通知任务输入已结束，完成剩余数据处理
   * @throws IOException 通信IO异常
   */
  void endOfInput() throws IOException;
  
  /**
   * 通知任务发生错误，尽快中止执行
   * @throws IOException 通信IO异常
   */
  void abort() throws IOException;
  
  /**
   * 刷新所有缓冲数据，确保发送完成
   * @throws IOException 通信IO异常
   */
  void flush() throws IOException;
  
  /**
   * 关闭通信连接，释放资源
   * @throws IOException 通信IO异常
   * @throws InterruptedException 中断异常
   */
  void close() throws IOException, InterruptedException;
}