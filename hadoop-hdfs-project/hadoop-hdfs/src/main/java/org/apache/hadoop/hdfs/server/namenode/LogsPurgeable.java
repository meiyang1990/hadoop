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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Collection;

/**
 * 可清理编辑日志的抽象接口，为需要清理过期编辑日志的类提供统一抽象
 * HDFS NameNode编辑日志不同实现类（文件日志、仲裁日志）通过实现该接口支持日志清理操作
 */
interface LogsPurgeable {
  
  /**
   * 清理所有事务ID小于指定值的过期编辑日志
   * 
   * @param minTxIdToKeep 需要保留的最小事务ID，小于该ID的日志会被删除
   * @throws IOException 清理过程中发生IO异常时抛出
   */
  public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException;
  
  /**
   * 筛选出从指定事务ID开始的编辑日志输入流集合，从包含起始事务ID的日志开始一直覆盖到当前日志末尾
   * 
   * @param streams 用于存放筛选出的编辑日志输入流的集合
   * @param fromTxId 需要读取的起始事务ID
   * @param inProgressOk 是否允许返回正在写入中的未完成日志流
   * @param onlyDurableTxns 是否只限制返回已持久化的事务，持久化事务在QJM中是已提交事务ID，在FJM中是已写入文件的最大事务ID
   * @throws IOException 底层存储访问错误或不可访问时抛出
   */
  void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxId, boolean inProgressOk, boolean onlyDurableTxns)
      throws IOException;
  
}