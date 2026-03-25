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
package org.apache.hadoop.hdfs.server.common;

import java.io.File;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;

/**
 * HDFS存储错误报告接口，供JournalManager实现类报告底层存储目录的错误。
 * 通过该接口解耦了日志管理器与其创建者存储模块之间的循环依赖关系。
 */
@InterfaceAudience.Private
public interface StorageErrorReporter {

  /**
   * 报告指定文件发生IO错误。
   * 
   * @param f 发生错误的文件对象
   */
  public void reportErrorOnFile(File f);
}