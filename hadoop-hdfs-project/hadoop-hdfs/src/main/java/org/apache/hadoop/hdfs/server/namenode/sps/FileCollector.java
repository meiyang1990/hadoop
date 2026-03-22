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

package org.apache.hadoop.hdfs.server.namenode.sps;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * HDFS存储策略满足器(SPS)的文件收集接口，负责递归扫描指定目录，收集需要进行块移动的文件。
 * 用于在目录级存储策略变更后，收集目录下所有符合条件的文件，以便后续进行块存储位置调整。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface FileCollector {

  /**
   * 递归扫描指定目录下的所有文件，将需要调整块存储位置的文件收集到块移动任务队列中。
   * 当目录设置了新的存储策略后，通过该方法收集目录下所有需要按新策略移动块的文件。
   *
   * @param path
   *          - 需要扫描的目录的inode编号文件路径ID
   * @throws IOException 扫描目录过程中发生IO异常
   * @throws InterruptedException 扫描过程被中断
   */
  void scanAndCollectFiles(long path)
      throws IOException, InterruptedException;
}