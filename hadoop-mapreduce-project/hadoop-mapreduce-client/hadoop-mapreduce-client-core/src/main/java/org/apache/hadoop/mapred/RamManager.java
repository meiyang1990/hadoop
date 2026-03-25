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
package org.apache.hadoop.mapred;

import java.io.InputStream;

/**
 * 内存池管理器接口，负责管理固定上限的Map任务内存池，为MapReduce shuffle阶段提供内存分配与回收能力
 */
interface RamManager {
  /**
   * 为指定输入流的数据处理预留内存空间
   * 
   * @param requestedSize 请求分配的内存大小
   * @param in 需要处理数据的输入流
   * @throws InterruptedException 内存等待过程中被中断时抛出
   * @return <code>true</code> 如果内存立即分配成功，<code>false</code> 如果需要等待内存释放
   */
  boolean reserve(int requestedSize, InputStream in) 
  throws InterruptedException;
  
  /**
   * 将使用完毕的内存归还到内存池
   * 
   * @param requestedSize 需要归还的内存大小
   */
  void unreserve(int requestedSize);
}