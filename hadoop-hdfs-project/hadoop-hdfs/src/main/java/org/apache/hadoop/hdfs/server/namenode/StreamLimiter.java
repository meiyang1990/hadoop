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

/**
 * 流读取字节数限制接口，用于对输入流的读取总量进行限制，防止读取过量数据
 * 限制表示读取超过该字节数会抛出异常，用于NameNode防护恶意超大读取请求
 */
interface StreamLimiter {
  /**
   * 设置流读取字节数限制，调用此方法会清除之前已存在的限制
   * @param limit 允许读取的最大字节数
   */
  public void setLimit(long limit);
  
  /**
   * 清除流读取字节数限制，取消流量限制，允许无限制读取
   */
  public void clearLimit();
}