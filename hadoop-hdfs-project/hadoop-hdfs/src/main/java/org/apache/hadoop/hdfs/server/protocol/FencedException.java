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
package org.apache.hadoop.hdfs.server.protocol;

import java.io.IOException;

/**
 * HDFS 服务端协议异常：共享资源被其他节点隔离后原持有者再次访问时抛出
 * <p>
 * 核心应用场景是 HDFS 高可用（HA）架构中，当备 NameNode 切换为主节点后，
 * 会对旧主 NameNode 执行隔离（fencing）操作，若旧主 NameNode 尝试再次访问
 * 共享存储（比如共享编辑日志目录），则抛出该异常阻止访问，避免脑裂问题。
 * </p>
 */
public class FencedException extends IOException {
  private static final long serialVersionUID = 1L;
  
  /**
   * 构造带错误信息的隔离异常实例
   * @param errorMsg 异常描述信息
   */
  public FencedException(String errorMsg) {
    super(errorMsg);
  }
}