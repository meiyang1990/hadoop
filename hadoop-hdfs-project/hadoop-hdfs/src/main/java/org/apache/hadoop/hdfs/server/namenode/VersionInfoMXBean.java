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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * NameNode版本信息MXBean接口，提供JMX方式访问NameNode版本编译信息
 * 用于监控系统通过JMX接口获取当前NameNode实例的版本和编译元数据
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface VersionInfoMXBean {
  /**
   * 获取编译信息，包含编译日期、编译用户、分支信息
   * @return 编译信息字符串
   */
  public String getCompileInfo();

  /**
   * 获取Hadoop软件版本号
   * @return Hadoop版本号字符串
   */
  public String getSoftwareVersion();
}