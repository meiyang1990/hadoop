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
package org.apache.hadoop.hdfs.server.balancer;

/**
 * HDFS数据均衡服务Balancer的JMX管理接口，
 * 提供Balancer服务运行版本、编译信息等指标的暴露能力，供监控系统集成。
 */
public interface BalancerMXBean {

  /**
   * 获取Hadoop版本信息。
   *
   * @return Hadoop版本字符串
   */
  String getVersion();

  /**
   * 获取当前运行Balancer服务的软件版本信息。
   *
   * @return 代表Balancer版本的字符串
   */
  String getSoftwareVersion();

  /**
   * 获取Balancer编译信息，包含编译日期、编译用户、代码分支等信息。
   *
   * @return JSON格式的编译信息字符串
   */
  String getCompileInfo();

}