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

package org.apache.hadoop.mapreduce.v2.hs.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer;
import org.apache.hadoop.util.VersionInfo;

/**
 * 作业历史服务器信息数据传输对象，用于Web REST API返回历史服务器基本信息
 * 包含服务器启动时间和Hadoop版本构建信息，供前端UI展示
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class HistoryInfo {

  protected long startedOn;
  protected String hadoopVersion;
  protected String hadoopBuildVersion;
  protected String hadoopVersionBuiltOn;

  /**
   * 构造历史信息对象，从全局获取历史服务器启动时间和版本信息
   */
  public HistoryInfo() {
    this.startedOn = JobHistoryServer.historyServerTimeStamp;
    this.hadoopVersion = VersionInfo.getVersion();
    this.hadoopBuildVersion = VersionInfo.getBuildVersion();
    this.hadoopVersionBuiltOn = VersionInfo.getDate();
  }

  /**
   * 获取Hadoop版本号
   * @return Hadoop版本字符串
   */
  public String getHadoopVersion() {
    return this.hadoopVersion;
  }

  /**
   * 获取Hadoop构建版本信息
   * @return 包含commit哈希的构建版本字符串
   */
  public String getHadoopBuildVersion() {
    return this.hadoopBuildVersion;
  }

  /**
   * 获取Hadoop版本构建日期
   * @return 构建日期字符串
   */
  public String getHadoopVersionBuiltOn() {
    return this.hadoopVersionBuiltOn;
  }

  /**
   * 获取历史服务器启动时间戳
   * @return 启动时间毫秒时间戳
   */
  public long getStartedOn() {
    return this.startedOn;
  }

}