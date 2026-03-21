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

package org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao;

import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGContext;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator;
import org.apache.hadoop.yarn.util.YarnVersionInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 全局策略生成器(GPG)信息数据对象，用于Web REST API返回版本和启动信息。
 * 封装GPG自身和Hadoop的版本信息，以及GPG服务启动时间。
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class GpgInfo {
  private String gpgVersion;
  private String gpgBuildVersion;
  private String gpgVersionBuiltOn;
  private String hadoopVersion;
  private String hadoopBuildVersion;
  private String hadoopVersionBuiltOn;
  private long gpgStartupTime;

  /**
   * JAXB要求的无参构造方法，用于序列化/反序列化。
   */
  public GpgInfo() {
  } // JAXB needs this

  /**
   * 构造GPG信息对象，从上下文获取并填充版本和启动时间信息。
   * @param context GPG上下文对象
   */
  public GpgInfo(final GPGContext context) {
    this.gpgVersion = YarnVersionInfo.getVersion();
    this.gpgBuildVersion = YarnVersionInfo.getBuildVersion();
    this.gpgVersionBuiltOn = YarnVersionInfo.getDate();
    this.hadoopVersion = VersionInfo.getVersion();
    this.hadoopBuildVersion = VersionInfo.getBuildVersion();
    this.hadoopVersionBuiltOn = VersionInfo.getDate();
    this.gpgStartupTime = GlobalPolicyGenerator.getGPGStartupTime();
  }

  public String getGpgVersion() {
    return gpgVersion;
  }

  public String getGpgBuildVersion() {
    return gpgBuildVersion;
  }

  public String getGpgVersionBuiltOn() {
    return gpgVersionBuiltOn;
  }

  public String getHadoopVersion() {
    return hadoopVersion;
  }

  public String getHadoopBuildVersion() {
    return hadoopBuildVersion;
  }

  public String getHadoopVersionBuiltOn() {
    return hadoopVersionBuiltOn;
  }

  public long getGpgStartupTime() {
    return gpgStartupTime;
  }
}