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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * HDFS版本不匹配异常，当外部组件版本与当前应用版本不兼容时抛出
 * 通常用于HDFS不同节点之间版本协商检查，确保集群节点版本兼容
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class IncorrectVersionException extends IOException {
  private static final long serialVersionUID = 1L;
  
  /**
   * 构造函数，使用自定义异常信息创建版本不匹配异常
   * @param message 异常描述信息
   */
  public IncorrectVersionException(String message) {
    super(message);
  }

  /**
   * 构造函数，基于远端守护进程版本过低场景创建异常
   * @param minimumVersion 当前节点要求的远端最低兼容版本
   * @param reportedVersion 远端实际上报的版本
   * @param remoteDaemon 远端守护进程名称
   * @param thisDaemon 当前本地守护进程名称
   */
  public IncorrectVersionException(String minimumVersion, String reportedVersion,
      String remoteDaemon, String thisDaemon) {
    this("The reported " + remoteDaemon + " version is too low to communicate" +
        " with this " + thisDaemon + ". " + remoteDaemon + " version: '" +
        reportedVersion + "' Minimum " + remoteDaemon + " version: '" +
        minimumVersion + "'");
  }
  
  /**
   * 构造函数，基于布局版本不匹配场景创建异常
   * @param currentLayoutVersion 当前期望的布局版本
   * @param versionReported 实际上报的布局版本
   * @param ofWhat 版本所属对象描述
   */
  public IncorrectVersionException(int currentLayoutVersion,
      int versionReported, String ofWhat) {
    this(versionReported, ofWhat, currentLayoutVersion);
  }
  
  /**
   * 构造函数，基于数字版本号不匹配场景创建异常
   * @param versionReported 实际上报的版本号
   * @param ofWhat 版本所属对象描述
   * @param versionExpected 期望的版本号
   */
  public IncorrectVersionException(int versionReported,
                                   String ofWhat,
                                   int versionExpected) {
    this("Unexpected version " 
        + (ofWhat==null ? "" : "of " + ofWhat) + ". Reported: "
        + versionReported + ". Expecting = " + versionExpected + ".");
  }

}