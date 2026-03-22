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

package org.apache.hadoop.mapreduce.v2.api;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.KerberosInfo;

/**
 * 历史服务器管理刷新协议，定义了历史服务端运行时动态刷新配置和缓存的RPC接口
 * 供管理员客户端调用，实现不重启历史服务器即可更新配置
 */
@KerberosInfo(serverPrincipal = CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY)
@Private
@InterfaceStability.Evolving
public interface HSAdminRefreshProtocol {
  /**
   * 刷新历史服务器管理员访问控制列表配置，重新加载ACL规则
   * 
   * @throws IOException 刷新失败时抛出IO异常
   */
  public void refreshAdminAcls() throws IOException;
  
  /**
   * 刷新已加载作业缓存，清除过期缓存重新加载作业信息
   * @throws IOException 刷新失败时抛出IO异常
   */
  public void refreshLoadedJobCache() throws IOException;

  /**
   * 刷新作业保留时间配置，应用新的作业过期清理规则
   * 
   * @throws IOException 刷新失败时抛出IO异常
   */
  public void refreshJobRetentionSettings() throws IOException;

  /**
   * 刷新日志保留时间配置，应用新的日志过期清理规则
   * 
   * @throws IOException 刷新失败时抛出IO异常
   */
  public void refreshLogRetentionSettings() throws IOException;
  
}