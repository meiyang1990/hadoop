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

/**
 * @file HSAdminProtocol.java
 * 该文件定义了Hadoop MapReduce历史服务器管理协议的公共接口，聚合了多类管理功能接口
 */

package org.apache.hadoop.mapreduce.v2.api;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.tools.GetUserMappingsProtocol;

/**
 * 历史服务器管理协议接口，聚合多类用户与配置刷新管理能力
 * 该接口定义了历史服务器端提供给管理客户端调用的管理操作集合，
 * 组合了用户组映射获取、用户映射刷新、历史服务器配置刷新三类核心管理能力，
 * 用于支持对MapReduce历史服务器的动态运维管理操作
 */
@KerberosInfo(serverPrincipal = CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY)
@Private
@InterfaceStability.Evolving
public interface HSAdminProtocol extends GetUserMappingsProtocol,
    RefreshUserMappingsProtocol, HSAdminRefreshProtocol {

}