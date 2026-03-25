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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;

/**
 * 文件概述： Namenode注册信息类，用于HDFS联邦/HA架构中从节点NameNode向主节点NameNode注册时
 * 携带自身的元数据信息和地址信息，完成注册流程。
 * 核心职责：存储 subordinate Namenode（备用节点、观察者节点等）的注册信息，包括地址、角色、存储版本等，
 * 供主节点NameNode识别和管理集群中的所有NameNode节点。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class NamenodeRegistration extends StorageInfo
implements NodeRegistration {
  final String rpcAddress;          // 当前NameNode节点的RPC服务地址
  final String httpAddress;         // 当前NameNode节点的HTTP服务地址
  final NamenodeRole role;          // 当前NameNode节点的角色（Active/Standby/Observer等）

  /**
   * 构造Namenode注册信息对象
   * @param address 当前NameNode的RPC服务地址
   * @param httpAddress 当前NameNode的HTTP服务地址
   * @param storageInfo 存储信息对象，包含存储版本等元数据
   * @param role 当前NameNode的角色
   */
  public NamenodeRegistration(String address,
                              String httpAddress,
                              StorageInfo storageInfo,
                              NamenodeRole role) {
    super(storageInfo);
    this.rpcAddress = address;
    this.httpAddress = httpAddress;
    this.role = role;
  }

  @Override // NodeRegistration
  public String getAddress() {
    return rpcAddress;
  }
  
  /**
   * 获取当前NameNode的HTTP服务地址
   * @return HTTP地址字符串
   */
  public String getHttpAddress() {
    return httpAddress;
  }
  
  @Override // NodeRegistration
  public String getRegistrationID() {
    return Storage.getRegistrationID(this);
  }

  @Override // NodeRegistration
  public int getVersion() {
    return super.getLayoutVersion();
  }

  @Override // NodeRegistration
  public String toString() {
    return getClass().getSimpleName()
    + "(" + rpcAddress
    + ", role=" + getRole()
    + ")";
  }

  /**
   * 获取当前NameNode的角色
   * @return NameNode角色枚举
   */
  public NamenodeRole getRole() {
    return role;
  }

  /**
   * 判断当前NameNode角色是否等于指定角色
   * @param that 待比较的目标角色
   * @return 如果相等返回true，否则返回false
   */
  public boolean isRole(NamenodeRole that) {
    return role.equals(that);
  }
}