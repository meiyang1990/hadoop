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

/**
 * 文件级注释：HDFS NameNode安全模式相关操作接口，定义了安全模式状态查询的统一规范
 * 安全模式是NameNode启动时的特殊阶段，此时仅对外提供元数据只读服务，等待数据块报告完成后退出
 */
/** SafeMode related operations. */
@InterfaceAudience.Private
/**
 * 安全模式操作接口，定义查询安全模式状态的方法，供NameNode安全模式实现使用
 * 安全模式下NameNode不接受块的增删改操作，用于启动阶段等待足够的数据块报告，保障集群启动正确性
 */
public interface SafeMode {
  /**
   * 检查当前系统是否处于安全模式
   * @return true表示处于安全模式，false表示已退出安全模式
   */
  /** Is the system in safe mode? */
  public boolean isInSafeMode();

  /**
   * 检查当前系统是否处于启动阶段自动进入的安全模式
   * 区分手动设置的安全模式和启动过程中自动进入的安全模式
   * @return true表示是启动阶段自动进入的安全模式，false表示不是
   */
  /**
   * Is the system in startup safe mode, i.e. the system is starting up with
   * safe mode turned on automatically?
   */
  public boolean isInStartupSafeMode();
}