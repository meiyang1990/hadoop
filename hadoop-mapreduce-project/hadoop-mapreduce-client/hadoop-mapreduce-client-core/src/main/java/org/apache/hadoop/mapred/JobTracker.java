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

package org.apache.hadoop.mapred;

/**
 * 兼容MapReduce 1.x应用的空JobTracker占位类
 * <p>
 * 在MapReduce 2.x（YARN架构）中，原有的JobTracker核心职责已经被拆分
 * 为ResourceManager和ApplicationMaster，本类仅作为向后兼容的占位存在，
 * 不再承担实际的作业调度与集群管理功能，避免旧版应用编译运行报错。
 * </p>
 */
public class JobTracker {

  /**
   * JobTracker状态枚举，仅为兼容旧版应用保留，不再实际使用
   * <p>
   * 保存原有的状态定义，供依赖该枚举的旧版MapReduce 1.x应用正常编译运行，
   * YARN架构中不再使用该状态标识。
   * </p>
   */
  public enum State {
    INITIALIZING, RUNNING
  }

}