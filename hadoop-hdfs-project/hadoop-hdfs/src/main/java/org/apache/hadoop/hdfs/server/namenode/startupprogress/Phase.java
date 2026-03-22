// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 文件功能：定义NameNode启动流程的各个阶段枚举，按执行顺序排列所有启动阶段，
 * 用于NameNode启动进度跟踪，记录当前启动所处阶段。
 */
@InterfaceAudience.Private
public enum Phase {
  /**
   * 加载fsimage文件到内存，构建文件系统元数据初始状态
   */
  LOADING_FSIMAGE("LoadingFsImage", "Loading fsimage"),

  /**
   * 加载edits编辑日志，将日志中的操作应用到内存元数据，
   * 对齐fsimage checkpoint之后的文件系统变更
   */
  LOADING_EDITS("LoadingEdits", "Loading edits"),

  /**
   * 保存新的检查点，将合并后的元数据写入新的fsimage文件
   */
  SAVING_CHECKPOINT("SavingCheckpoint", "Saving checkpoint"),

  /**
   * 进入安全模式，等待所有DataNode上报块信息，完成块信息收集后退出安全模式
   */
  SAFEMODE("SafeMode", "Safe mode");

  private final String name, description;

  /**
   * 获取当前启动阶段的描述文本
   * 
   * @return 阶段描述字符串
   */
  public String getDescription() {
    return description;
  }

  /**
   * 获取当前启动阶段的名称
   * 
   * @return 阶段名称字符串
   */
  public String getName() {
    return name;
  }

  /**
   * 枚举构造方法，初始化阶段名称和描述
   * 
   * @param name 阶段名称
   * @param description 阶段描述
   */
  private Phase(String name, String description) {
    this.name = name;
    this.description = description;
  }
}