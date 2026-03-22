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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.util.EnumSet;

/**
 * 文件级注释：HDFS离线镜像检查工具中，描述Protobuf格式FsImage损坏信息的数据容器
 * 存储损坏节点ID、损坏类型和损坏子节点数量，用于报表输出和结果统计
 */
/**
 * Class representing a corruption in the PBImageCorruptionDetector processor.
 */
public class PBImageCorruption {
  private static final String WITH = "With";

  /**
   * 枚举类，定义Protobuf格式FsImage支持的损坏类型
   * 目前支持损坏节点本身、节点缺失子节点两种损坏类型，可扩展新增其他类型
   */
  private enum PBImageCorruptionType {
    CORRUPT_NODE("CorruptNode"),
    MISSING_CHILD("MissingChild");

    private final String name;

    PBImageCorruptionType(String s) {
      name = s;
    }

    @Override
    public String toString() {
      return this.name;
    }
  }

  private long id;
  private EnumSet<PBImageCorruptionType> type;
  private int numOfCorruptChildren;

  /**
   * 构造方法，创建一个FsImage损坏信息实例
   * @param id 损坏节点的ID
   * @param missingChild 是否存在子节点缺失损坏
   * @param corruptNode 是否存在节点本身损坏
   * @param numOfCorruptChildren 损坏子节点的数量
   */
  PBImageCorruption(long id, boolean missingChild, boolean corruptNode,
                    int numOfCorruptChildren) {
    if (!missingChild && !corruptNode) {
      throw new IllegalArgumentException(
          "Corruption must have at least one aspect!");
    }
    this.id = id;
    this.type = EnumSet.noneOf(PBImageCorruptionType.class);
    if (missingChild) {
      type.add(PBImageCorruptionType.MISSING_CHILD);
    }
    if (corruptNode) {
      type.add(PBImageCorruptionType.CORRUPT_NODE);
    }
    this.numOfCorruptChildren = numOfCorruptChildren;
  }

  /**
   * 添加子节点缺失损坏类型到当前损坏信息
   */
  void addMissingChildCorruption() {
    type.add(PBImageCorruptionType.MISSING_CHILD);
  }

  /**
   * 添加节点本身损坏类型到当前损坏信息
   */
  void addCorruptNodeCorruption() {
    type.add(PBImageCorruptionType.CORRUPT_NODE);
  }

  /**
   * 设置损坏子节点的数量
   * @param numOfCorruption 损坏子节点数量
   */
  void setNumberOfCorruption(int numOfCorruption) {
    this.numOfCorruptChildren = numOfCorruption;
  }

  /**
   * 获取损坏节点的ID
   * @return 损坏节点ID
   */
  long getId() {
    return id;
  }

  /**
   * 获取当前损坏信息的格式化类型字符串，支持多种损坏类型拼接
   * @return 格式化后的损坏类型描述字符串
   */
  String getType() {
    StringBuilder s = new StringBuilder();
    if (type.contains(PBImageCorruptionType.CORRUPT_NODE)) {
      s.append(PBImageCorruptionType.CORRUPT_NODE);
    }
    if (type.contains(PBImageCorruptionType.CORRUPT_NODE) &&
        type.contains(PBImageCorruptionType.MISSING_CHILD)) {
      s.append(WITH);
    }

    if (type.contains(PBImageCorruptionType.MISSING_CHILD)) {
      s.append(PBImageCorruptionType.MISSING_CHILD);
    }
    return s.toString();
  }

  /**
   * 获取损坏子节点的数量
   * @return 损坏子节点数量
   */
  int getNumOfCorruptChildren() {
    return numOfCorruptChildren;
  }

}