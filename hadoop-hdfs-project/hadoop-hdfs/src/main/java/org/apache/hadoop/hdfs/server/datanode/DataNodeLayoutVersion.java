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
package org.apache.hadoop.hdfs.server.datanode;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.FeatureInfo;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.LayoutFeature;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * DataNode磁盘存储布局版本管理类，负责维护DataNode不同存储布局版本支持的功能特性，
 * 用于支持滚动升级和版本兼容性，定义了影响存储布局的功能特性版本信息。
 */
@InterfaceAudience.Private
public class DataNodeLayoutVersion {  
  /** 存储布局版本到该版本支持的所有功能特性集合的映射 */
  public final static Map<Integer, SortedSet<LayoutFeature>> FEATURES = 
    new HashMap<Integer, SortedSet<LayoutFeature>>();
  
  /** 当前DataNode使用的存储布局版本 */
  private static int currentLayoutVersion
      = LayoutVersion.getCurrentLayoutVersion(Feature.values());

  /**
   * 仅用于滚动升级测试的方法，生产环境禁止调用，用于设置测试用的布局版本。
   * @param lv 要设置的测试布局版本号
   */
  @VisibleForTesting
  static void setCurrentLayoutVersionForTesting(int lv) {
    currentLayoutVersion = lv;
  }

  /**
   * 获取当前DataNode的布局版本号。
   * @return 当前DataNode的布局版本号
   */
  public static int getCurrentLayoutVersion() {
    return currentLayoutVersion;
  }

  // 静态初始化块，构建功能特性与布局版本的映射关系
  static{
    // 加载通用布局功能特性
    LayoutVersion.updateMap(FEATURES, LayoutVersion.Feature.values());
    // 加载DataNode特有布局功能特性
    LayoutVersion.updateMap(FEATURES, DataNodeLayoutVersion.Feature.values());
  }
  
  /**
   * 获取指定布局版本支持的所有功能特性集合。
   * @param lv 布局版本号
   * @return 该版本支持的所有功能特性有序集合
   */
  public static SortedSet<LayoutFeature> getFeatures(int lv) {
    return FEATURES.get(lv);
  }

  /**
   * 检查指定布局版本是否支持给定功能特性。
   * @param f 待检查的功能特性
   * @param lv 目标布局版本号
   * @return 如果支持返回true，否则返回false
   */
  public static boolean supports(final LayoutFeature f, final int lv) {
    return LayoutVersion.supports(FEATURES, f, lv);
  }

  /**
   * 影响DataNode存储布局的功能特性枚举，每个枚举对应一个布局版本变更。
   * 新增布局版本需要遵循枚举定义规则：
   * 1. 定义新的枚举常量，指定新版本号和功能描述
   * 2. 如果新分支版本不是继承自直接前驱版本，需要显式指定祖先版本号
   */
  public enum Feature implements LayoutFeature {
    /** 第一个DataNode独立布局版本，起始版本号-55 */
    FIRST_LAYOUT(-55, -53, "First datanode layout", false),
    /** 基于块ID的目录结构布局，已完成块的块ID唯一确定其在磁盘上的存储位置 */
    BLOCKID_BASED_LAYOUT(-56,
        "The block ID of a finalized block uniquely determines its position " +
        "in the directory structure"),
    /** 32x32分级目录结构的基于块ID布局，相比-56版本使用更小更紧凑的目录结构 */
    BLOCKID_BASED_LAYOUT_32_by_32(-57,
        "Identical to the block id based layout (-56) except it uses a smaller"
        + " directory structure (32x32)");
   
    private final FeatureInfo info;

    /**
     * 构造DataNode布局功能特性，新版本从lv+1派生。
     * @param lv 当前功能特性对应的新版本号
     * @param description 功能特性描述
     */
    Feature(final int lv, final String description) {
      this(lv, lv + 1, description, false);
    }

    /**
     * 构造DataNode布局功能特性，指定祖先版本号。
     * @param lv 当前功能特性对应的新版本号
     * @param ancestorLV 派生该版本的祖先版本号
     * @param description 功能特性描述
     * @param reserved 是否为旧版本保留的布局版本
     * @param features 该版本需要启用的子功能特性
     */
    Feature(final int lv, final int ancestorLV, final String description,
        boolean reserved, Feature... features) {
      info = new FeatureInfo(lv, ancestorLV, description, reserved, features);
    }
    
    @Override
    public FeatureInfo getInfo() {
      return info;
    }
  }
}