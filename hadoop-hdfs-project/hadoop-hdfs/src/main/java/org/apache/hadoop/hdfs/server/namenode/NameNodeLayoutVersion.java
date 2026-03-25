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

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.FeatureInfo;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.LayoutFeature;

/**
 * NameNode元数据磁盘布局版本管理类。
 * 负责维护HDFS NameNode不同布局版本对应的功能特性，支持版本兼容性检查和滚动升级兼容性管理。
 * 核心职责是记录每个布局版本引入的新功能，以及提供版本兼容性查询能力。
 */
@InterfaceAudience.Private
public class NameNodeLayoutVersion { 
  /** 存储布局版本到对应支持的功能特性集合的映射 */
  public final static Map<Integer, SortedSet<LayoutFeature>> FEATURES
      = new HashMap<Integer, SortedSet<LayoutFeature>>();

  /** 当前NameNode布局版本号，由所有功能特性计算得出 */
  public static final int CURRENT_LAYOUT_VERSION
      = LayoutVersion.getCurrentLayoutVersion(Feature.values());
  /** 最低兼容布局版本号，用于回滚兼容性检查 */
  public static final int MINIMUM_COMPATIBLE_LAYOUT_VERSION
      = LayoutVersion.getMinimumCompatibleLayoutVersion(Feature.values());

  static {
    // 加载通用布局版本功能特性到映射表
    LayoutVersion.updateMap(FEATURES, LayoutVersion.Feature.values());
    // 加载NameNode专属布局版本功能特性到映射表
    LayoutVersion.updateMap(FEATURES, NameNodeLayoutVersion.Feature.values());
  }
  
  /**
   * 获取指定布局版本支持的所有功能特性集合。
   * @param lv 布局版本号
   * @return 对应版本支持的功能特性集合
   */
  public static SortedSet<LayoutFeature> getFeatures(int lv) {
    return FEATURES.get(lv);
  }

  /**
   * 检查指定布局版本是否支持某一特定功能特性。
   * @param f 待检查的功能特性
   * @param lv 目标布局版本号
   * @return true如果支持该特性，false否则
   */
  public static boolean supports(final LayoutFeature f, final int lv) {
    return LayoutVersion.supports(FEATURES, f, lv);
  }

  /**
   * 枚举定义所有会改变NameNode布局版本的功能特性。
   * 每个枚举实例对应一个新布局版本引入的功能，记录版本信息和兼容性描述。
   * 新增布局版本需要遵循文档说明的添加规则，以保证回滚兼容性。
   */
  public enum Feature implements LayoutFeature {
    ROLLING_UPGRADE(-55, -53, -55, "Support rolling upgrade", false),
    EDITLOG_LENGTH(-56, -56, "Add length field to every edit log op"),
    XATTRS(-57, -57, "Extended attributes"),
    CREATE_OVERWRITE(-58, -58, "Use single editlog record for " +
      "creating file with overwrite"),
    XATTRS_NAMESPACE_EXT(-59, -59, "Increase number of xattr namespaces"),
    BLOCK_STORAGE_POLICY(-60, -60, "Block Storage policy"),
    TRUNCATE(-61, -61, "Truncate"),
    APPEND_NEW_BLOCK(-62, -61, "Support appending to new block"),
    QUOTA_BY_STORAGE_TYPE(-63, -61, "Support quota for specific storage types"),
    ERASURE_CODING(-64, -61, "Support erasure coding"),
    EXPANDED_STRING_TABLE(-65, -61, "Support expanded string table in fsimage"),
    SNAPSHOT_MODIFICATION_TIME(-66, -61, "Support modification time for snapshot"),
    NVDIMM_SUPPORT(-67, -61, "Support NVDIMM storage type");

    private final FeatureInfo info;

    /**
     * 构造功能特性枚举，父版本为当前新版本号+1。
     * @param lv 新增该功能后的新布局版本号
     * @param minCompatLV 该功能支持的最低兼容布局版本（回滚时允许回滚到的最早版本）
     * @param description 功能特性描述
     */
    Feature(final int lv, int minCompatLV, final String description) {
      this(lv, lv + 1, minCompatLV, description, false);
    }

    /**
     * 构造功能特性枚举，允许指定父版本和兼容性信息。
     * @param lv 新增该功能后的新布局版本号
     * @param ancestorLV 该版本派生自的父布局版本号
     * @param minCompatLV 该功能支持的最低兼容布局版本（回滚时允许回滚到的最早版本）
     * @param description 功能特性描述
     * @param reserved 是否为旧版本保留的布局版本
     * @param features 该版本默认启用的子功能特性列表
     */
    Feature(final int lv, final int ancestorLV, int minCompatLV,
        final String description, boolean reserved, Feature... features) {
      info = new FeatureInfo(lv, ancestorLV, minCompatLV, description, reserved,
          features);
    }
    
    @Override
    public FeatureInfo getInfo() {
      return info;
    }
  }
}