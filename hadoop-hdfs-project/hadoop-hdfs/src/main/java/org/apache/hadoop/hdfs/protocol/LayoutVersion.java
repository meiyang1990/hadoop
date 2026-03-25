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
package org.apache.hadoop.hdfs.protocol;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 文件级注释：HDFS磁盘布局版本管理类，负责跟踪HDFS元数据存储格式的版本变更
 *
 * 布局版本变更通常由以下原因触发：
 * <ol>
 * <li>NameNode或DataNode磁盘存储结构发生变化</li>
 * <li>Editlog新增操作码</li>
 * <li>Editlog或FsImage中的记录格式、内容发生修改</li>
 * </ol>
 * <br>
 * <b>如何更新布局版本:<br></b>
 * 当变更需要新增布局版本时，在{@link Feature}枚举中添加新条目，包含简短枚举名、新版本号和变更描述，详见{@link Feature}
 * <br>
 */
@InterfaceAudience.Private
public class LayoutVersion {
  /**
   * 修复HDFS-2991问题的版本阈值，该问题会导致append调用有时跳过OP_ADD操作
   * 加载editlog时，如果遇到该问题且版本低于此阈值，会进行兼容处理；否则判定为数据损坏
   */
  public static final int BUGFIX_HDFS_2991_VERSION = -40;

  /**
   * 布局特性接口，NameNode和DataNode布局特性需要实现该接口
   */
  public interface LayoutFeature {
    public FeatureInfo getInfo();
  }

  /**
   * 布局特性枚举，定义所有会改变布局版本的特性，支持滚动升级前的版本管理
   * <br><br>
   * 添加新布局版本步骤：
   * <ul>
   * <li>定义新枚举常量，包含简短名称、新布局版本号和特性描述</li>
   * <li>如果新版本的父版本不是直接前驱，使用可指定父版本的构造方法</li>
   * </ul>
   */
  public enum Feature implements LayoutFeature {
    NAMESPACE_QUOTA(-16, "Support for namespace quotas"),
    FILE_ACCESS_TIME(-17, "Support for access time on files"),
    DISKSPACE_QUOTA(-18, "Support for disk space quotas"),
    STICKY_BIT(-19, "Support for sticky bits"),
    APPEND_RBW_DIR(-20, "Datanode has \"rbw\" subdirectory for append"),
    ATOMIC_RENAME(-21, "Support for atomic rename"),
    CONCAT(-22, "Support for concat operation"),
    SYMLINKS(-23, "Support for symbolic links"),
    DELEGATION_TOKEN(-24, "Support for delegation tokens for security"),
    FSIMAGE_COMPRESSION(-25, "Support for fsimage compression"),
    FSIMAGE_CHECKSUM(-26, "Support checksum for fsimage"),
    REMOVE_REL13_DISK_LAYOUT_SUPPORT(-27, "Remove support for 0.13 disk layout"),
    EDITS_CHECKSUM(-28, "Support checksum for editlog"),
    UNUSED(-29, "Skipped version"),
    FSIMAGE_NAME_OPTIMIZATION(-30, "Store only last part of path in fsimage"),
    RESERVED_REL20_203(-31, -19, "Reserved for release 0.20.203", true,
        DELEGATION_TOKEN),
    RESERVED_REL20_204(-32, -31, "Reserved for release 0.20.204", true),
    RESERVED_REL22(-33, -27, "Reserved for release 0.22", true),
    RESERVED_REL23(-34, -30, "Reserved for release 0.23", true),
    FEDERATION(-35, "Support for namenode federation"),
    LEASE_REASSIGNMENT(-36, "Support for persisting lease holder reassignment"),
    STORED_TXIDS(-37, "Transaction IDs are stored in edits log and image files"),
    TXID_BASED_LAYOUT(-38, "File names in NN Storage are based on transaction IDs"), 
    EDITLOG_OP_OPTIMIZATION(-39,
        "Use LongWritable and ShortWritable directly instead of ArrayWritable of UTF8"),
    OPTIMIZE_PERSIST_BLOCKS(-40,
        "Serialize block lists with delta-encoded variable length ints, " +
        "add OP_UPDATE_BLOCKS"),
    RESERVED_REL1_2_0(-41, -32, "Reserved for release 1.2.0", true, CONCAT),
    ADD_INODE_ID(-42, -40, "Assign a unique inode id for each inode", false),
    SNAPSHOT(-43, "Support for snapshot feature"),
    RESERVED_REL1_3_0(-44, -41, "Reserved for release 1.3.0", true,
    		ADD_INODE_ID, SNAPSHOT, FSIMAGE_NAME_OPTIMIZATION),
    OPTIMIZE_SNAPSHOT_INODES(-45, -43,
        "Reduce snapshot inode memory footprint", false),
    SEQUENTIAL_BLOCK_ID(-46, "Allocate block IDs sequentially and store " +
        "block IDs in the edits log and image files"),
    EDITLOG_SUPPORT_RETRYCACHE(-47, "Record ClientId and CallId in editlog to " 
        + "enable rebuilding retry cache in case of HA failover"),
    EDITLOG_ADD_BLOCK(-48, "Add new editlog that only records allocation of " +
        "the new block instead of the entire block list"),
    ADD_DATANODE_AND_STORAGE_UUIDS(-49, "Replace StorageID with DatanodeUuid."
        + " Use distinct StorageUuid per storage directory."),
    ADD_LAYOUT_FLAGS(-50, "Add support for layout flags."),
    CACHING(-51, "Support for cache pools and path-based caching"),
    // Hadoop 2.4.0
    PROTOBUF_FORMAT(-52, "Use protobuf to serialize FSImage"),
    EXTENDED_ACL(-53, "Extended ACL"),
    RESERVED_REL2_4_0(-54, -51, "Reserved for release 2.4.0", true,
        PROTOBUF_FORMAT, EXTENDED_ACL);

    private final FeatureInfo info;

    /**
     * 构造方法，在当前版本号减一引入的布局特性
     * @param lv 新增该特性后的新布局版本
     * @param description 特性描述
     */
    Feature(final int lv, final String description) {
      this(lv, lv + 1, description, false);
    }

    /**
     * 构造方法，从指定父版本派生的布局特性
     * @param lv 新增该特性后的新布局版本
     * @param ancestorLV 该版本派生自的父布局版本
     * @param description 特性描述
     * @param reserved 是否为旧版本预留的版本
     * @param features 该版本需要启用的额外特性集合
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
  
  /**
   * 布局特性信息类，存储单个布局特性的元数据
   */
  public static class FeatureInfo {
    private final int lv;
    private final int ancestorLV;
    private final Integer minCompatLV;
    private final String description;
    private final boolean reserved;
    private final LayoutFeature[] specialFeatures;

    public FeatureInfo(final int lv, final int ancestorLV, final String description,
        boolean reserved, LayoutFeature... specialFeatures) {
      this(lv, ancestorLV, null, description, reserved, specialFeatures);
    }

    public FeatureInfo(final int lv, final int ancestorLV, Integer minCompatLV,
        final String description, boolean reserved,
        LayoutFeature... specialFeatures) {
      this.lv = lv;
      this.ancestorLV = ancestorLV;
      this.minCompatLV = minCompatLV;
      this.description = description;
      this.reserved = reserved;
      this.specialFeatures = specialFeatures;
    }
    
    /** 
     * 获取特性对应的布局版本
     * @return 布局版本号
     */
    public int getLayoutVersion() {
      return lv;
    }

    /** 
     * 获取父布局版本
     * @return 父布局版本号
     */
    public int getAncestorLayoutVersion() {
      return ancestorLV;
    }

    /**
     * 获取该特性最小兼容布局版本
     * 如果未定义最小兼容版本，则返回特性自身版本，表示不兼容任何更早版本
     *
     * @return 最小兼容布局版本号
     */
    public int getMinimumCompatibleLayoutVersion() {
      return minCompatLV != null ? minCompatLV : lv;
    }

    /**
     * 获取特性描述
     * @return 特性描述字符串
     */
    public String getDescription() {
      return description;
    }
    
    public boolean isReservedForOldRelease() {
      return reserved;
    }
    
    public LayoutFeature[] getSpecialFeatures() {
      return specialFeatures;
    }
  }

  /**
   * 布局特性比较器，按布局版本号排序
   */
  static class LayoutFeatureComparator implements Comparator<LayoutFeature> {
    @Override
    public int compare(LayoutFeature arg0, LayoutFeature arg1) {
      return arg0.getInfo().getLayoutVersion()
          - arg1.getInfo().getLayoutVersion();
    }
  }
 
  /**
   * 更新布局版本到特性集合的映射，构建每个布局版本支持的所有特性集合
   * @param map 待更新的版本-特性集合映射
   * @param features 需要添加的布局特性数组
   */
  public static void updateMap(Map<Integer, SortedSet<LayoutFeature>> map,
      LayoutFeature[] features) {
    // 收集已有的所有特性，用于顺序校验
    SortedSet<LayoutFeature> existingFeatures = new TreeSet<LayoutFeature>(
        new LayoutFeatureComparator());
    for (SortedSet<LayoutFeature> s : map.values()) {
      existingFeatures.addAll(s);
    }
    // 前一个特性，用于校验最小兼容版本的顺序
    LayoutFeature prevF = existingFeatures.isEmpty() ? null :
        existingFeatures.first();
    // 遍历所有待添加特性
    for (LayoutFeature f : features) {
      final FeatureInfo info = f.getInfo();
      int minCompatLV = info.getMinimumCompatibleLayoutVersion();
      // 校验：特性必须按最小兼容版本升序排列
      if (prevF != null &&
          minCompatLV > prevF.getInfo().getMinimumCompatibleLayoutVersion()) {
        throw new AssertionError(String.format(
            "Features must be listed in order of minimum compatible layout " +
            "version.  Check features %s and %s.", prevF, f));
      }
      prevF = f;
      // 获取父版本对应的特性集合
      SortedSet<LayoutFeature> ancestorSet = map.get(info.getAncestorLayoutVersion());
      if (ancestorSet == null) {
        // 父版本不存在，新建空集合
        ancestorSet = new TreeSet<LayoutFeature>(new LayoutFeatureComparator());
        map.put(info.getAncestorLayoutVersion(), ancestorSet);
      }
      // 基于父版本集合创建新版本特性集合
      SortedSet<LayoutFeature> featureSet = new TreeSet<LayoutFeature>(ancestorSet);
      // 添加当前版本指定的额外特性
      if (info.getSpecialFeatures() != null) {
        for (LayoutFeature specialFeature : info.getSpecialFeatures()) {
          featureSet.add(specialFeature);
        }
      }
      // 添加当前特性
      featureSet.add(f);
      // 将新版本特性集合存入映射
      map.put(info.getLayoutVersion(), featureSet);
    }
  }
  
  /**
   * 生成布局版本信息的格式化字符串，用于日志输出和展示
   * @param map 版本到特性集合的映射
   * @param values 所有布局特性数组
   * @return 格式化后的布局版本信息字符串
   */
  public String getString(Map<Integer, SortedSet<LayoutFeature>> map,
      LayoutFeature[] values) {
    final StringBuilder buf = new StringBuilder();
    buf.append("Feature List:\n");
    for (LayoutFeature f : values) {
      final FeatureInfo info = f.getInfo();
      buf.append(f).append(" introduced in layout version ")
          .append(info.getLayoutVersion()).append(" (")
          .append(info.getDescription()).append(")\n");
    }

    buf.append("\n\nLayoutVersion and supported features:\n");
    for (LayoutFeature f : values) {
      final FeatureInfo info = f.getInfo();
      buf.append(info.getLayoutVersion()).append(": ")
          .append(map.get(info.getLayoutVersion())).append("\n");
    }
    return buf.toString();
  }
  
  /**
   * 检查指定布局版本是否支持给定特性
   * @param map 版本到特性集合的映射
   * @param f 待检查的布局特性
   * @param lv 目标布局版本
   * @return true 如果指定版本支持该特性，否则false
   */
  public static boolean supports(Map<Integer, SortedSet<LayoutFeature>> map,
      final LayoutFeature f, final int lv) {
    final SortedSet<LayoutFeature> set =  map.get(lv);
    return set != null && set.contains(f);
  }
  
  /**
   * 获取当前最新的布局版本号
   * @param features 所有布局特性数组
   * @return 当前最新布局版本号
   */
  public static int getCurrentLayoutVersion(LayoutFeature[] features) {
    return getLastNonReservedFeature(features).getInfo().getLayoutVersion();
  }

  /**
   * 获取当前版本最小兼容的布局版本号
   * @param features 所有布局特性数组
   * @return 最小兼容布局版本号
   */
  public static int getMinimumCompatibleLayoutVersion(
      LayoutFeature[] features) {
    return getLastNonReservedFeature(features).getInfo()
        .getMinimumCompatibleLayoutVersion();
  }

  /**
   * 获取最后一个非预留的布局特性，用于获取当前最新版本信息
   * @param features 所有布局特性数组
   * @return 最后一个非预留布局特性
   */
  static LayoutFeature getLastNonReservedFeature(LayoutFeature[] features) {
    // 从后往前遍历，找到第一个非预留版本
    for (int i = features.length -1; i >= 0; i--) {
      final FeatureInfo info = features[i].getInfo();
      if (!info.isReservedForOldRelease()) {
        return features[i];
      }
    }
    // 所有版本都是预留的，抛出断言错误
    throw new AssertionError("All layout versions are reserved.");
  }
}