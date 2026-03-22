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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 文件级块存储策略集合管理类，负责维护HDFS支持的所有块存储策略，
 * 提供策略查询、默认策略获取和XAttr相关工具方法，支持冷热分级存储特性。
 */
/** A collection of block storage policies. */
public class BlockStoragePolicySuite {
  static final Logger LOG = LoggerFactory.getLogger(BlockStoragePolicySuite
      .class);

  // 存储策略XAttr属性名称
  public static final String STORAGE_POLICY_XATTR_NAME
      = "hsm.block.storage.policy.id";
  // 存储策略XAttr所属命名空间
  public static final XAttr.NameSpace XAttrNS = XAttr.NameSpace.SYSTEM;

  // 存储策略ID占用比特位数，共4位支持最多16种策略
  public static final int ID_BIT_LENGTH = 4;

  /**
   * 创建默认的块存储策略集合，使用默认配置。
   * @return 默认策略集合实例
   */
  @VisibleForTesting
  public static BlockStoragePolicySuite createDefaultSuite() {
    return createDefaultSuite(null);
  }

  /**
   * 根据配置创建默认的块存储策略集合，初始化所有内置的存储策略。
   * @param conf HDFS配置对象，用于获取默认策略配置
   * @return 初始化完成的策略集合实例
   */
  @VisibleForTesting
  public static BlockStoragePolicySuite createDefaultSuite(
      final Configuration conf) {
    // 按ID位数创建策略数组，最多容纳1<<ID_BIT_LENGTH种策略
    final BlockStoragePolicy[] policies =
        new BlockStoragePolicy[1 << ID_BIT_LENGTH];
    final byte lazyPersistId =
        HdfsConstants.StoragePolicy.LAZY_PERSIST.value();
    policies[lazyPersistId] = new BlockStoragePolicy(lazyPersistId,
        HdfsConstants.StoragePolicy.LAZY_PERSIST.name(),
        new StorageType[]{StorageType.RAM_DISK, StorageType.DISK},
        new StorageType[]{StorageType.DISK},
        new StorageType[]{StorageType.DISK},
        true);    // Cannot be changed on regular files, but inherited.
    final byte allnvdimmId = HdfsConstants.StoragePolicy.ALL_NVDIMM.value();
    policies[allnvdimmId] = new BlockStoragePolicy(allnvdimmId,
        HdfsConstants.StoragePolicy.ALL_NVDIMM.name(),
        new StorageType[]{StorageType.NVDIMM},
        new StorageType[]{StorageType.DISK},
        new StorageType[]{StorageType.DISK});
    final byte allssdId = HdfsConstants.StoragePolicy.ALL_SSD.value();
    policies[allssdId] = new BlockStoragePolicy(allssdId,
        HdfsConstants.StoragePolicy.ALL_SSD.name(),
        new StorageType[]{StorageType.SSD},
        new StorageType[]{StorageType.DISK},
        new StorageType[]{StorageType.DISK});
    final byte onessdId = HdfsConstants.StoragePolicy.ONE_SSD.value();
    policies[onessdId] = new BlockStoragePolicy(onessdId,
        HdfsConstants.StoragePolicy.ONE_SSD.name(),
        new StorageType[]{StorageType.SSD, StorageType.DISK},
        new StorageType[]{StorageType.SSD, StorageType.DISK},
        new StorageType[]{StorageType.SSD, StorageType.DISK});
    final byte hotId = HdfsConstants.StoragePolicy.HOT.value();
    policies[hotId] = new BlockStoragePolicy(hotId,
        HdfsConstants.StoragePolicy.HOT.name(),
        new StorageType[]{StorageType.DISK}, StorageType.EMPTY_ARRAY,
        new StorageType[]{StorageType.ARCHIVE});
    final byte warmId = HdfsConstants.StoragePolicy.WARM.value();
    policies[warmId] = new BlockStoragePolicy(warmId,
        HdfsConstants.StoragePolicy.WARM.name(),
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE},
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE},
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE});
    final byte coldId = HdfsConstants.StoragePolicy.COLD.value();
    policies[coldId] = new BlockStoragePolicy(coldId,
        HdfsConstants.StoragePolicy.COLD.name(),
        new StorageType[]{StorageType.ARCHIVE}, StorageType.EMPTY_ARRAY,
        StorageType.EMPTY_ARRAY);
    final byte providedId = HdfsConstants.StoragePolicy.PROVIDED.value();
    policies[providedId] = new BlockStoragePolicy(providedId,
      HdfsConstants.StoragePolicy.PROVIDED.name(),
      new StorageType[]{StorageType.PROVIDED, StorageType.DISK},
      new StorageType[]{StorageType.PROVIDED, StorageType.DISK},
      new StorageType[]{StorageType.PROVIDED, StorageType.DISK});

    return new BlockStoragePolicySuite(getDefaultPolicyID(conf, policies),
        policies);
  }

  /**
   * 从配置中读取并获取默认块存储策略ID，如果配置无效则返回系统默认值。
   * @param conf HDFS配置对象
   * @param policies 已初始化的策略数组
   * @return 默认策略ID
   */
  private static byte getDefaultPolicyID(
      final Configuration conf, final BlockStoragePolicy[] policies) {
    if (conf != null) {
      HdfsConstants.StoragePolicy defaultPolicy = conf.getEnum(
          DFSConfigKeys.DFS_STORAGE_DEFAULT_POLICY,
          DFSConfigKeys.DFS_STORAGE_DEFAULT_POLICY_DEFAULT);
      for (BlockStoragePolicy policy : policies) {
        if (policy != null &&
            policy.getName().equalsIgnoreCase(defaultPolicy.name())) {
          return policy.getId();
        }
      }
    }
    return DFSConfigKeys.DFS_STORAGE_DEFAULT_POLICY_DEFAULT.value();
  }

  private final byte defaultPolicyID;
  private final BlockStoragePolicy[] policies;

  /**
   * 构造块存储策略集合实例。
   * @param defaultPolicyID 默认策略ID
   * @param policies 按ID索引的策略数组
   */
  public BlockStoragePolicySuite(byte defaultPolicyID,
      BlockStoragePolicy[] policies) {
    this.defaultPolicyID = defaultPolicyID;
    this.policies = policies;
  }

  /**
   * 根据策略ID获取对应的块存储策略，ID为0时返回默认策略。
   * @return 对应ID的块存储策略
   */
  public BlockStoragePolicy getPolicy(byte id) {
    // id == 0 means policy not specified.
    return id == 0? getDefaultPolicy(): policies[id];
  }

  /**
   * 获取当前配置的默认块存储策略。
   * @return 默认块存储策略
   */
  public BlockStoragePolicy getDefaultPolicy() {
    return getPolicy(defaultPolicyID);
  }

  /**
   * 根据策略名称（忽略大小写）获取对应的块存储策略。
   * @param policyName 策略名称
   * @return 匹配到的策略，未找到返回null
   */
  public BlockStoragePolicy getPolicy(String policyName) {
    Preconditions.checkNotNull(policyName);

    if (policies != null) {
      for (BlockStoragePolicy policy : policies) {
        if (policy != null && policy.getName().equalsIgnoreCase(policyName)) {
          return policy;
        }
      }
    }
    return null;
  }

  /**
   * 获取当前所有已定义的非空块存储策略列表。
   * @return 所有有效策略的数组
   */
  public BlockStoragePolicy[] getAllPolicies() {
    List<BlockStoragePolicy> list = Lists.newArrayList();
    if (policies != null) {
      for (BlockStoragePolicy policy : policies) {
        if (policy != null) {
          list.add(policy);
        }
      }
    }
    return list.toArray(new BlockStoragePolicy[list.size()]);
  }

  /**
   * 构建存储策略XAttr的完整名称，包含命名空间前缀。
   * @return 完整XAttr名称
   */
  public static String buildXAttrName() {
    return StringUtils.toLowerCase(XAttrNS.toString())
        + "." + STORAGE_POLICY_XATTR_NAME;
  }

  /**
   * 根据策略ID构建存储策略XAttr对象，用于存储文件级策略信息。
   * @param policyId 存储策略ID
   * @return 构造完成的XAttr对象
   */
  public static XAttr buildXAttr(byte policyId) {
    final String name = buildXAttrName();
    return XAttrHelper.buildXAttr(name, new byte[]{policyId});
  }

  /**
   * 获取带命名空间前缀的存储策略XAttr名称。
   * @return 带前缀的XAttr名称
   */
  public static String getStoragePolicyXAttrPrefixedName() {
    return XAttrHelper.getPrefixedName(XAttrNS, STORAGE_POLICY_XATTR_NAME);
  }

  /**
   * 判断给定XAttr是否为存储策略属性。
   * @param xattr 待判断的XAttr对象
   * @return 如果是存储策略属性返回true，否则返回false
   */
  public static boolean isStoragePolicyXAttr(XAttr xattr) {
    return xattr != null && xattr.getNameSpace() == XAttrNS
        && xattr.getName().equals(STORAGE_POLICY_XATTR_NAME);
  }
}