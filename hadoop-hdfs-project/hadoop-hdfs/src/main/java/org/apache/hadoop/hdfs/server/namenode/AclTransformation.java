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

import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.ScopedAclEntries;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.thirdparty.com.google.common.collect.ComparisonChain;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.com.google.common.collect.Ordering;

/**
 * @file Acl转换工具类，提供ACL修改操作的核心实现。
 * 所有ACL修改操作都基于现有ACL进行处理，支持添加、修改、删除ACL条目，
 * 自动处理mask条目计算和默认条目推导，修改后保证输出ACL保持正确排序。
 * 本类在HDFS NameNode端负责处理用户提交的ACL变更请求，生成符合规范的新ACL。
 */
@InterfaceAudience.Private
final class AclTransformation {
  // ACL单作用域允许的最大条目数
  private static final int MAX_ENTRIES = 32;

  /**
   * 根据ACL规范删除现有ACL中与规范条目冲突（同作用域、类型、名称）的条目。
   * 必要时重新计算mask，推导默认条目。不允许删除仍需要的mask条目。
   * 
   * @param existingAcl 现有ACL条目列表
   * @param inAclSpec 需要删除的条目列表（作为ACL规范输入
   * @return 处理后的新ACL
   * @throws AclException 校验失败时抛出异常
   */
  public static List<AclEntry> filterAclEntriesByAclSpec(
      List<AclEntry> existingAcl, List<AclEntry> inAclSpec) throws AclException {
    // 预处理输入ACL规范：排序和预校验
    ValidatedAclSpec aclSpec = new ValidatedAclSpec(inAclSpec);
    // 新建列表存储处理后的ACL条目
    ArrayList<AclEntry> aclBuilder = Lists.newArrayListWithCapacity(MAX_ENTRIES);
    // 存储用户指定的mask条目，按作用域分类
    EnumMap<AclEntryScope, AclEntry> providedMask =
      Maps.newEnumMap(AclEntryScope.class);
    // 标记mask条目已修改的作用域
    EnumSet<AclEntryScope> maskDirty = EnumSet.noneOf(AclEntryScope.class);
    // 标记有条目修改的作用域
    EnumSet<AclEntryScope> scopeDirty = EnumSet.noneOf(AclEntryScope.class);
    // 遍历现有ACL，过滤掉待删除条目
    for (AclEntry existingEntry: existingAcl) {
      if (aclSpec.containsKey(existingEntry)) {
        scopeDirty.add(existingEntry.getScope());
        if (existingEntry.getType() == MASK) {
          maskDirty.add(existingEntry.getScope());
        }
      } else {
        if (existingEntry.getType() == MASK) {
          providedMask.put(existingEntry.getScope(), existingEntry);
        } else {
          aclBuilder.add(existingEntry);
        }
      }
    }
    // 如果需要，从访问条目复制推导缺失的默认条目
    copyDefaultsIfNeeded(aclBuilder);
    // 重新计算各作用域需要的mask条目
    calculateMasks(aclBuilder, providedMask, maskDirty, scopeDirty);
    // 最终校验、排序后返回不可修改的ACL列表
    return buildAndValidateAcl(aclBuilder);
  }

  /**
   * 删除现有ACL中所有默认条目，仅保留访问ACL条目。
   *
   * @param existingAcl 现有ACL条目列表
   * @return 处理后仅保留访问条目的新ACL
   * @throws AclException 校验失败时抛出异常
   */
  public static List<AclEntry> filterDefaultAclEntries(
      List<AclEntry> existingAcl) throws AclException {
    ArrayList<AclEntry> aclBuilder = Lists.newArrayListWithCapacity(MAX_ENTRIES);
    for (AclEntry existingEntry: existingAcl) {
      // 默认条目排序在访问条目之后，找到第一个默认条目即可提前退出
      if (existingEntry.getScope() == DEFAULT) {
        break;
      }
      aclBuilder.add(existingEntry);
    }
    return buildAndValidateAcl(aclBuilder);
  }

  /**
   * 将ACL规范中的条目合并到现有ACL。同键（作用域、类型、名称相同）的条目会被覆盖，
   * 新条目会被添加。必要时重新计算mask，推导默认条目。
   *
   * @param existingAcl 现有ACL条目列表
   * @param inAclSpec 待合并的ACL条目列表
   * @return 合并后的新ACL
   * @throws AclException 校验失败时抛出异常
   */
  public static List<AclEntry> mergeAclEntries(List<AclEntry> existingAcl,
      List<AclEntry> inAclSpec) throws AclException {
    ValidatedAclSpec aclSpec = new ValidatedAclSpec(inAclSpec);
    ArrayList<AclEntry> aclBuilder = Lists.newArrayListWithCapacity(MAX_ENTRIES);
    // 存储已找到的替换条目，用于区分新增和替换
    List<AclEntry> foundAclSpecEntries =
      Lists.newArrayListWithCapacity(MAX_ENTRIES);
    EnumMap<AclEntryScope, AclEntry> providedMask =
      Maps.newEnumMap(AclEntryScope.class);
    EnumSet<AclEntryScope> maskDirty = EnumSet.noneOf(AclEntryScope.class);
    EnumSet<AclEntryScope> scopeDirty = EnumSet.noneOf(AclEntryScope.class);
    // 遍历现有ACL，处理已存在条目
    for (AclEntry existingEntry: existingAcl) {
      AclEntry aclSpecEntry = aclSpec.findByKey(existingEntry);
      if (aclSpecEntry != null) {
        // 找到同键条目，用规范条目替换现有条目
        foundAclSpecEntries.add(aclSpecEntry);
        scopeDirty.add(aclSpecEntry.getScope());
        if (aclSpecEntry.getType() == MASK) {
          providedMask.put(aclSpecEntry.getScope(), aclSpecEntry);
          maskDirty.add(aclSpecEntry.getScope());
        } else {
          aclBuilder.add(aclSpecEntry);
        }
      } else {
        // 保留现有未匹配条目
        if (existingEntry.getType() == MASK) {
          providedMask.put(existingEntry.getScope(), existingEntry);
        } else {
          aclBuilder.add(existingEntry);
        }
      }
    }
    // 添加规范中不存在于现有ACL的新条目
    for (AclEntry newEntry: aclSpec) {
      if (Collections.binarySearch(foundAclSpecEntries, newEntry,
          ACL_ENTRY_COMPARATOR) < 0) {
        scopeDirty.add(newEntry.getScope());
        if (newEntry.getType() == MASK) {
          providedMask.put(newEntry.getScope(), newEntry);
          maskDirty.add(newEntry.getScope());
        } else {
          aclBuilder.add(newEntry);
        }
      }
    }
    copyDefaultsIfNeeded(aclBuilder);
    calculateMasks(aclBuilder, providedMask, maskDirty, scopeDirty);
    return buildAndValidateAcl(aclBuilder);
  }

  /**
   * 使用ACL规范完全替换ACL，访问和默认作用域分别处理。
   * 仅规范中存在的作用域会被替换，未提到的作用域保留原有条目。
   * 必要时重新计算mask，推导默认条目。
   *
   * @param existingAcl 现有ACL条目列表
   * @param inAclSpec 替换用的新ACL条目列表
   * @return 替换后的新ACL
   * @throws AclException 校验失败时抛出异常
   */
  public static List<AclEntry> replaceAclEntries(List<AclEntry> existingAcl,
      List<AclEntry> inAclSpec) throws AclException {
    ValidatedAclSpec aclSpec = new ValidatedAclSpec(inAclSpec);
    ArrayList<AclEntry> aclBuilder = Lists.newArrayListWithCapacity(MAX_ENTRIES);
    // 分别处理访问和默认两个作用域，仅替换规范中包含的作用域
    EnumMap<AclEntryScope, AclEntry> providedMask =
      Maps.newEnumMap(AclEntryScope.class);
    EnumSet<AclEntryScope> maskDirty = EnumSet.noneOf(AclEntryScope.class);
    EnumSet<AclEntryScope> scopeDirty = EnumSet.noneOf(AclEntryScope.class);
    // 先添加规范提供的条目，替换对应作用域
    for (AclEntry aclSpecEntry: aclSpec) {
      scopeDirty.add(aclSpecEntry.getScope());
      if (aclSpecEntry.getType() == MASK) {
        providedMask.put(aclSpecEntry.getScope(), aclSpecEntry);
        maskDirty.add(aclSpecEntry.getScope());
      } else {
        aclBuilder.add(aclSpecEntry);
      }
    }
    // 保留未被替换的作用域中的原有条目
    for (AclEntry existingEntry: existingAcl) {
      if (!scopeDirty.contains(existingEntry.getScope())) {
        if (existingEntry.getType() == MASK) {
          providedMask.put(existingEntry.getScope(), existingEntry);
        } else {
          aclBuilder.add(existingEntry);
        }
      }
    }
    copyDefaultsIfNeeded(aclBuilder);
    calculateMasks(aclBuilder, providedMask, maskDirty, scopeDirty);
    return buildAndValidateAcl(aclBuilder);
  }

  /**
   * 禁止实例化该工具类
   */
  private AclTransformation() {
  }

  /**
   * ACL条目排序比较器，定义ACL条目必须遵循的排序规则：
   * 1. 所有访问ACL条目排在默认ACL条目之前
   * 2. 同作用域内按类型排序：所有者用户 -> 命名用户 -> 所属组 -> 命名组 -> 掩码 -> 其他
   * 3. 同类型下按名称自然排序，无名称排在有名称之前
   */
  static final Comparator<AclEntry> ACL_ENTRY_COMPARATOR =
    new Comparator<AclEntry>() {
      @Override
      public int compare(AclEntry entry1, AclEntry entry2) {
        return ComparisonChain.start()
          .compare(entry1.getScope(), entry2.getScope(),
            Ordering.explicit(ACCESS, DEFAULT))
          .compare(entry1.getType(), entry2.getType(),
            Ordering.explicit(USER, GROUP, MASK, OTHER))
          .compare(entry1.getName(), entry2.getName(),
            Ordering.natural().nullsFirst())
          .result();
      }
    };

  /**
   * 对构建中的ACL条目进行修剪、排序、校验，生成最终不可修改的ACL列表。
   * 检查重复条目、非法条目结构，保证基础条目（用户、组、其他）必须存在，限制条目数量。
   *
   * @param aclBuilder 待构建的ACL条目列表
   * @return 排序校验完成的不可修改ACL列表
   * @throws AclException 校验失败抛出异常
   */
  private static List<AclEntry> buildAndValidateAcl(
      ArrayList<AclEntry> aclBuilder) throws AclException {
    aclBuilder.trimToSize();
    Collections.sort(aclBuilder, ACL_ENTRY_COMPARATOR);
    // 遍历检查重复和非法命名条目
    AclEntry prevEntry = null;
    for (AclEntry entry: aclBuilder) {
      if (prevEntry != null &&
          ACL_ENTRY_COMPARATOR.compare(prevEntry, entry) == 0) {
        throw new AclException(
          "Invalid ACL: multiple entries with same scope, type and name.");
      }
      // MASK和OTHER类型不能带名称，非法配置
      if (entry.getName() != null && (entry.getType() == MASK ||
          entry.getType() == OTHER)) {
        throw new AclException(
          "Invalid ACL: this entry type must not have a name: " + entry + ".");
      }
      prevEntry = entry;
    }

    ScopedAclEntries scopedEntries = new ScopedAclEntries(aclBuilder);
    // 检查访问和默认作用域条目数都不超过最大值
    checkMaxEntries(scopedEntries);

    // 检查每个作用域都必须包含用户、组、其他三个基础条目
    for (AclEntryType type: EnumSet.of(USER, GROUP, OTHER)) {
      AclEntry accessEntryKey = new AclEntry.Builder().setScope(ACCESS)
        .setType(type).build();
      if (Collections.binarySearch(scopedEntries.getAccessEntries(),
          accessEntryKey, ACL_ENTRY_COMPARATOR) < 0) {
        throw new AclException(
          "Invalid ACL: the user, group and other entries are required.");
      }
      // 如果存在默认ACL，也需要检查默认ACL包含三个基础条目
      if (!scopedEntries.getDefaultEntries().isEmpty()) {
        AclEntry defaultEntryKey = new AclEntry.Builder().setScope(DEFAULT)
          .setType(type).build();
        if (Collections.binarySearch(scopedEntries.getDefaultEntries(),
            defaultEntryKey, ACL_ENTRY_COMPARATOR) < 0) {
          throw new AclException(
            "Invalid default ACL: the user, group and other entries are required.");
        }
      }
    }
    return Collections.unmodifiableList(aclBuilder);
  }

  /**
   * 分别检查访问和默认作用域的条目数不超过最大值
   * 对应HDFS-7582问题修复，需要分别统计两个作用域的条目数
   * @param scopedEntries 按作用域拆分后的ACL条目
   * @throws AclException 任一作用域超出限制抛出异常
   */
  private static void checkMaxEntries(ScopedAclEntries scopedEntries)
      throws AclException {
    List<AclEntry> accessEntries = scopedEntries.getAccessEntries();
    List<AclEntry> defaultEntries = scopedEntries.getDefaultEntries();
    if (accessEntries.size() > MAX_ENTRIES) {
      throw new AclException("Invalid ACL: ACL has " + accessEntries.size()
          + " access entries, which exceeds maximum of " + MAX_ENTRIES + ".");
    }
    if (defaultEntries.size() > MAX_ENTRIES) {
      throw new AclException("Invalid ACL: ACL has " + defaultEntries.size()
          + " default entries, which exceeds maximum of " + MAX_ENTRIES + ".");
    }
  }

  /**
   * 为ACL计算所需的mask条目，分别处理访问和默认两个作用域。
   * 处理逻辑：
   * 1. 如果需要mask但用户删除了mask，抛出异常
   * 2. 用户指定了mask，使用用户提供的mask
   * 3. 用户未指定但作用域有修改，自动计算新mask，权限为所有组类条目的权限并集
   *
   * @param aclBuilder 存储处理后的ACL条目列表
   * @param providedMask 用户提供的mask条目，按作用域索引
   * @param maskDirty 标记mask被修改的作用域
   * @param scopeDirty 标记有任意条目修改的作用域
   * @throws AclException 非法删除mask时抛出异常
   */
  private static void calculateMasks(List<AclEntry> aclBuilder,
      EnumMap<AclEntryScope, AclEntry> providedMask,
      EnumSet<AclEntryScope> maskDirty, EnumSet<AclEntryScope> scopeDirty)
      throws AclException {
    EnumSet<AclEntryScope> scopeFound = EnumSet.noneOf(AclEntryScope.class);
    EnumMap<AclEntryScope, FsAction> unionPerms =
      Maps.newEnumMap(AclEntryScope.class);
    EnumSet<AclEntryScope> maskNeeded = EnumSet.noneOf(AclEntryScope.class);
    // 遍历确定每个作用域，统计是否需要mask，计算权限并集
    for (