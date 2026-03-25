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

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;

/**
 * 文件级存储策略合规性统计类，聚合一组数据块的存储类型分布信息，统计符合和不符合指定存储策略的块数量
 * 用于HDFS存储策略合规性报告生成
 */
public class StoragePolicySummary {

  // 存储类型组合与对应块数量的映射
  Map<StorageTypeAllocation, Long> storageComboCounts = new HashMap<>();
  // 系统可用的所有存储策略数组
  final BlockStoragePolicy[] storagePolicies;
  // 统计的总块数
  int totalBlocks;

  /**
   * 构造函数，初始化存储策略统计对象
   * @param storagePolicies 系统所有可用存储策略数组
   */
  StoragePolicySummary(BlockStoragePolicy[] storagePolicies) {
    this.storagePolicies = storagePolicies;
  }

  /**
   * 添加一个块的存储类型分配信息，更新对应组合计数
   * @param storageTypes 当前块使用的存储类型数组
   * @param policy 该块所属文件指定的存储策略
   */
  void add(StorageType[] storageTypes, BlockStoragePolicy policy) {
    StorageTypeAllocation storageCombo = 
        new StorageTypeAllocation(storageTypes, policy);
    Long count = storageComboCounts.get(storageCombo);
    if (count == null) {
      storageComboCounts.put(storageCombo, 1l);
      // 匹配该存储类型组合实际对应的存储策略
      storageCombo.setActualStoragePolicy(
          getStoragePolicy(storageCombo.getStorageTypes()));
    } else {
      storageComboCounts.put(storageCombo, count.longValue()+1);
    }
    totalBlocks++;
  }

  /**
   * 按存储组合的块数量降序排序所有存储类型组合
   * @param unsortMap 未排序的存储组合计数映射
   * @return 排序后的存储组合条目列表
   */
  static List<Entry<StorageTypeAllocation, Long>> sortByComparator(
      Map<StorageTypeAllocation, Long> unsortMap) {
    List<Entry<StorageTypeAllocation, Long>> storageAllocations = 
        new LinkedList<>(unsortMap.entrySet());
    // 根据块数量降序排序
    Collections.sort(storageAllocations, 
      new Comparator<Entry<StorageTypeAllocation, Long>>() {
          public int compare(Entry<StorageTypeAllocation, Long> o1,
              Entry<StorageTypeAllocation, Long> o2)
          {
            return o2.getValue().compareTo(o1.getValue());
          }
    });
    return storageAllocations;
  }

  @Override
  public String toString() {
    // 符合策略块的字符串构建器
    StringBuilder compliantBlocksSB = new StringBuilder();
    compliantBlocksSB
        .append("\nBlocks satisfying the specified storage policy:")
        .append("\nStorage Policy"
            + "                  # of blocks       % of blocks\n");
    // 不符合策略块的字符串构建器
    StringBuilder nonCompliantBlocksSB = new StringBuilder();
    Formatter compliantFormatter = new Formatter(compliantBlocksSB);
    Formatter nonCompliantFormatter = new Formatter(nonCompliantBlocksSB);
    // 百分比格式化，保留四位小数
    NumberFormat percentFormat = NumberFormat.getPercentInstance();
    percentFormat.setMinimumFractionDigits(4);
    percentFormat.setMaximumFractionDigits(4);
    // 遍历排序后的所有存储组合
    for (Map.Entry<StorageTypeAllocation, Long> storageComboCount:
      sortByComparator(storageComboCounts)) {
      // 计算当前组合占总块数的百分比
      double percent = (double) storageComboCount.getValue() / 
          (double) totalBlocks;
      StorageTypeAllocation sta = storageComboCount.getKey();
      if (sta.policyMatches()) {
        // 添加到符合策略列表
        compliantFormatter.format("%-25s %10d  %20s%n",
            sta.getStoragePolicyDescriptor(),
            storageComboCount.getValue(),
            percentFormat.format(percent));
      } else {
        // 首次添加不符合策略时输出表头
        if (nonCompliantBlocksSB.length() == 0) {
          nonCompliantBlocksSB
              .append("\nBlocks NOT satisfying the specified storage policy:")
              .append("\nStorage Policy                  ")
              .append(
              "Specified Storage Policy      # of blocks       % of blocks\n");
        }
        // 添加到不符合策略列表
        nonCompliantFormatter.format("%-35s %-20s %10d  %20s%n",
            sta.getStoragePolicyDescriptor(),
            sta.getSpecifiedStoragePolicy().getName(),
            storageComboCount.getValue(),
            percentFormat.format(percent));
      }
    }
    // 所有块都符合策略时输出提示
    if (nonCompliantBlocksSB.length() == 0) {
      nonCompliantBlocksSB.append("\nAll blocks satisfy specified storage policy.\n");
    }
    compliantFormatter.close();
    nonCompliantFormatter.close();
    return compliantBlocksSB.toString() + nonCompliantBlocksSB;
  }

  /**
   * 根据给定的存储类型组合匹配对应的存储策略
   * @param storageTypes 已排序的存储类型数组
   * @return 匹配到的存储策略，无匹配返回null
   */
  private BlockStoragePolicy getStoragePolicy(StorageType[] storageTypes) {
    // 遍历所有存储策略进行匹配
    for (BlockStoragePolicy storagePolicy:storagePolicies) {
      // 获取策略要求的存储类型并排序
      StorageType[] policyStorageTypes = storagePolicy.getStorageTypes();
      policyStorageTypes = Arrays.copyOf(policyStorageTypes, policyStorageTypes.length);
      Arrays.sort(policyStorageTypes);
      if (policyStorageTypes.length <= storageTypes.length) {
        int i = 0;
        // 顺序匹配前N个存储类型
        for (; i < policyStorageTypes.length; i++) {
          if (policyStorageTypes[i] != storageTypes[i]) {
            break;
          }
        }
        if (i < policyStorageTypes.length) {
          continue;
        }
        // 剩余存储类型必须和策略最后一个类型相同
        int j=policyStorageTypes.length;
        for (; j < storageTypes.length; j++) {
          if (policyStorageTypes[i-1] != storageTypes[j]) {
            break;
          }
        }

        if (j==storageTypes.length) {
          return storagePolicy;
        }
      }
    }
    return null;
  }

  /**
   * 内部类，代表唯一的存储类型分配组合，关联指定存储策略和实际匹配到的存储策略
   */
  static class StorageTypeAllocation {
    // 文件指定的存储策略
    private final BlockStoragePolicy specifiedStoragePolicy;
    // 当前块实际使用的存储类型数组（已排序）
    private final StorageType[] storageTypes;
    // 实际存储类型组合匹配到的存储策略
    private BlockStoragePolicy actualStoragePolicy;

    /**
     * 构造存储类型分配对象，自动对存储类型数组排序
     * @param storageTypes 实际存储类型数组
     * @param specifiedStoragePolicy 文件指定的存储策略
     */
    StorageTypeAllocation(StorageType[] storageTypes, 
        BlockStoragePolicy specifiedStoragePolicy) {
      Arrays.sort(storageTypes);
      this.storageTypes = storageTypes;
      this.specifiedStoragePolicy = specifiedStoragePolicy;
    }
    
    /**
     * 获取实际存储类型数组
     * @return 已排序的实际存储类型数组
     */
    StorageType[] getStorageTypes() {
      return storageTypes;
    }

    /**
     * 获取文件指定的存储策略
     * @return 指定的存储策略
     */
    BlockStoragePolicy getSpecifiedStoragePolicy() {
      return specifiedStoragePolicy;
    }
    
    /**
     * 设置实际匹配到的存储策略
     * @param actualStoragePolicy 实际匹配的存储策略
     */
    void setActualStoragePolicy(BlockStoragePolicy actualStoragePolicy) {
      this.actualStoragePolicy = actualStoragePolicy;
    }
    
    /**
     * 获取实际匹配到的存储策略
     * @return 实际匹配的存储策略
     */
    BlockStoragePolicy getActualStoragePolicy() {
      return actualStoragePolicy;
    }

    /**
     * 将存储类型计数映射转换为字符串描述
     * @param storageType_countmap 存储类型到数量的映射
     * @return 格式化的存储类型分布字符串
     */
    private static String getStorageAllocationAsString
      (Map<StorageType, Integer> storageType_countmap) {
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<StorageType, Integer> 
      storageTypeCountEntry:storageType_countmap.entrySet()) {
        sb.append(storageTypeCountEntry.getKey().name()+ ":"
            + storageTypeCountEntry.getValue() + ",");
      }
      if (sb.length() > 1) {
        sb.deleteCharAt(sb.length()-1);
      }
      return sb.toString();
    }

    /**
     * 将当前存储类型分配转换为字符串描述
     * @return 格式化的存储类型分布字符串
     */
    private String getStorageAllocationAsString() {
      Map<StorageType, Integer> storageType_countmap = 
          new EnumMap<>(StorageType.class);
      // 统计每种存储类型的数量
      for (StorageType storageType: storageTypes) {
        Integer count = storageType_countmap.get(storageType);
        if (count == null) {
          storageType_countmap.put(storageType, 1);
        } else {
          storageType_countmap.put(storageType, count.intValue()+1);
        }
      }
      return (getStorageAllocationAsString(storageType_countmap));
    }
    
    /**
     * 获取存储策略的描述字符串，包含存储类型分布和实际策略名称
     * @return 存储策略描述字符串
     */
    String getStoragePolicyDescriptor() {
      StringBuilder storagePolicyDescriptorSB = new StringBuilder();
      if (actualStoragePolicy!=null) {
        storagePolicyDescriptorSB.append(getStorageAllocationAsString())
        .append("(")
        .append(actualStoragePolicy.getName())
        .append(")");
      } else {
        storagePolicyDescriptorSB.append(getStorageAllocationAsString());
      }
      return storagePolicyDescriptorSB.toString();
    }
    
    /**
     * 检查指定存储策略是否和实际匹配的存储策略一致
     * @return true 一致（符合策略），false 不一致（不符合策略）
     */
    boolean policyMatches() {
      return specifiedStoragePolicy.equals(actualStoragePolicy);
    }
    
    @Override
    public String toString() {
      return specifiedStoragePolicy.getName() + "|" + getStoragePolicyDescriptor();
    }

    @Override
    public int hashCode() {
      return Objects.hash(specifiedStoragePolicy,Arrays.hashCode(storageTypes));
    }

    @Override
    public boolean equals(Object another) {
      return (another instanceof StorageTypeAllocation && 
          Objects.equals(specifiedStoragePolicy,
              ((StorageTypeAllocation)another).specifiedStoragePolicy) &&
              Arrays.equals(storageTypes,
                  ((StorageTypeAllocation)another).storageTypes));
    }
  }
}