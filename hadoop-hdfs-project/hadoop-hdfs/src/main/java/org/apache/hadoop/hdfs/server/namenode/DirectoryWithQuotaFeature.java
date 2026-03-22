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

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.security.AccessControlException;

/**
 * 目录配额特性，为INodeDirectory提供配额统计与检查能力
 * 负责存储目录的配额限制与实际使用情况，支持命名空间、存储空间、按存储类型分类三种配额
 */
public final class DirectoryWithQuotaFeature implements INode.Feature {
  /** 默认命名空间配额：不限制 */
  public static final long DEFAULT_NAMESPACE_QUOTA = Long.MAX_VALUE;
  /** 默认存储空间配额：未设置配额 */
  public static final long DEFAULT_STORAGE_SPACE_QUOTA = HdfsConstants.QUOTA_RESET;

  /** 配额限制信息，存储各类配额的最大值 */
  private QuotaCounts quota;
  /** 当前使用情况，存储各类配额的实际用量 */
  private QuotaCounts usage;

  /**
   * DirectoryWithQuotaFeature构建器，支持流式设置各类配额
   */
  public static class Builder {
    private QuotaCounts quota;
    private QuotaCounts usage;

    /**
     * 构造构建器，初始化默认配额值和初始使用量
     */
    public Builder() {
      this.quota = new QuotaCounts.Builder().nameSpace(DEFAULT_NAMESPACE_QUOTA).
          storageSpace(DEFAULT_STORAGE_SPACE_QUOTA).
          typeSpaces(DEFAULT_STORAGE_SPACE_QUOTA).build();
      this.usage = new QuotaCounts.Builder().nameSpace(1).build();
    }

    /**
     * 设置命名空间配额
     * @param nameSpaceQuota 命名空间配额值
     * @return 当前构建器实例
     */
    public Builder nameSpaceQuota(long nameSpaceQuota) {
      this.quota.setNameSpace(nameSpaceQuota);
      return this;
    }

    /**
     * 设置总存储空间配额
     * @param spaceQuota 总存储空间配额值
     * @return 当前构建器实例
     */
    public Builder storageSpaceQuota(long spaceQuota) {
      this.quota.setStorageSpace(spaceQuota);
      return this;
    }

    /**
     * 批量设置按存储类型分类的配额
     * @param typeQuotas 各存储类型的配额计数器
     * @return 当前构建器实例
     */
    public Builder typeQuotas(EnumCounters<StorageType> typeQuotas) {
      this.quota.setTypeSpaces(typeQuotas);
      return this;
    }

    /**
     * 设置单个存储类型的配额
     * @param type 存储类型
     * @param quota 该存储类型的配额值
     * @return 当前构建器实例
     */
    public Builder typeQuota(StorageType type, long quota) {
      this.quota.setTypeSpace(type, quota);
      return this;
    }

    /**
     * 构建DirectoryWithQuotaFeature实例
     * @return 构建完成的配额特性实例
     */
    public DirectoryWithQuotaFeature build() {
      return new DirectoryWithQuotaFeature(this);
    }
  }

  /**
   * 构造方法，通过构建器初始化配额特性
   * @param builder 包含配额和使用量信息的构建器
   */
  private DirectoryWithQuotaFeature(Builder builder) {
    this.quota = builder.quota;
    this.usage = builder.usage;
  }

  /**
   * 获取当前目录的配额配置，返回拷贝避免外部修改内部状态
   * @return 配额配置信息的拷贝
   */
  QuotaCounts getQuota() {
    return new QuotaCounts.Builder().quotaCount(this.quota).build();
  }

  /**
   * 设置命名空间和总存储空间配额
   * @param nsQuota 命名空间配额
   * @param ssQuota 总存储空间配额
   */
  void setQuota(long nsQuota, long ssQuota) {
    this.quota.setNameSpace(nsQuota);
    this.quota.setStorageSpace(ssQuota);
  }

  /**
   * 设置指定存储类型的配额
   * @param quota 配额值
   * @param type 存储类型
   */
  void setQuota(long quota, StorageType type) {
    this.quota.setTypeSpace(type, quota);
  }

  /**
   * 批量设置存储类型配额，仅用于加载FSImage时
   *
   * @param tsQuotas 所有支持配额的存储类型配额计数
   */
  void setQuota(EnumCounters<StorageType> tsQuotas) {
    this.quota.setTypeSpaces(tsQuotas);
  }

  /**
   * 将当前目录的空间用量添加到传入的计数对象中，用于汇总目录树用量
   * @param counts 待累加的计数对象
   * @return 累加完成后的计数对象
   */
  QuotaCounts AddCurrentSpaceUsage(QuotaCounts counts) {
    counts.add(this.usage);
    return counts;
  }

  /**
   * 计算目录内容摘要，并在计算完成后校验缓存的存储空间用量一致性
   * @param dir 当前目录INode对象
   * @param summary 内容摘要计算上下文
   * @return 计算完成的内容摘要上下文
   * @throws AccessControlException 权限校验失败时抛出
   */
  ContentSummaryComputationContext computeContentSummary(final INodeDirectory dir,
      final ContentSummaryComputationContext summary)
      throws AccessControlException {
    final long original = summary.getCounts().getStoragespace();
    long oldYieldCount = summary.getYieldCount();
    dir.computeDirectoryContentSummary(summary, Snapshot.CURRENT_STATE_ID);
    // 仅当本次计算过程中没有中断yield时，才进行一致性校验
    if (oldYieldCount == summary.getYieldCount()) {
      checkStoragespace(dir, summary.getCounts().getStoragespace() - original);
    }
    return summary;
  }

  /**
   * 校验缓存的存储空间用量和重新计算得到的用量是否一致，不一致则打警告日志
   * @param dir 当前目录
   * @param computed 重新计算得到的当前目录存储空间用量
   */
  private void checkStoragespace(final INodeDirectory dir, final long computed) {
    if (-1 != quota.getStorageSpace() && usage.getStorageSpace() != computed) {
      NameNode.LOG.warn("BUG: Inconsistent storagespace for directory "
          + dir.getFullPathName() + ". Cached = " + usage.getStorageSpace()
          + " != Computed = " + computed);
    }
  }

  /**
   * 将用量变化量更新到缓存中，修改当前目录的配额使用统计
   * @param delta 命名空间/空间/类型用量的变化量
   */
  public void addSpaceConsumed2Cache(QuotaCounts delta) {
    usage.add(delta);
  }

  /** 
   * 直接设置当前目录树的各类用量，不做配额校验，调用方需要保证正确性
   * 
   * @param namespace 要设置的命名空间用量
   * @param storagespace 要设置的总存储空间用量
   * @param typespaces 各存储类型的用量计数器
   */
  void setSpaceConsumed(long namespace, long storagespace,
      EnumCounters<StorageType> typespaces) {
    usage.setNameSpace(namespace);
    usage.setStorageSpace(storagespace);
    usage.setTypeSpaces(typespaces);
  }

  /**
   * 通过QuotaCounts对象直接设置所有用量
   * @param c 包含各类用量的QuotaCounts对象
   */
  void setSpaceConsumed(QuotaCounts c) {
    usage.setNameSpace(c.getNameSpace());
    usage.setStorageSpace(c.getStorageSpace());
    usage.setTypeSpaces(c.getTypeSpaces());
  }

  /**
   * 获取当前目录允许的各类配额最大值，返回拷贝避免外部修改内部状态
   * @return 配额最大值信息
   */
  public QuotaCounts getSpaceAllowed() {
    return new QuotaCounts.Builder().quotaCount(quota).build();
  }

  /**
   * 获取当前目录实际消耗的各类配额用量，返回拷贝避免外部修改内部状态
   * @return 当前用量信息
   */
  public QuotaCounts getSpaceConsumed() {
    return new QuotaCounts.Builder().quotaCount(usage).build();
  }

  /**
   * 校验加上变化量后是否违反命名空间配额
   * @param delta 命名空间用量变化量
   * @throws NSQuotaExceededException 配额超出时抛出
   */
  private void verifyNamespaceQuota(long delta) throws NSQuotaExceededException {
    if (Quota.isViolated(quota.getNameSpace(), usage.getNameSpace(), delta)) {
      throw new NSQuotaExceededException(quota.getNameSpace(),
          usage.getNameSpace() + delta);
    }
  }

  /**
   * 校验加上变化量后是否违反总存储空间配额
   * @param delta 存储空间用量变化量
   * @throws DSQuotaExceededException 配额超出时抛出
   */
  private void verifyStoragespaceQuota(long delta) throws DSQuotaExceededException {
    if (Quota.isViolated(quota.getStorageSpace(), usage.getStorageSpace(), delta)) {
      throw new DSQuotaExceededException(quota.getStorageSpace(),
          usage.getStorageSpace() + delta);
    }
  }

  /**
   * 校验加上变化量后是否违反任何存储类型配额
   * @param typeDelta 各存储类型用量变化量
   * @throws QuotaByStorageTypeExceededException 某存储类型配额超出时抛出
   */
  private void verifyQuotaByStorageType(EnumCounters<StorageType> typeDelta)
      throws QuotaByStorageTypeExceededException {
    // 未设置任何存储类型配额直接跳过校验
    if (!isQuotaByStorageTypeSet()) {
      return;
    }
    // 遍历所有支持配额的存储类型逐个校验
    for (StorageType t: StorageType.getTypesSupportingQuota()) {
      // 当前存储类型未设置配额跳过
      if (!isQuotaByStorageTypeSet(t)) {
        continue;
      }
      // 校验是否超出配额
      if (Quota.isViolated(quota.getTypeSpace(t), usage.getTypeSpace(t),
          typeDelta.get(t))) {
        throw new QuotaByStorageTypeExceededException(
          quota.getTypeSpace(t), usage.getTypeSpace(t) + typeDelta.get(t), t);
      }
    }
  }

  /**
   * 一次性校验命名空间、总存储空间、存储类型三类配额，任意超出则抛出异常
   * @param counts 三类配额的变化量
   * @throws QuotaExceededException 任意配额超出时抛出
   */
  void verifyQuota(QuotaCounts counts) throws QuotaExceededException {
    verifyNamespaceQuota(counts.getNameSpace());
    verifyStoragespaceQuota(counts.getStorageSpace());
    verifyQuotaByStorageType(counts.getTypeSpaces());
  }

  /**
   * 检查当前目录是否设置了任何配额
   * @return true表示设置了至少一种配额，false表示未设置任何配额
   */
  boolean isQuotaSet() {
    return quota.anyNsSsCountGreaterOrEqual(0) ||
        quota.anyTypeSpaceCountGreaterOrEqual(0);
  }

  /**
   * 检查是否设置了任何按存储类型分类的配额
   * @return true表示至少设置了一种存储类型配额，false表示未设置
   */
  boolean isQuotaByStorageTypeSet() {
    return quota.anyTypeSpaceCountGreaterOrEqual(0);
  }

  /**
   * 检查指定存储类型是否设置了配额
   * @param t 存储类型
   * @return true表示该存储类型设置了配额，false表示未设置
   */
  boolean isQuotaByStorageTypeSet(StorageType t) {
    return quota.getTypeSpace(t) >= 0;
  }

  /**
   * 生成命名空间配额字符串信息
   * @return 命名空间配额字符串
   */
  private String namespaceString() {
    return "namespace: " + (quota.getNameSpace() < 0? "-":
        usage.getNameSpace() + "/" + quota.getNameSpace());
  }

  /**
   * 生成总存储空间配额字符串信息
   * @return 总存储空间配额字符串
   */
  private String storagespaceString() {
    return "storagespace: " + (quota.getStorageSpace() < 0? "-":
        usage.getStorageSpace() + "/" + quota.getStorageSpace());
  }

  /**
   * 生成各存储类型配额字符串信息
   * @return 存储类型配额字符串
   */
  private String typeSpaceString() {
    StringBuilder sb = new StringBuilder();
    for (StorageType t : StorageType.getTypesSupportingQuota()) {
      sb.append("StorageType: " + t +
          (quota.getTypeSpace(t) < 0? "-":
          usage.getTypeSpace(t) + "/" + usage.getTypeSpace(t)));
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return "Quota[" + namespaceString() + ", " + storagespaceString() +
        ", " + typeSpaceString() + "]";
  }
}