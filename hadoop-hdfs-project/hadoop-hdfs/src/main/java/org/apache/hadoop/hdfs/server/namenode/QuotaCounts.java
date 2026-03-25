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
 * Unless required by applicable law or agreed to writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.util.ConstEnumCounters;
import org.apache.hadoop.hdfs.util.EnumCounters;

import java.util.function.Consumer;

/**
 * 文件配额计数器，用于管理命名空间、存储空间和存储类型配额的已使用量和配额限制
 * 实现了对象复用优化，对常用默认值和重置值使用预定义常量对象避免重复分配
 */
public class QuotaCounts {

  /**
   * 预定义4种最常用的计数器对象实现对象复用。当计数正好是这几种常用值时，直接引用预定义对象
   * 避免创建大量相同值的重复对象，优化内存占用。参见HDFS-14547
   */
  final static EnumCounters<Quota> QUOTA_RESET =
      new ConstEnumCounters<>(Quota.class, HdfsConstants.QUOTA_RESET);
  final static EnumCounters<Quota> QUOTA_DEFAULT =
      new ConstEnumCounters<>(Quota.class, 0);
  final static EnumCounters<StorageType> STORAGE_TYPE_RESET =
      new ConstEnumCounters<>(StorageType.class, HdfsConstants.QUOTA_RESET);
  final static EnumCounters<StorageType> STORAGE_TYPE_DEFAULT =
      new ConstEnumCounters<>(StorageType.class, 0);

  /**
   * 修改计数器，处理常量计数器的复制逻辑：如果计数器是不可变常量，则先深拷贝为可变对象再修改
   * @param counter 待修改的计数器
   * @param action 具体修改操作
   * @return 修改后的计数器对象
   */
  static <T extends Enum<T>> EnumCounters<T> modify(EnumCounters<T> counter,
      Consumer<EnumCounters<T>> action) {
    if (counter instanceof ConstEnumCounters) {
      counter = counter.deepCopyEnumCounter();
    }
    action.accept(counter);
    return counter;
  }

  /**
   * 命名空间和存储空间计数器（HDFS-7775将原磁盘空间计数重构为通用存储空间计数）
   */
  @VisibleForTesting
  EnumCounters<Quota> nsSsCounts;
  /**
   * 各存储类型存储空间计数器
   */
  @VisibleForTesting
  EnumCounters<StorageType> tsCounts;

  /**
   * QuotaCounts构造器，使用Builder模式构建QuotaCounts对象
   */
  public static class Builder {
    private EnumCounters<Quota> nsSsCounts;
    private EnumCounters<StorageType> tsCounts;

    public Builder() {
      this.nsSsCounts = QUOTA_DEFAULT;
      this.tsCounts = STORAGE_TYPE_DEFAULT;
    }

    /**
     * 设置命名空间配额/用量值
     * @param val 命名空间数值
     * @return 当前Builder实例
     */
    public Builder nameSpace(long val) {
      nsSsCounts =
          setQuotaCounter(nsSsCounts, Quota.NAMESPACE, Quota.STORAGESPACE, val);
      return this;
    }

    /**
     * 设置存储空间配额/用量值
     * @param val 存储空间数值
     * @return 当前Builder实例
     */
    public Builder storageSpace(long val) {
      nsSsCounts =
          setQuotaCounter(nsSsCounts, Quota.STORAGESPACE, Quota.NAMESPACE, val);
      return this;
    }

    /**
     * 设置所有存储类型的配额/用量
     * @param val 存储类型计数器
     * @return 当前Builder实例
     */
    public Builder typeSpaces(EnumCounters<StorageType> val) {
      if (val != null) {
        if (val == STORAGE_TYPE_DEFAULT || val == STORAGE_TYPE_RESET) {
          tsCounts = val;
        } else {
          tsCounts = modify(tsCounts, ec -> ec.set(val));
        }
      }
      return this;
    }

    /**
     * 将所有存储类型配额/用量统一设置为指定值
     * @param val 统一设置的数值
     * @return 当前Builder实例
     */
    public Builder typeSpaces(long val) {
      if (val == HdfsConstants.QUOTA_RESET) {
        tsCounts = STORAGE_TYPE_RESET;
      } else if (val == 0) {
        tsCounts = STORAGE_TYPE_DEFAULT;
      } else {
        tsCounts = modify(tsCounts, ec -> ec.reset(val));
      }
      return this;
    }

    /**
     * 从已有QuotaCounts对象复制值构建新Builder
     * @param that 源QuotaCounts对象
     * @return 当前Builder实例
     */
    public Builder quotaCount(QuotaCounts that) {
      if (that.nsSsCounts == QUOTA_DEFAULT || that.nsSsCounts == QUOTA_RESET) {
        nsSsCounts = that.nsSsCounts;
      } else {
        nsSsCounts = modify(nsSsCounts, ec -> ec.set(that.nsSsCounts));
      }
      if (that.tsCounts == STORAGE_TYPE_DEFAULT
          || that.tsCounts == STORAGE_TYPE_RESET) {
        tsCounts = that.tsCounts;
      } else {
        tsCounts = modify(tsCounts, ec -> ec.set(that.tsCounts));
      }
      return this;
    }

    /**
     * 构建最终QuotaCounts对象
     * @return 构造完成的QuotaCounts实例
     */
    public QuotaCounts build() {
      return new QuotaCounts(this);
    }
  }

  private QuotaCounts(Builder builder) {
    this.nsSsCounts = builder.nsSsCounts;
    this.tsCounts = builder.tsCounts;
  }

  /**
   * 将另一个QuotaCounts的所有计数加到当前对象
   * @param that 待相加的QuotaCounts
   * @return 当前对象
   */
  public QuotaCounts add(QuotaCounts that) {
    nsSsCounts = modify(nsSsCounts, ec -> ec.add(that.nsSsCounts));
    tsCounts = modify(tsCounts, ec -> ec.add(that.tsCounts));
    return this;
  }

  /**
   * 从当前对象减去另一个QuotaCounts的所有计数
   * @param that 待减去的QuotaCounts
   * @return 当前对象
   */
  public QuotaCounts subtract(QuotaCounts that) {
    nsSsCounts = modify(nsSsCounts, ec -> ec.subtract(that.nsSsCounts));
    tsCounts = modify(tsCounts, ec -> ec.subtract(that.tsCounts));
    return this;
  }

  /**
   * 返回当前所有计数取反后的新QuotaCounts对象
   * @return 计数取反后的新对象
   */
  public QuotaCounts negation() {
    QuotaCounts ret = new QuotaCounts.Builder().quotaCount(this).build();
    ret.nsSsCounts = modify(ret.nsSsCounts, ec -> ec.negation());
    ret.tsCounts = modify(ret.tsCounts, ec -> ec.negation());
    return ret;
  }

  /**
   * 获取命名空间计数值
   * @return 命名空间计数
   */
  public long getNameSpace(){
    return nsSsCounts.get(Quota.NAMESPACE);
  }

  /**
   * 设置命名空间计数值
   * @param nameSpaceCount 命名空间计数值
   */
  public void setNameSpace(long nameSpaceCount) {
    nsSsCounts =
        setQuotaCounter(nsSsCounts, Quota.NAMESPACE, Quota.STORAGESPACE,
            nameSpaceCount);
  }

  /**
   * 给命名空间计数增加增量
   * @param nsDelta 增量值
   */
  public void addNameSpace(long nsDelta) {
    nsSsCounts = modify(nsSsCounts, ec -> ec.add(Quota.NAMESPACE, nsDelta));
  }

  /**
   * 获取存储空间计数值
   * @return 存储空间计数
   */
  public long getStorageSpace(){
    return nsSsCounts.get(Quota.STORAGESPACE);
  }

  /**
   * 设置存储空间计数值
   * @param spaceCount 存储空间计数值
   */
  public void setStorageSpace(long spaceCount) {
    nsSsCounts =
        setQuotaCounter(nsSsCounts, Quota.STORAGESPACE, Quota.NAMESPACE,
            spaceCount);
  }

  /**
   * 给存储空间计数增加增量
   * @param dsDelta 增量值
   */
  public void addStorageSpace(long dsDelta) {
    nsSsCounts = modify(nsSsCounts, ec -> ec.add(Quota.STORAGESPACE, dsDelta));
  }

  /**
   * 获取所有存储类型的计数快照
   * @return 存储类型计数器的深拷贝
   */
  public EnumCounters<StorageType> getTypeSpaces() {
    EnumCounters<StorageType> ret =
        new EnumCounters<StorageType>(StorageType.class);
    ret.set(tsCounts);
    return ret;
  }

  /**
   * 设置所有存储类型计数
   * @param that 源存储类型计数器
   */
  void setTypeSpaces(EnumCounters<StorageType> that) {
    if (that == STORAGE_TYPE_DEFAULT || that == STORAGE_TYPE_RESET) {
      tsCounts = that;
    } else if (that != null) {
      tsCounts = modify(tsCounts, ec -> ec.set(that));
    }
  }

  /**
   * 获取指定存储类型的计数值
   * @param type 存储类型
   * @return 指定存储类型的计数
   */
  long getTypeSpace(StorageType type) {
    return this.tsCounts.get(type);
  }

  /**
   * 设置指定存储类型的计数值
   * @param type 存储类型
   * @param spaceCount 计数值
   */
  void setTypeSpace(StorageType type, long spaceCount) {
    tsCounts = modify(tsCounts, ec -> ec.set(type, spaceCount));
  }

  /**
   * 给指定存储类型计数增加增量
   * @param type 存储类型
   * @param delta 增量值
   */
  public void addTypeSpace(StorageType type, long delta) {
    tsCounts = modify(tsCounts, ec -> ec.add(type, delta));
  }

  /**
   * 检查命名空间或存储空间是否存在任意计数大于等于指定值
   * @param val 比较阈值
   * @return 是否存在任意计数大于等于阈值
   */
  public boolean anyNsSsCountGreaterOrEqual(long val) {
  if (nsSsCounts == QUOTA_DEFAULT) {
      return val <= 0;
    } else if (nsSsCounts == QUOTA_RESET) {
      return val <= HdfsConstants.QUOTA_RESET;
    }
    return nsSsCounts.anyGreaterOrEqual(val);
  }

  /**
   * 检查任意存储类型计数是否大于等于指定值
   * @param val 比较阈值
   * @return 是否存在任意存储类型计数大于等于阈值
   */
  public boolean anyTypeSpaceCountGreaterOrEqual(long val) {
    if (tsCounts == STORAGE_TYPE_DEFAULT) {
      return val <= 0;
    } else if (tsCounts == STORAGE_TYPE_RESET) {
      return val <= HdfsConstants.QUOTA_RESET;
    }
    return tsCounts.anyGreaterOrEqual(val);
  }

  /**
   * 设置指定配额类型的计数值，尽可能复用预定义常量对象
   * @param inputCounts 输入计数器
   * @param quotaToSet 待设置的配额类型
   * @param otherQuota 另一个配额类型（用于判断是否可以复用预定义对象）
   * @param val 待设置的值
   * @return 修改后的计数器
   */
  private static EnumCounters<Quota> setQuotaCounter(
      EnumCounters<Quota> inputCounts, Quota quotaToSet, Quota otherQuota,
      long val) {
    if (val == HdfsConstants.QUOTA_RESET
        && inputCounts.get(otherQuota) == HdfsConstants.QUOTA_RESET) {
      return QUOTA_RESET;
    } else if (val == 0 && inputCounts.get(otherQuota) == 0) {
      return QUOTA_DEFAULT;
    } else {
      return modify(inputCounts, ec -> ec.set(quotaToSet, val));
    }
  }

  @Override
  public String toString() {
    return "name space=" + getNameSpace() +
        "\nstorage space=" + getStorageSpace() +
        "\nstorage types=" + getTypeSpaces();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (!(obj instanceof QuotaCounts)) {
      return false;
    }
    final QuotaCounts that = (QuotaCounts)obj;
    return this.nsSsCounts.equals(that.nsSsCounts)
        && this.tsCounts.equals(that.tsCounts);
  }

  @Override
  public int hashCode() {
    assert false : "hashCode not designed";
    return 42; // any arbitrary constant will do
  }

}