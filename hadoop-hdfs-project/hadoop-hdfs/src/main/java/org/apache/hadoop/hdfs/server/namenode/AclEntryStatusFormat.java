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

import java.util.List;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.util.LongBitFormat;

/**
 * 文件路径：hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/AclEntryStatusFormat.java
 * <p>
 * 将ACL访问控制表条目编码工具，将完整AclEntry打包编码为32位整数存储。
 * 编码格式同时用于内存存储和磁盘持久化，格式修改会导致兼容性问题。
 * 采用大端字节序存储，各字段按PERMISSION(3位)、TYPE(2位)、SCOPE(1位)、NAME(24位)排列。
 */
public enum AclEntryStatusFormat implements LongBitFormat.Enum {

  /** 权限字段，占用3位，对应FsAction的枚举值 */
  PERMISSION(null, 3),
  /** 条目类型字段，占用2位，对应用户/组/其他等类型 */
  TYPE(PERMISSION.BITS, 2),
  /** 作用域字段，占用1位，区分访问ACL默认ACL */
  SCOPE(TYPE.BITS, 1),
  /** 名称序列号字段，占用24位，存储用户名/组名的序列号 */
  NAME(SCOPE.BITS, 24);

  /** 预缓存FsAction枚举数组，用于快速通过序号获取实例 */
  private static final FsAction[] FSACTION_VALUES = FsAction.values();
  /** 预缓存AclEntryScope枚举数组，用于快速通过序号获取实例 */
  private static final AclEntryScope[] ACL_ENTRY_SCOPE_VALUES =
      AclEntryScope.values();
  /** 预缓存AclEntryType枚举数组，用于快速通过序号获取实例 */
  private static final AclEntryType[] ACL_ENTRY_TYPE_VALUES =
      AclEntryType.values();

  /** 当前字段的位格式定义 */
  private final LongBitFormat BITS;

  /**
   * 构造枚举实例，初始化当前字段的位格式定义
   * @param previous 前一个相邻字段的位格式，用于计算当前字段起始偏移
   * @param length 当前字段占用的比特数
   */
  private AclEntryStatusFormat(LongBitFormat previous, int length) {
    BITS = new LongBitFormat(name(), previous, length, 0);
  }

  /**
   * 从编码后的整数中提取ACL作用域
   * @param aclEntry 编码后的ACL条目整数
   * @return 提取出的AclEntryScope
   */
  static AclEntryScope getScope(int aclEntry) {
    int ordinal = (int) SCOPE.BITS.retrieve(aclEntry);
    return ACL_ENTRY_SCOPE_VALUES[ordinal];
  }

  /**
   * 从编码后的整数中提取ACL条目类型
   * @param aclEntry 编码后的ACL条目整数
   * @return 提取出的AclEntryType
   */
  static AclEntryType getType(int aclEntry) {
    int ordinal = (int) TYPE.BITS.retrieve(aclEntry);
    return ACL_ENTRY_TYPE_VALUES[ordinal];
  }

  /**
   * 从编码后的整数中提取访问权限
   * @param aclEntry 编码后的ACL条目整数
   * @return 提取出的FsAction权限
   */
  static FsAction getPermission(int aclEntry) {
    int ordinal = (int) PERMISSION.BITS.retrieve(aclEntry);
    return FSACTION_VALUES[ordinal];
  }

  /**
   * 从编码后的整数中提取用户名/组名，使用默认字符串表解析
   * @param aclEntry 编码后的ACL条目整数
   * @return 解析后的名称字符串
   */
  static String getName(int aclEntry) {
    return getName(aclEntry, null);
  }

  /**
   * 从编码后的整数中提取用户名/组名，使用指定字符串表解析
   * @param aclEntry 编码后的ACL条目整数
   * @param stringTable 序列号对应的字符串表，用于名称解析
   * @return 解析后的名称字符串
   */
  static String getName(int aclEntry,
                        SerialNumberManager.StringTable stringTable) {
    SerialNumberManager snm = getSerialNumberManager(getType(aclEntry));
    if (snm != null) {
      int nid = (int)NAME.BITS.retrieve(aclEntry);
      return snm.getString(nid, stringTable);
    }
    return null;
  }

  /**
   * 将完整AclEntry对象编码为整数
   * @param aclEntry 待编码的AclEntry对象
   * @return 编码后的32位整数
   */
  static int toInt(AclEntry aclEntry) {
    long aclEntryInt = 0;
    // 编码作用域字段
    aclEntryInt = SCOPE.BITS
        .combine(aclEntry.getScope().ordinal(), aclEntryInt);
    // 编码类型字段
    aclEntryInt = TYPE.BITS.combine(aclEntry.getType().ordinal(), aclEntryInt);
    // 编码权限字段
    aclEntryInt = PERMISSION.BITS.combine(aclEntry.getPermission().ordinal(),
        aclEntryInt);
    // 如果是用户/组类型，编码名称序列号
    SerialNumberManager snm = getSerialNumberManager(aclEntry.getType());
    if (snm != null) {
      int nid = snm.getSerialNumber(aclEntry.getName());
      aclEntryInt = NAME.BITS.combine(nid, aclEntryInt);
    }
    return (int) aclEntryInt;
  }

  /**
   * 将编码后的整数解码为AclEntry对象，使用默认字符串表
   * @param aclEntry 编码后的ACL条目整数
   * @return 解码后的AclEntry对象
   */
  static AclEntry toAclEntry(int aclEntry) {
    return toAclEntry(aclEntry, null);
  }

  /**
   * 将编码后的整数解码为AclEntry对象，使用指定字符串表
   * @param aclEntry 编码后的ACL条目整数
   * @param stringTable 序列号对应的字符串表，用于名称解析
   * @return 解码后的AclEntry对象
   */
  static AclEntry toAclEntry(int aclEntry,
                             SerialNumberManager.StringTable stringTable) {
    // 使用Builder组装各个解码出的字段构建AclEntry对象
    return new AclEntry.Builder()
        .setScope(getScope(aclEntry))
        .setType(getType(aclEntry))
        .setPermission(getPermission(aclEntry))
        .setName(getName(aclEntry, stringTable))
        .build();
  }

  /**
   * 将AclEntry列表批量编码为整数数组
   * @param aclEntries 待编码的AclEntry列表
   * @return 编码后的整数数组，每个元素对应一个ACL条目
   */
  public static int[] toInt(List<AclEntry> aclEntries) {
    int[] entries = new int[aclEntries.size()];
    // 遍历逐个编码
    for (int i = 0; i < entries.length; i++) {
      entries[i] = toInt(aclEntries.get(i));
    }
    return entries;
  }

  /**
   * 根据ACL条目类型获取对应用户/组的序列号管理器
   * @param type ACL条目类型
   * @return 对应类型的序列号管理器，其他类型返回null
   */
  private static SerialNumberManager getSerialNumberManager(AclEntryType type) {
    switch (type) {
      case USER:
        return SerialNumberManager.USER;
      case GROUP:
        return SerialNumberManager.GROUP;
      default:
        return null;
    }
  }

  @Override
  public int getLength() {
    return BITS.getLength();
  }
}