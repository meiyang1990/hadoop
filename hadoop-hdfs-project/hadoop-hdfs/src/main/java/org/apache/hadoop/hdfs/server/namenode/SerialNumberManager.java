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

import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields.PermissionStatusFormat;
import org.apache.hadoop.hdfs.util.LongBitFormat;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 字符串表序列号管理器，为不同类型的字符串（用户、用户组、扩展属性等）分配全局唯一序列号，
 * 通过将字符串映射为整数序列号节省NameNode内存，同时支持持久化快照和恢复。
 * 采用枚举类型实现不同类型字符串表的独立管理。
 */
/** Manage name-to-serial-number maps for various string tables. */
public enum SerialNumberManager {
  /** 全局类型序列号管理 */
  GLOBAL(),
  /** 用户名称类型序列号管理 */
  USER(PermissionStatusFormat.USER, AclEntryStatusFormat.NAME),
  /** 用户组名称类型序列号管理 */
  GROUP(PermissionStatusFormat.GROUP, AclEntryStatusFormat.NAME),
  /** XAttr扩展属性名称类型序列号管理 */
  XATTR(XAttrFormat.NAME);

  /** 缓存所有枚举实例，避免调用values()重复生成数组 */
  private static final SerialNumberManager[] values = values();
  /** 区分不同类型序列号所需的最大bit位数 */
  private static final int maxEntryBits;
  /** 最大可容纳的类型数量，基于maxEntryBits计算 */
  private static final int maxEntryNumber;
  /** 序列号域占用的bit位数，总长度减去类型标识位 */
  private static final int maskBits;

  /** 当前类型的字符串到序列号的映射表 */
  private SerialNumberMap<String> serialMap;
  /** 当前类型序列号占用的bit长度 */
  private int bitLength = Integer.SIZE;

  static {
    // 计算区分所有类型所需的最小leading zero位数，得到所需bit长度
    maxEntryBits = Integer.numberOfLeadingZeros(values.length);
    // 计算最大类型编号
    maxEntryNumber = (1 << maxEntryBits) - 1;
    // 计算序列号域bit长度
    maskBits = Integer.SIZE - maxEntryBits;
    for (SerialNumberManager snm : values) {
      // 为每个类型更新bit长度，初始化映射表
      snm.updateLength(maxEntryBits);
      snm.serialMap = new SerialNumberMap<String>(snm);
      FSDirectory.LOG.info(snm + " serial map: bits=" + snm.getLength() +
          " maxEntries=" + snm.serialMap.getMax());
    }
  }

  /**
   * 构造指定类型的序列号管理器，根据注册的位域更新bit长度
   * @param elements 该类型使用的LongBitFormat枚举，从中获取允许的最大bit长度
   */
  SerialNumberManager(LongBitFormat.Enum... elements) {
    // 计算该类型允许的最小bit长度（取所有注册位域的最小值）
    for (LongBitFormat.Enum element : elements) {
      updateLength(element.getLength());
    }
  }

  /**
   * 获取当前类型序列号占用的bit长度
   * @return bit长度
   */
  int getLength() {
    return bitLength;
  }

  /**
   * 更新当前类型的bit长度，取最小值保证不超出允许范围
   * @param maxLength 新的最大允许bit长度
   */
  private void updateLength(int maxLength) {
    bitLength = Math.min(bitLength, maxLength);
  }

  /**
   * 根据字符串获取对应序列号，如果不存在则分配新序列号
   * @param str 待查询的字符串
   * @return 对应序列号
   */
  public int getSerialNumber(String str) {
    return serialMap.get(str);
  }

  /**
   * 根据序列号获取对应字符串
   * @param id 序列号
   * @return 对应字符串
   */
  public String getString(int id) {
    return serialMap.get(id);
  }

  /**
   * 根据序列号从指定字符串表获取对应字符串，若字符串表为空则从本地映射表获取
   * @param id 序列号
   * @param stringTable 外部加载的字符串表（用于持久化恢复场景）
   * @return 对应字符串
   */
  public String getString(int id, StringTable stringTable) {
    return (stringTable != null)
        ? stringTable.get(this, id) : getString(id);
  }

  /**
   * 计算当前类型在复合序列号中的类型掩码
   * @param bits 序列号域bit长度
   * @return 类型掩码，将类型编号左移到高位区域
   */
  private int getMask(int bits) {
    return ordinal() << (Integer.SIZE - bits);
  }

  /**
   * 获取全局的序列号域bit长度
   * @return 序列号域bit长度
   */
  private static int getMaskBits() {
    return maskBits;
  }

  /**
   * 获取当前类型映射表的条目数量
   * @return 条目数量
   */
  private int size() {
    return serialMap.size();
  }

  /**
   * 获取当前类型所有条目迭代器
   * @return 条目集合可迭代对象
   */
  private Iterable<Entry<Integer, String>> entrySet() {
    return serialMap.entrySet();
  }

  /**
   * 获取当前所有类型字符串的完整快照，用于持久化保存NameNode元数据
   * @return 包含所有条目复合结构的字符串表
   */
  // returns snapshot of current values for a save.
  public static StringTable getStringTable() {
    // 计算总容量，初始化哈希表
    int size = 0;
    for (final SerialNumberManager snm : values) {
      size += snm.size();
    }
    int tableMaskBits = getMaskBits();
    StringTable map = new StringTable(size, tableMaskBits);
    // 合并所有类型的条目到统一表，将类型掩码合并到序列号生成复合id
    for (final SerialNumberManager snm : values) {
      final int mask = snm.getMask(tableMaskBits);
      for (Entry<Integer, String> entry : snm.entrySet()) {
        map.put(entry.getKey() | mask, entry.getValue());
      }
    }
    return map;
  }

  /**
   * 创建空字符串表，用于从持久化文件加载元数据
   * @param size 预估容量
   * @param bits 加载得到的序列号域bit长度
   * @return 空字符串表实例
   */
  // returns an empty table for load.
  public static StringTable newStringTable(int size, int bits) {
    if (bits > maskBits) {
      throw new IllegalArgumentException(
        "String table bits " + bits + " > " + maskBits);
    }
    return new StringTable(size, bits);
  }

  /**
   * 统一字符串表，用于保存所有类型序列号到字符串的映射，支持持久化保存和加载，
   * 通过高位bit区分不同类型的序列号，将所有条目存储在一个哈希表中。
   */
  public static class StringTable implements Iterable<Entry<Integer, String>> {
    /** 本字符串表使用的序列号域bit长度 */
    private final int tableMaskBits;
    /** 复合id到字符串的映射表 */
    private final Map<Integer,String> map;

    /**
     * 构造字符串表实例
     * @param size 预估容量
     * @param loadingMaskBits 序列号域bit长度
     */
    private StringTable(int size, int loadingMaskBits) {
      this.tableMaskBits = loadingMaskBits;
      this.map = new HashMap<>(size);
    }

    /**
     * 根据类型和序列号获取对应字符串
     * @param snm 序列号管理器类型
     * @param id 序列号
     * @return 对应字符串
     */
    private String get(SerialNumberManager snm, int id) {
      if (tableMaskBits != 0) {
        // 校验序列号不超出范围
        if (id > maxEntryNumber) {
          throw new IllegalStateException(
              "serial id " + id + " > " + maxEntryNumber);
        }
        // 合并类型掩码生成复合id
        id |= snm.getMask(tableMaskBits);
      }
      return map.get(id);
    }

    /**
     * 添加复合id到字符串的映射
     * @param id 复合id
     * @param str 字符串
     */
    public void put(int id, String str) {
      map.put(id, str);
    }

    /**
     * 获取所有条目的迭代器
     * @return 条目迭代器
     */
    public Iterator<Entry<Integer, String>> iterator() {
      return map.entrySet().iterator();
    }

    /**
     * 获取字符串表大小
     * @return 条目数量
     */
    public int size() {
      return map.size();
    }

    /**
     * 获取本字符串表使用的序列号域bit长度
     * @return 序列号域bit长度
     */
    public int getMaskBits() {
      return tableMaskBits;
    }
  }
}