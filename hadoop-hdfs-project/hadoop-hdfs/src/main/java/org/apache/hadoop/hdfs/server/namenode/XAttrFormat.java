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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.XAttrHelper;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;
import org.apache.hadoop.hdfs.util.LongBitFormat;

/**
 * 扩展属性(XAttr)的二进制格式编解码工具类，用于将XAttr列表打包/解包为字节数组。<br>
 * 该格式同时用于内存和磁盘存储，格式变更不兼容旧版本。
 * 基于位域格式存储XAttr元数据，实现紧凑存储，节省NameNode内存和磁盘空间。
 */
public enum XAttrFormat implements LongBitFormat.Enum {
  /** 保留位域，未使用 */
  RESERVED(null, 5),
  /** 命名空间扩展位域，用于存储命名空间编号高比特位 */
  NS_EXT(RESERVED.BITS, 1),
  /** XAttr名称序列号位域，存储XAttr名称在序列化表中的编号 */
  NAME(NS_EXT.BITS, 24),
  /** 命名空间编号低比特位域 */
  NS(NAME.BITS, 2);

  /** 命名空间扩展位移，用于高低位拼接 */
  private static final int NS_EXT_SHIFT = NS.BITS.getLength();
  /** 命名空间低比特位掩码 */
  private static final int NS_MASK = (1 << NS_EXT_SHIFT) - 1;

  /** XAttr值最大长度限制 */
  private static final int XATTR_VALUE_LEN_MAX = 1 << 16;
  /** 缓存XAttr命名空间枚举值数组，用于快速查询 */
  private static final XAttr.NameSpace[] XATTR_NAMESPACE_VALUES =
      XAttr.NameSpace.values();

  /** 当前位域的格式定义 */
  private final LongBitFormat BITS;

  /**
   * 构造XAttr格式位域定义
   * @param previous 前一个相邻位域，用于计算当前位域偏移
   * @param length 当前位域比特长度
   */
  XAttrFormat(LongBitFormat previous, int length) {
    BITS = new LongBitFormat(name(), previous, length, 0);
  }

  @Override
  public int getLength() {
    return BITS.getLength();
  }

  /**
   * 从元数据记录中解析XAttr命名空间
   * @param record 存储XAttr元数据的整型记录
   * @return 解析出的XAttr命名空间
   */
  static XAttr.NameSpace getNamespace(int record) {
    long nid = NS.BITS.retrieve(record);
    nid |= NS_EXT.BITS.retrieve(record) << NS_EXT_SHIFT;
    return XATTR_NAMESPACE_VALUES[(int) nid];
  }

  /**
   * 从元数据记录中解析XAttr名称
   * @param record 存储XAttr元数据的整型记录
   * @return 解析出的XAttr名称字符串
   */
  public static String getName(int record) {
    int nid = (int)NAME.BITS.retrieve(record);
    return SerialNumberManager.XATTR.getString(nid);
  }

  /**
   * 将单个XAttr的元数据编码为整型记录
   * @param a 待编码的XAttr对象
   * @return 编码后的整型元数据记录
   */
  public static int toInt(XAttr a) {
    int nid = SerialNumberManager.XATTR.getSerialNumber(a.getName());
    int nsOrd = a.getNameSpace().ordinal();
    long value = NS.BITS.combine(nsOrd & NS_MASK, 0L);
    value = NS_EXT.BITS.combine(nsOrd >>> NS_EXT_SHIFT, value);
    value = NAME.BITS.combine(nid, value);
    return (int)value;
  }

  /**
   * 将元数据记录解析为完整XAttr对象
   * @param record 存储XAttr元数据的整型记录
   * @param value XAttr值的字节数组
   * @param stringTable 字符串表，用于名称反序列化
   * @return 解析完成的XAttr对象
   */
  static XAttr toXAttr(int record, byte[] value,
                       SerialNumberManager.StringTable stringTable) {
    int nid = (int)NAME.BITS.retrieve(record);
    String name = SerialNumberManager.XATTR.getString(nid, stringTable);
    return new XAttr.Builder()
        .setNameSpace(getNamespace(record))
        .setName(name)
        .setValue(value)
        .build();
  }

  /**
   * 将打包的字节数组解包为XAttr列表
   * 
   * @param attrs 打包后的XAttr字节数组
   * @return 解包完成的XAttr列表
   */
  static List<XAttr> toXAttrs(byte[] attrs) {
    List<XAttr> xAttrs = new ArrayList<>();
    if (attrs == null || attrs.length == 0) {
      return xAttrs;
    }
    for (int i = 0; i < attrs.length;) {
      XAttr.Builder builder = new XAttr.Builder();
      // 按大端字节序读取4字节元数据记录
      int v = Ints.fromBytes(attrs[i], attrs[i + 1],
          attrs[i + 2], attrs[i + 3]);
      i += 4;
      builder.setNameSpace(XAttrFormat.getNamespace(v));
      builder.setName(XAttrFormat.getName(v));
      // 按大端字节序读取2字节XAttr值长度
      int vlen = ((0xff & attrs[i]) << 8) | (0xff & attrs[i + 1]);
      i += 2;
      if (vlen > 0) {
        // 读取XAttr值字节数组
        byte[] value = new byte[vlen];
        System.arraycopy(attrs, i, value, 0, vlen);
        builder.setValue(value);
        i += vlen;
      }
      xAttrs.add(builder.build());
    }
    return xAttrs;
  }

  /**
   * 从打包字节数组中查询指定名称的XAttr，无需解包所有XAttr
   * 
   * @param attrs 打包后的XAttr字节数组
   * @param prefixedName 带命名空间前缀的XAttr完整名称
   * @return 查询到的XAttr对象，不存在则返回null
   */
  static XAttr getXAttr(byte[] attrs, String prefixedName) {
    if (prefixedName == null || attrs == null) {
      return null;
    }

    // 构建目标XAttr模板，用于匹配
    XAttr xAttr = XAttrHelper.buildXAttr(prefixedName);
    for (int i = 0; i < attrs.length;) {
      // 按大端字节序读取4字节元数据记录
      int v = Ints.fromBytes(attrs[i], attrs[i + 1],
          attrs[i + 2], attrs[i + 3]);
      i += 4;
      XAttr.NameSpace namespace = XAttrFormat.getNamespace(v);
      String name = XAttrFormat.getName(v);
      // 按大端字节序读取2字节XAttr值长度
      int vlen = ((0xff & attrs[i]) << 8) | (0xff & attrs[i + 1]);
      i += 2;
      // 匹配命名空间和名称，匹配成功则返回结果
      if (xAttr.getNameSpace() == namespace &&
          xAttr.getName().equals(name)) {
        if (vlen > 0) {
          byte[] value = new byte[vlen];
          System.arraycopy(attrs, i, value, 0, vlen);
          return new XAttr.Builder().setNameSpace(namespace).
              setName(name).setValue(value).build();
        }
        return xAttr;
      }
      // 不匹配则跳过当前XAttr值，继续遍历
      i += vlen;
    }
    return null;
  }

  /**
   * 将XAttr列表打包为字节数组，用于持久化存储
   * 
   * @param xAttrs 待打包的XAttr列表
   * @return 打包完成的字节数组，列表为空则返回null
   */
  static byte[] toBytes(List<XAttr> xAttrs) {
    if (xAttrs == null || xAttrs.isEmpty()) {
      return null;
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      for (XAttr a : xAttrs) {
        // 按大端字节序写入4字节元数据记录
        int v = XAttrFormat.toInt(a);
        out.write(Ints.toByteArray(v));
        // 计算XAttr值长度，空值长度为0
        int vlen = a.getValue() == null ? 0 : a.getValue().length;
        // 检查值长度不超过最大限制
        Preconditions.checkArgument(vlen < XATTR_VALUE_LEN_MAX,
            "The length of xAttr values is too long.");
        // 按大端字节序写入2字节长度
        out.write((byte)(vlen >> 8));
        out.write((byte)(vlen));
        if (vlen > 0) {
          // 写入XAttr值字节数组
          out.write(a.getValue());
        }
      }
    } catch (IOException e) {
      // ByteArrayOutputStream不会抛出IO异常，这里仅捕获声明
    }
    return out.toByteArray();
  }
}