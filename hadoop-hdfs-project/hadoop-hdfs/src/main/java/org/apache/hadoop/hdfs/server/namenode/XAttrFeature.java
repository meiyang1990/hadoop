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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.XAttrHelper;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;

/**
 * 文件扩展属性（XAttr）特性，作为INode的功能特性实现，
 * 用于存储和管理HDFS文件/目录的扩展属性，根据属性值大小采用不同存储优化策略。
 */
@InterfaceAudience.Private
public class XAttrFeature implements INode.Feature {
  /** 打包存储阈值：属性值长度小于等于该值的会被压缩打包存储 */
  static final int PACK_THRESHOLD = 1024;

  /** 小尺寸XAttr的压缩打包字节存储 */
  private byte[] attrs;

  /**
   * 大尺寸XAttr的列表存储，值长度超过阈值的属性存储在这里。
   * 通常XAttr值都很小，所以该列表一般为null。
   */
  private ImmutableList<XAttr> xAttrs;

  /**
   * 构造XAttr特性对象，根据属性值大小拆分存储到不同区域
   * @param xAttrs 全部扩展属性列表
   */
  public XAttrFeature(List<XAttr> xAttrs) {
    if (xAttrs != null && !xAttrs.isEmpty()) {
      // 存储可打包的小尺寸属性
      List<XAttr> toPack = new ArrayList<XAttr>();
      // 存储大尺寸属性的Builder
      ImmutableList.Builder<XAttr> b = null;
      for (XAttr attr : xAttrs) {
        // 按阈值拆分属性
        if (attr.getValue() == null ||
            attr.getValue().length <= PACK_THRESHOLD) {
          toPack.add(attr);
        } else {
          if (b == null) {
            b = ImmutableList.builder();
          }
          b.add(attr);
        }
      }
      // 将小属性打包为字节数组
      this.attrs = XAttrFormat.toBytes(toPack);
      if (b != null) {
        this.xAttrs = b.build();
      }
    }
  }

  /**
   * 获取所有扩展属性，合并小尺寸打包存储和大尺寸列表存储的结果
   * @return 全部扩展属性列表
   */
  public List<XAttr> getXAttrs() {
    if (xAttrs == null) {
      // 只有打包存储的小属性，直接反序列化返回
      return XAttrFormat.toXAttrs(attrs);
    } else {
      if (attrs == null) {
        // 只有列表存储的大属性，直接返回
        return xAttrs;
      } else {
        // 同时存在两种存储，合并结果返回
        List<XAttr> result = new ArrayList<>();
        result.addAll(XAttrFormat.toXAttrs(attrs));
        result.addAll(xAttrs);
        return result;
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    // 比较全部扩展属性是否相等
    return getXAttrs().equals(((XAttrFeature) o).getXAttrs());
  }

  @Override
  public int hashCode() {
    // 基于全部扩展属性计算哈希值
    return Arrays.hashCode(getXAttrs().toArray());
  }

  /**
   * 根据带前缀的名称查找指定扩展属性
   * @param prefixedName 带命名空间前缀的XAttr名称
   * @return 匹配到的XAttr，未找到返回null
   */
  public XAttr getXAttr(String prefixedName) {
    // 先在打包存储的小属性中查找
    XAttr attr = XAttrFormat.getXAttr(attrs, prefixedName);
    if (attr == null && xAttrs != null) {
      // 小属性中未找到，再在大属性列表中查找
      XAttr toFind = XAttrHelper.buildXAttr(prefixedName);
      for (XAttr a : xAttrs) {
        // 忽略值比较，只匹配名称和前缀
        if (a.equalsIgnoreValue(toFind)) {
          attr = a;
          break;
        }
      }
    }
    return attr;
  }
}