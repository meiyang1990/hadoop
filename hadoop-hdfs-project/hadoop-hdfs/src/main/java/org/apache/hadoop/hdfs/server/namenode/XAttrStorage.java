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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;

/**
 * 文件扩展属性(XAttr)存储工具类，负责对inode节点的扩展属性进行读写操作
 * 为HDFS提供扩展属性的存储管理能力，支持安全、命名空间等自定义元数据存储
 */
@InterfaceAudience.Private
public class XAttrStorage {

  /**
   * 根据带前缀的名称读取inode节点指定的扩展属性
   * 支持快照场景，可读取指定快照版本的扩展属性
   *
   * @param inode 目标inode节点
   * @param snapshotId 请求路径对应的快照ID
   * @param prefixedName 带命名空间前缀的扩展属性名称
   * @return 匹配的扩展属性，不存在则返回null
   */
  public static XAttr readINodeXAttrByPrefixedName(INode inode, int snapshotId,
                                                   String prefixedName) {
    XAttrFeature f = inode.getXAttrFeature(snapshotId);
    return f == null ? null : f.getXAttr(prefixedName);
  }

  /**
   * 读取inode节点所有已存在的扩展属性列表
   * <p>
   * 调用方必须持有FSDirectory的读锁
   *
   * @param inodeAttr inode节点属性对象
   * @return 该节点所有扩展属性列表，无扩展属性则返回空列表
   */
  public static List<XAttr> readINodeXAttrs(INodeAttributes inodeAttr) {
    XAttrFeature f = inodeAttr.getXAttrFeature();
    return f == null ? new ArrayList<XAttr>(0) : f.getXAttrs();
  }
  
  /**
   * 更新inode节点的扩展属性列表
   * <p>
   * 调用方必须持有FSDirectory的写锁，支持快照场景下的增量修改
   * 
   * @param inode 需要更新的inode节点
   * @param xAttrs 更新后的扩展属性列表，空列表表示清除所有扩展属性
   * @param snapshotId inode最新快照的ID
   * @throws QuotaExceededException 当存储扩展属性超出配额时抛出异常
   */
  public static void updateINodeXAttrs(INode inode, 
      List<XAttr> xAttrs, int snapshotId) throws QuotaExceededException {
    if (inode.getXAttrFeature() != null) {
      inode.removeXAttrFeature(snapshotId);
    }
    if (xAttrs == null || xAttrs.isEmpty()) {
      return;
    }
    inode.addXAttrFeature(new XAttrFeature(xAttrs), snapshotId);
  }
}