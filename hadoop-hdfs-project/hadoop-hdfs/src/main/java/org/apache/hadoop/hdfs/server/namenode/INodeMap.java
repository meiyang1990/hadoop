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

import java.util.Iterator;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;

import org.apache.hadoop.util.Preconditions;

/**
 * 文件级注释：HDFS NameNode中存储所有INode节点的全局映射表，维护INode ID到INode对象的映射关系
 * Storing all the {@link INode}s and maintaining the mapping between INode ID
 * and INode.  
 */
public class INodeMap {
  
  /**
   * 创建并初始化INodeMap实例，将根目录INode加入映射表
   * @param rootDir 根目录INode对象
   * @return 初始化完成的INodeMap实例
   */
  static INodeMap newInstance(INodeDirectory rootDir) {
    // 根据总内存的1%计算映射表初始容量
    int capacity = LightWeightGSet.computeCapacity(1, "INodeMap");
    // 创建基于LightWeightGSet的存储结构
    GSet<INode, INodeWithAdditionalFields> map =
        new LightWeightGSet<>(capacity);
    // 将根目录INode加入映射表
    map.put(rootDir);
    return new INodeMap(map);
  }

  /** 存储结构，所有操作由外部锁保证线程安全 */
  /** Synchronized by external lock. */
  private final GSet<INode, INodeWithAdditionalFields> map;
  
  /**
   * 获取INode映射表的迭代器
   * @return 所有带额外字段的INode对象迭代器
   */
  public Iterator<INodeWithAdditionalFields> getMapIterator() {
    return map.iterator();
  }

  /**
   * 私有构造方法，使用指定GSet实例初始化INodeMap
   * @param map 底层存储使用的GSet实例
   */
  private INodeMap(GSet<INode, INodeWithAdditionalFields> map) {
    Preconditions.checkArgument(map != null);
    this.map = map;
  }
  
  /**
   * Add an {@link INode} into the {@link INode} map. Replace the old value if 
   * necessary. 
   * @param inode The {@link INode} to be added to the map.
   */
  public final void put(INode inode) {
    if (inode instanceof INodeWithAdditionalFields) {
      map.put((INodeWithAdditionalFields)inode);
    }
  }
  
  /**
   * Remove a {@link INode} from the map.
   * @param inode The {@link INode} to be removed.
   */
  public final void remove(INode inode) {
    map.remove(inode);
  }
  
  /**
   * @return The size of the map.
   */
  public int size() {
    return map.size();
  }
  
  /**
   * Get the {@link INode} with the given id from the map.
   * @param id ID of the {@link INode}.
   * @return The {@link INode} in the map with the given id. Return null if no 
   *         such {@link INode} in the map.
   */
  public INode get(long id) {
    // 根据给定ID构造临时INode对象，用于GSet查询匹配
    INode inode = new INodeWithAdditionalFields(id, null, new PermissionStatus(
        "", "", new FsPermission((short) 0)), 0, 0) {
      
      @Override
      void recordModification(int latestSnapshotId) {
      }
      
      @Override
      public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
        // Nothing to do
      }

      @Override
      public QuotaCounts computeQuotaUsage(
          BlockStoragePolicySuite bsps, byte blockStoragePolicyId,
          boolean useCache, int lastSnapshotId) {
        return null;
      }

      @Override
      public ContentSummaryComputationContext computeContentSummary(
          int snapshotId, ContentSummaryComputationContext summary) {
        return null;
      }
      
      @Override
      public void cleanSubtree(
          ReclaimContext reclaimContext, int snapshotId, int priorSnapshotId) {
      }

      @Override
      public byte getStoragePolicyID(){
        return HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      }

      @Override
      public byte getLocalStoragePolicyID() {
        return HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      }
    };
      
    return map.get(inode);
  }
  
  /**
   * Clear the {@link #map}
   */
  public void clear() {
    map.clear();
  }
}