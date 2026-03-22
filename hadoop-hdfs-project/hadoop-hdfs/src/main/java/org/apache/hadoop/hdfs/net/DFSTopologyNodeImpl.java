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
package org.apache.hadoop.hdfs.net;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.net.InnerNode;
import org.apache.hadoop.net.InnerNodeImpl;
import org.apache.hadoop.net.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;

/**
 * HDFS特定的网络拓扑内层节点实现。在通用网络拓扑节点基础上增加了子树存储类型统计信息，
 * 该信息用于数据块放置时选择合适的拓扑子树，满足数据放置的存储类型要求。
 */
public class DFSTopologyNodeImpl extends InnerNodeImpl {

  public static final Logger LOG =
      LoggerFactory.getLogger(DFSTopologyNodeImpl.class);

  static final InnerNodeImpl.Factory FACTORY
      = new DFSTopologyNodeImpl.Factory();

  /**
   * DFSTopologyNodeImpl节点工厂，负责创建HDFS特定拓扑内层节点实例
   */
  static final class Factory extends InnerNodeImpl.Factory {
    private Factory() {}

    @Override
    public InnerNodeImpl newInnerNode(String path) {
      return new DFSTopologyNodeImpl(path);
    }
  }

  /**
   * 当前节点下所有子节点的存储类型统计。
   * 键为子节点ID，值为该子节点下各存储类型的数据节点数量统计。
   * 存储每个子节点独立的统计信息保证更新一致性，当子节点存储信息变化时可以快速更新当前节点统计。
   */
  private final HashMap
      <String, EnumMap<StorageType, Integer>> childrenStorageInfo;

  /**
   * 当前节点整个子树的各存储类型总数据节点数量统计。
   * 缓存该统计信息避免每次查询都遍历所有子节点，优化查询性能。
   */
  private final EnumMap<StorageType, Integer> storageTypeCounts;

  DFSTopologyNodeImpl(String path) {
    super(path);
    childrenStorageInfo = new HashMap<>();
    storageTypeCounts = new EnumMap<>(StorageType.class);
  }

  DFSTopologyNodeImpl(
      String name, String location, InnerNode parent, int level) {
    super(name, location, parent, level);
    childrenStorageInfo = new HashMap<>();
    storageTypeCounts = new EnumMap<>(StorageType.class);
  }

  /**
   * 获取当前子树中指定存储类型的可用数据节点总数
   * @param type 存储类型
   * @return 指定存储类型的数据节点总数，不存在则返回0
   */
  public int getSubtreeStorageCount(StorageType type) {
    if (storageTypeCounts.containsKey(type)) {
      return storageTypeCounts.get(type);
    } else {
      return 0;
    }
  }

  /**
   * 增加指定存储类型的总计数
   * @param type 存储类型
   */
  private void incStorageTypeCount(StorageType type) {
    // 调用方已经持有锁，无需额外加锁
    if (storageTypeCounts.containsKey(type)) {
      storageTypeCounts.put(type, storageTypeCounts.get(type)+1);
    } else {
      storageTypeCounts.put(type, 1);
    }
  }

  /**
   * 减少指定存储类型的总计数
   * @param type 存储类型
   */
  private void decStorageTypeCount(StorageType type) {
    // 调用方已经持有锁，无需额外加锁
    int current = storageTypeCounts.get(type);
    current -= 1;
    if (current == 0) {
      storageTypeCounts.remove(type);
    } else {
      storageTypeCounts.put(type, current);
    }
  }

  /**
   * 更新已存在数据节点的存储类型信息。当数据节点重启后存储类型发生变化时调用该方法，
   * 更新当前节点及所有祖先节点的存储类型统计。
   * @param dnDescriptor 重新添加的数据节点描述符，可能包含新的存储类型信息
   */
  private void updateExistingDatanode(DatanodeDescriptor dnDescriptor) {
    if (childrenStorageInfo.containsKey(dnDescriptor.getName())) {
      // 先检查存储类型集合是否发生变化
      boolean same = dnDescriptor.getStorageTypes().size()
          == childrenStorageInfo.get(dnDescriptor.getName()).keySet().size();
      for (StorageType type :
          childrenStorageInfo.get(dnDescriptor.getName()).keySet()) {
        same = same && dnDescriptor.hasStorageType(type);
      }
      if (same) {
        // 存储类型无变化，直接返回
        return;
      }
      // 存储类型发生变化，需要逐层更新统计
      DFSTopologyNodeImpl parent = (DFSTopologyNodeImpl)getParent();
      // 移除数据节点已不再持有的存储类型统计
      for (StorageType type :
          childrenStorageInfo.get(dnDescriptor.getName()).keySet()) {
        if (!dnDescriptor.hasStorageType(type)) {
          childrenStorageInfo.get(dnDescriptor.getName()).remove(type);
          decStorageTypeCount(type);
          if (parent != null) {
            parent.childRemoveStorage(getName(), type);
          }
        }
      }
      // 新增数据节点新增持有的存储类型统计
      for (StorageType type : dnDescriptor.getStorageTypes()) {
        if (!childrenStorageInfo.get(dnDescriptor.getName())
            .containsKey(type)) {
          childrenStorageInfo.get(dnDescriptor.getName()).put(type, 1);
          incStorageTypeCount(type);
          if (parent != null) {
            parent.childAddStorage(getName(), type);
          }
        }
      }
    }
  }

  @Override
  public boolean add(Node n) {
    LOG.debug("adding node {}", n.getName());
    if (!isAncestor(n)) {
      throw new IllegalArgumentException(n.getName()
          + ", which is located at " + n.getNetworkLocation()
          + ", is not a descendant of " + getPath(this));
    }
    // HDFS拓扑的叶子节点必须是DatanodeDescriptor
    if (!(n instanceof DatanodeDescriptor)) {
      throw new IllegalArgumentException("Unexpected node type "
          + n.getClass().getName());
    }
    DatanodeDescriptor dnDescriptor = (DatanodeDescriptor) n;
    if (isParent(n)) {
      // 当前节点是n的直接父节点，直接添加n作为子节点
      n.setParent(this);
      n.setLevel(this.level + 1);
      Node prev = childrenMap.put(n.getName(), n);
      if (prev != null) {
        // 节点已存在，更新存储信息并返回
        for(int i=0; i<children.size(); i++) {
          if (children.get(i).getName().equals(n.getName())) {
            children.set(i, n);
            updateExistingDatanode(dnDescriptor);
            return false;
          }
        }
      }
      // 添加新节点，更新存储统计
      children.add(n);
      numOfLeaves++;
      if (!childrenStorageInfo.containsKey(dnDescriptor.getName())) {
        childrenStorageInfo.put(
            dnDescriptor.getName(), new EnumMap<>(StorageType.class));
      }
      for (StorageType st : dnDescriptor.getStorageTypes()) {
        childrenStorageInfo.get(dnDescriptor.getName()).put(st, 1);
        incStorageTypeCount(st);
      }
      return true;
    } else {
      // 找到下一层祖先节点，递归添加
      String parentName = getNextAncestorName(n);
      InnerNode parentNode = (InnerNode)childrenMap.get(parentName);
      if (parentNode == null) {
        // 下一层节点不存在，创建新的拓扑节点
        parentNode = createParentNode(parentName);
        children.add(parentNode);
        childrenMap.put(parentNode.getName(), parentNode);
      }
      // 递归添加节点到子树
      if (parentNode.add(n)) {
        numOfLeaves++;
        // 更新当前节点存储统计
        if (!childrenStorageInfo.containsKey(parentNode.getName())) {
          childrenStorageInfo.put(
              parentNode.getName(), new EnumMap<>(StorageType.class));
          for (StorageType st : dnDescriptor.getStorageTypes()) {
            childrenStorageInfo.get(parentNode.getName()).put(st, 1);
          }
        } else {
          EnumMap<StorageType, Integer> currentCount =
              childrenStorageInfo.get(parentNode.getName());
          for (StorageType st : dnDescriptor.getStorageTypes()) {
            if (currentCount.containsKey(st)) {
              currentCount.put(st, currentCount.get(st) + 1);
            } else {
              currentCount.put(st, 1);
            }
          }
        }
        for (StorageType st : dnDescriptor.getStorageTypes()) {
          incStorageTypeCount(st);
        }
        return true;
      } else {
        return false;
      }
    }
  }

  @VisibleForTesting
  HashMap <String, EnumMap<StorageType, Integer>> getChildrenStorageInfo() {
    return childrenStorageInfo;
  }


  /**
   * 创建下一层拓扑父节点
   * @param parentName 父节点名称
   * @return 新建的DFSTopologyNodeImpl实例
   */
  private DFSTopologyNodeImpl createParentNode(String parentName) {
    return new DFSTopologyNodeImpl(
        parentName, getPath(this), this, this.getLevel() + 1);
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean remove(Node n) {
    LOG.debug("removing node {}", n.getName());
    if (!isAncestor(n)) {
      throw new IllegalArgumentException(n.getName()
          + ", which is located at " + n.getNetworkLocation()
          + ", is not a descendant of " + getPath(this));
    }
    // HDFS拓扑的叶子节点必须是DatanodeDescriptor
    if (!(n instanceof DatanodeDescriptor)) {
      throw new IllegalArgumentException("Unexpected node type "
          + n.getClass().getName());
    }
    DatanodeDescriptor dnDescriptor = (DatanodeDescriptor) n;
    if (isParent(n)) {
      // 当前节点是n的直接父节点，直接移除n
      if (childrenMap.containsKey(n.getName())) {
        for (int i=0; i<children.size(); i++) {
          if (children.get(i).getName().equals(n.getName())) {
            children.remove(i);
            childrenMap.remove(n.getName());
            childrenStorageInfo.remove(dnDescriptor.getName());
            // 更新存储统计
            for (StorageType st : dnDescriptor.getStorageTypes()) {
              decStorageTypeCount(st);
            }
            numOfLeaves--;
            n.setParent(null);
            return true;
          }
        }
      }
      return false;
    } else {
      // 找到下一层祖先节点，递归移除
      String parentName = getNextAncestorName(n);
      DFSTopologyNodeImpl parentNode =
          (DFSTopologyNodeImpl)childrenMap.get(parentName);
      if (parentNode == null) {
        return false;
      }
      boolean isRemoved = parentNode.remove(n);
      if (isRemoved) {
        // 更新当前节点存储统计
        EnumMap<StorageType, Integer> currentCount =
            childrenStorageInfo.get(parentNode.getName());
        EnumSet<StorageType> toRemove = EnumSet.noneOf(StorageType.class);
        for (StorageType st : dnDescriptor.getStorageTypes()) {
          int newCount = currentCount.get(st) - 1;
          if (newCount == 0) {
            toRemove.add(st);
          }
          currentCount.put(st, newCount);
        }
        for (StorageType st : toRemove) {
          currentCount.remove(st);
        }
        for (StorageType st : dnDescriptor.getStorageTypes()) {
          decStorageTypeCount(st);
        }
        // 如果子节点已经没有任何孩子，移除该子节点
        if (parentNode.getNumOfChildren() == 0) {
          for(int i=0; i < children.size(); i++) {
            if (children.get(i).getName().equals(parentName)) {
              children.remove(i);
              childrenMap.remove(parentName);
              childrenStorageInfo.remove(parentNode.getName());
              break;
            }
          }
        }
        numOfLeaves--;
      }
      return isRemoved;
    }
  }

  /**
   * 子节点新增存储类型时，递归更新当前节点及所有祖先节点的存储统计。
   * 该方法由子节点调用，用于向上传播存储类型变化。
   * @param childName 新增存储类型的子节点名称
   * @param type 新增的存储类型
   */
  public synchronized void childAddStorage(
      String childName, StorageType type) {
    LOG.debug("child add storage: {}:{}", childName, type);
    // 子节点必须已经存在于childrenStorageInfo中
    Preconditions.checkArgument(childrenStorageInfo.containsKey(childName));
    EnumMap<StorageType, Integer> typeCount =
        childrenStorageInfo.get(childName);
    // 更新子节点存储计数
    if (typeCount.containsKey(type)) {
      typeCount.put(type, typeCount.get(type) + 1);
    } else {
      typeCount.put(type, 1);
    }
    // 更新当前节点总计数
    if (storageTypeCounts.containsKey(type)) {
      storageTypeCounts.put(type, storageTypeCounts.get(type) + 1);
    } else {
      storageTypeCounts.put(type, 1);
    }
    // 递归向上更新祖先节点
    if (getParent() != null) {
      ((DFSTopologyNodeImpl)getParent()).childAddStorage(getName(), type);
    }
  }

  /**
   * 子节点移除存储类型时，递归更新当前节点及所有祖先节点的存储统计。
   * 该方法由子节点调用，用于向上传播存储类型变化。
   * @param childName 移除存储类型的子节点名称
   * @param type 移除的存储类型
   */
  public synchronized void childRemoveStorage(
      String childName, StorageType type) {
    LOG.debug("child remove storage: {}:{}", childName, type);
    Preconditions.checkArgument(childrenStorageInfo.containsKey(childName));
    EnumMap<StorageType, Integer> typeCount =
        childrenStorageInfo.get(childName);
    Preconditions.checkArgument(typeCount.containsKey(type));
    // 更新子节点存储计数
    if (typeCount.get(type) > 1) {
      typeCount.put(type, typeCount.get(type) - 1);
    } else {
      typeCount.remove(type);
    }
    // 更新当前节点总计数
    Preconditions.checkArgument(storageTypeCounts.containsKey(type));
    if (storageTypeCounts.get(type) > 1) {
      storageTypeCounts.put(type, storageTypeCounts.get(type) - 1);
    } else {
      storageTypeCounts.remove(type);
    }
    // 递归向上更新祖先节点
    if (getParent() != null) {
      ((DFSTopologyNodeImpl)getParent()).childRemoveStorage(getName(), type);
    }
  }
}