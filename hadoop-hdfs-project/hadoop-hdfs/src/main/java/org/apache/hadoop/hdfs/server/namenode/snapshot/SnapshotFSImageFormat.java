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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiffList;
import org.apache.hadoop.hdfs.tools.snapshot.SnapshotDiff;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import org.apache.hadoop.util.Preconditions;

/**
 * 文件级注释：HDFS快照相关信息在FSImage中的读写工具类，负责序列化和反序列化快照元数据
 * 为FSImage持久化提供快照相关数据结构的读写支持
 */
/**
 * A helper class defining static methods for reading/writing snapshot related
 * information from/to FSImage.
 */
public class SnapshotFSImageFormat {
  /**
   * 保存可快照目录的所有快照信息和快照配额到FSImage输出流
   * @param current 快照所属的当前目录
   * @param out 输出流，用于写入FSImage
   * @throws IOException 写入IO异常
   */
  public static void saveSnapshots(INodeDirectory current, DataOutput out)
      throws IOException {
    DirectorySnapshottableFeature sf = current.getDirectorySnapshottableFeature();
    Preconditions.checkArgument(sf != null);
    // list of snapshots in snapshotsByNames
    ReadOnlyList<Snapshot> snapshots = sf.getSnapshotList();
    out.writeInt(snapshots.size());
    for (Snapshot s : snapshots) {
      // write the snapshot id
      out.writeInt(s.getId());
    }
    // snapshot quota
    out.writeInt(sf.getSnapshotQuota());
  }

  /**
   * 将INode差异列表保存到FSImage输出流
   * 采用逆序存储，保证加载时能正确建立引用关系
   * @param diffs 需要保存的INode差异列表
   * @param out 输出流
   * @param referenceMap 引用映射表，用于处理重复引用
   * @throws IOException 写入IO异常
   */
  private static <N extends INode, A extends INodeAttributes, D extends AbstractINodeDiff<N, A, D>>
      void saveINodeDiffs(final AbstractINodeDiffList<N, A, D> diffs,
      final DataOutput out, ReferenceMap referenceMap) throws IOException {
    // Record the diffs in reversed order, so that we can find the correct
    // reference for INodes in the created list when loading the FSImage
    if (diffs == null) {
      out.writeInt(-1); // no diffs
    } else {
      final DiffList<D> list = diffs.asList();
      final int size = list.size();
      out.writeInt(size);
      for (int i = size - 1; i >= 0; i--) {
        list.get(i).write(out, referenceMap);
      }
    }
  }

  /**
   * 保存目录差异列表到FSImage输出流
   */
  public static void saveDirectoryDiffList(final INodeDirectory dir,
      final DataOutput out, final ReferenceMap referenceMap
      ) throws IOException {
    saveINodeDiffs(dir.getDiffs(), out, referenceMap);
  }

  /**
   * 保存文件差异列表到FSImage输出流
   */
  public static void saveFileDiffList(final INodeFile file,
      final DataOutput out) throws IOException {
    saveINodeDiffs(file.getDiffs(), out, null);
  }

  /**
   * 从FSImage输入流加载文件差异列表
   * @param in 输入流
   * @param loader FSImage加载器
   * @return 加载完成的文件差异列表
   * @throws IOException 读取IO异常
   */
  public static FileDiffList loadFileDiffList(DataInput in,
      FSImageFormat.Loader loader) throws IOException {
    final int size = in.readInt();
    if (size == -1) {
      return null;
    } else {
      final FileDiffList diffs = new FileDiffList();
      FileDiff posterior = null;
      for(int i = 0; i < size; i++) {
        final FileDiff d = loadFileDiff(posterior, in, loader);
        diffs.addFirst(d);
        posterior = d;
      }
      return diffs;
    }
  }

  /**
   * 从FSImage输入流加载单个文件差异
   * @param posterior 后序差异，用于建立链表引用
   * @param in 输入流
   * @param loader FSImage加载器
   * @return 加载完成的文件差异对象
   * @throws IOException 读取IO异常
   */
  private static FileDiff loadFileDiff(FileDiff posterior, DataInput in,
      FSImageFormat.Loader loader) throws IOException {
    // 1. Read the id of the Snapshot root to identify the Snapshot
    final Snapshot snapshot = loader.getSnapshot(in);

    // 2. Load file size
    final long fileSize = in.readLong();
    
    // 3. Load snapshotINode 
    final INodeFileAttributes snapshotINode = in.readBoolean()?
        loader.loadINodeFileAttributes(in): null;
    
    return new FileDiff(snapshot.getId(), snapshotINode, posterior, fileSize);
  }

  /**
   * 从FSImage加载创建列表中的INode，通过引用查找实际节点对象
   * @param createdNodeName 创建节点的名称
   * @param parent 创建列表所属的父目录
   * @return 查找到的INode对象
   * @throws IOException 找不到节点时抛出异常
   */
  public static INode loadCreated(byte[] createdNodeName,
      INodeDirectory parent) throws IOException {
    // the INode in the created list should be a reference to another INode
    // in posterior SnapshotDiffs or one of the current children
    for (DirectoryDiff postDiff : parent.getDiffs()) {
      final INode d = postDiff.getChildrenDiff().getDeleted(createdNodeName);
      if (d != null) {
        return d;
      } // else go to the next SnapshotDiff
    } 
    // use the current child
    INode currentChild = parent.getChild(createdNodeName,
        Snapshot.CURRENT_STATE_ID);
    if (currentChild == null) {
      throw new IOException("Cannot find an INode associated with the INode "
          + DFSUtil.bytes2String(createdNodeName)
          + " in created list while loading FSImage.");
    }
    return currentChild;
  }
  
  /**
   * 从FSImage加载创建的INode列表
   * @param parent 列表所属的父目录
   * @param in 输入流
   * @return 加载完成的创建列表
   * @throws IOException 读取IO异常
   */
  private static List<INode> loadCreatedList(INodeDirectory parent,
      DataInput in) throws IOException {
    // read the size of the created list
    int createdSize = in.readInt();
    List<INode> createdList = new ArrayList<INode>(createdSize);
    for (int i = 0; i < createdSize; i++) {
      byte[] createdNodeName = FSImageSerialization.readLocalName(in);
      INode created = loadCreated(createdNodeName, parent);
      createdList.add(created);
    }
    return createdList;
  }
    
  /**
   * 从FSImage加载删除的INode列表
   * @param parent 列表所属的父目录
   * @param createdList 同一个差异中对应的创建列表，用于引用解析
   * @param in 输入流
   * @param loader FSImage加载器
   * @return 加载完成的删除列表
   * @throws IOException 读取IO异常
   */
  private static List<INode> loadDeletedList(INodeDirectory parent,
      List<INode> createdList, DataInput in, FSImageFormat.Loader loader)
      throws IOException {
    int deletedSize = in.readInt();
    List<INode> deletedList = new ArrayList<INode>(deletedSize);
    for (int i = 0; i < deletedSize; i++) {
      final INode deleted = loader.loadINodeWithLocalName(true, in, true);
      deletedList.add(deleted);
      // set parent: the parent field of an INode in the deleted list is not 
      // useful, but set the parent here to be consistent with the original 
      // fsdir tree.
      deleted.setParent(parent);
      if (deleted.isFile()) {
        loader.updateBlocksMap(deleted.asFile());
      }
    }
    return deletedList;
  }
  
  /**
   * 从FSImage加载可快照目录的快照列表和快照配额
   * @param snapshottableParent 目标可快照目录
   * @param numSnapshots 该目录包含的快照数量
   * @param in 输入流
   * @param loader FSImage加载器
   * @throws IOException 读取IO异常
   */
  public static void loadSnapshotList(INodeDirectory snapshottableParent,
      int numSnapshots, DataInput in, FSImageFormat.Loader loader)
      throws IOException {
    DirectorySnapshottableFeature sf = snapshottableParent
        .getDirectorySnapshottableFeature();
    Preconditions.checkArgument(sf != null);
    for (int i = 0; i < numSnapshots; i++) {
      // read snapshots
      final Snapshot s = loader.getSnapshot(in);
      s.getRoot().setParent(snapshottableParent);
      sf.addSnapshot(s);
    }
    int snapshotQuota = in.readInt();
    snapshottableParent.setSnapshotQuota(snapshotQuota);
  }

  /**
   * 从FSImage加载目录差异列表
   * @param dir 目标目录
   * @param in 输入流
   * @param loader FSImage加载器
   * @throws IOException 读取IO异常
   */
  public static void loadDirectoryDiffList(INodeDirectory dir,
      DataInput in, FSImageFormat.Loader loader) throws IOException {
    final int size = in.readInt();
    if (dir.isWithSnapshot()) {
      DirectoryDiffList diffs = dir.getDiffs();
      for (int i = 0; i < size; i++) {
        diffs.addFirst(loadDirectoryDiff(dir, in, loader));
      }
    }
  }

  /**
   * 加载目录差异中的快照INode属性
   * @param snapshot 关联的快照
   * @param in 输入流
   * @param loader FSImage加载器
   * @return 加载完成的目录属性对象
   * @throws IOException 读取IO异常
   */
  private static INodeDirectoryAttributes loadSnapshotINodeInDirectoryDiff(
      Snapshot snapshot, DataInput in, FSImageFormat.Loader loader)
      throws IOException {
    // read the boolean indicating whether snapshotINode == Snapshot.Root
    boolean useRoot = in.readBoolean();      
    if (useRoot) {
      return snapshot.getRoot();
    } else {
      // another boolean is used to indicate whether snapshotINode is non-null
      return in.readBoolean()? loader.loadINodeDirectoryAttributes(in): null;
    }
  }
   
  /**
   * 从FSImage加载单个目录差异对象
   * @param parent 差异所属的父目录
   * @param in 输入流
   * @param loader FSImage加载器
   * @return 加载完成的目录差异对象
   * @throws IOException 读取IO异常
   */
  private static DirectoryDiff loadDirectoryDiff(INodeDirectory parent,
      DataInput in, FSImageFormat.Loader loader) throws IOException {
    // 1. Read the full path of the Snapshot root to identify the Snapshot
    final Snapshot snapshot = loader.getSnapshot(in);

    // 2. Load DirectoryDiff#childrenSize
    int childrenSize = in.readInt();
    
    // 3. Load DirectoryDiff#snapshotINode 
    INodeDirectoryAttributes snapshotINode = loadSnapshotINodeInDirectoryDiff(
        snapshot, in, loader);
    
    // 4. Load the created list in SnapshotDiff#Diff
    List<INode> createdList = loadCreatedList(parent, in);
    
    // 5. Load the deleted list in SnapshotDiff#Diff
    List<INode> deletedList = loadDeletedList(parent, createdList, in, loader);
    
    // 6. Compose the SnapshotDiff
    DiffList<DirectoryDiff> diffs = parent.getDiffs().asList();
    DirectoryDiff sdiff = new DirectoryDiff(snapshot.getId(), snapshotINode,
        diffs.isEmpty() ? null : diffs.get(0), childrenSize, createdList,
        deletedList, snapshotINode == snapshot.getRoot());
    return sdiff;
  }
  

  /**
   * 引用映射类，用于FSImage序列化时处理INode引用避免重复存储
   * 记录已经写入的引用节点，实现共享节点只存储一次
   */
  /** A reference map for fsimage serialization. */
  public static class ReferenceMap {
    /**
     * 记录已保存的带计数INode引用，key为节点ID
     */
    /**
     * Used to indicate whether the reference node itself has been saved
     */
    private final Map<Long, INodeReference.WithCount> referenceMap
        = new HashMap<Long, INodeReference.WithCount>();
    /**
     * 记录引用节点的子树是否已经保存，避免重复写入子树
     */
    /**
     * Used to record whether the subtree of the reference node has been saved 
     */
    private final Map<Long, Long> dirMap = new HashMap<Long, Long>();

    /**
     * 将带计数的INode引用写入FSImage，重复引用只写ID不重复存储节点内容
     * @param withCount 带计数的引用对象
     * @param out 输出流
     * @param writeUnderConstruction 是否写入构造中块信息
     * @throws IOException 写入IO异常
     */
    public void writeINodeReferenceWithCount(
        INodeReference.WithCount withCount, DataOutput out,
        boolean writeUnderConstruction) throws IOException {
      final INode referred = withCount.getReferredINode();
      final long id = withCount.getId();
      final boolean firstReferred = !referenceMap.containsKey(id);
      out.writeBoolean(firstReferred);

      if (firstReferred) {
        FSImageSerialization.saveINode2Image(referred, out,
            writeUnderConstruction, this);
        referenceMap.put(id, withCount);
      } else {
        out.writeLong(id);
      }
    }
    
    /**
     * 检查指定ID目录的子树是否需要处理，判断是否已经写入过
     * @param id 目录节点ID
     * @return true表示需要处理，false表示已经处理过无需重复处理
     */
    public boolean toProcessSubtree(long id) {
      if (dirMap.containsKey(id)) {
        return false;
      } else {
        dirMap.put(id, id);
        return true;
      }
    }
    
    /**
     * 从FSImage加载带计数的INode引用，复用已加载的节点避免重复创建
     * @param isSnapshotINode 是否是快照INode
     * @param in 输入流
     * @param loader FSImage加载器
     * @return 加载完成的引用对象
     * @throws IOException 读取IO异常
     */
    public INodeReference.WithCount loadINodeReferenceWithCount(
        boolean isSnapshotINode, DataInput in, FSImageFormat.Loader loader
        ) throws IOException {
      final boolean firstReferred = in.readBoolean();

      final INodeReference.WithCount withCount;
      if (firstReferred) {
        final INode referred = loader.loadINodeWithLocalName(isSnapshotINode,
            in, true);
        withCount = new INodeReference.WithCount(null, referred);
        referenceMap.put(withCount.getId(), withCount);
      } else {
        final long id = in.readLong();
        withCount = referenceMap.get(id);
      }
      return