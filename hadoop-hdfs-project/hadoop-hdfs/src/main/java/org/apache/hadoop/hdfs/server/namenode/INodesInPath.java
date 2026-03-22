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

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import org.apache.hadoop.util.Preconditions;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.ID_INTEGER_COMPARATOR;

/**
 * 文件路径解析得到的INode集合容器，存储从根目录到目标路径每一层的INode信息，
 * 同时支持快照路径解析，保存快照相关元数据信息，是NameNode路径查找的核心结果载体。
 */
public class INodesInPath {
  public static final Logger LOG = LoggerFactory.getLogger(INodesInPath.class);

  /**
   * 判断路径组件是否为快照目录(.snapshot)
   * @param pathComponent 待判断的路径组件字节数组
   * @return 是快照目录返回true，否则返回false
   */
  private static boolean isDotSnapshotDir(byte[] pathComponent) {
    return pathComponent != null &&
        Arrays.equals(HdfsServerConstants.DOT_SNAPSHOT_DIR_BYTES, pathComponent);
  }

  /**
   * 从给定INode向上遍历父节点，收集从根到该INode的所有INode数组
   * @param inode 目标INode
   * @return 从根到目标INode顺序排列的INode数组
   */
  private static INode[] getINodes(final INode inode) {
    int depth = 0, index;
    INode tmp = inode;
    while (tmp != null) {
      depth++;
      tmp = tmp.getParent();
    }
    INode[] inodes = new INode[depth];
    tmp = inode;
    index = depth;
    while (tmp != null) {
      index--;
      inodes[index] = tmp;
      tmp = tmp.getParent();
    }
    return inodes;
  }

  /**
   * 从INode数组提取各节点的名称字节数组，生成路径组件数组
   * @param inodes INode数组
   * @return 对应路径组件数组
   */
  private static byte[][] getPaths(final INode[] inodes) {
    byte[][] paths = new byte[inodes.length][];
    for (int i = 0; i < inodes.length; i++) {
      paths[i] = inodes[i].getKey();
    }
    return paths;
  }

  /**
   * 从单个INode构造完整的INodesInPath对象，自动向上遍历收集全路径INode
   * @param inode 目标INode
   * @return 构造完成的INodesInPath对象
   */
  static INodesInPath fromINode(INode inode) {
    INode[] inodes = getINodes(inode);
    byte[][] paths = getPaths(inodes);
    return new INodesInPath(inodes, paths);
  }

  /**
   * 从指定根目录和目标INode构造INodesInPath，会正确解析快照信息，
   * 专门用于 LeaseManager 获取带有租约的打开文件，用于新建快照时捕获元数据。
   * @param rootDir 起始根目录
   * @param inode 需要解析的目标INode
   * @return 构造完成的INodesInPath对象，包含完整快照信息
   */
  static INodesInPath fromINode(final INodeDirectory rootDir, INode inode) {
    byte[][] paths = getPaths(getINodes(inode));
    return resolve(rootDir, paths);
  }

  /**
   * 从路径组件数组构造空INodesInPath对象
   * @param components 路径组件数组
   * @return 构造完成的空INodesInPath对象
   */
  static INodesInPath fromComponents(byte[][] components) {
    return new INodesInPath(new INode[components.length], components);
  }

  /**
   * 从起始目录开始，按路径组件逐级解析得到全路径INode集合，
   * 自动处理快照路径，将/.snapshot/快照名合并为单个组件，保证INode和路径组件一一对应。
   * @param startingDir 起始目录
   * @param components 路径组件数组
   * @return 解析完成的INodesInPath对象
   */
  static INodesInPath resolve(final INodeDirectory startingDir,
      final byte[][] components) {
    return resolve(startingDir, components, false);
  }

  /**
   * 从起始目录开始，按路径组件逐级解析得到全路径INode集合，支持指定是否为/.reserved/raw路径，
   * 自动处理快照路径，将/.snapshot/快照名合并为单个组件，保证INode和路径组件一一对应。
   * @param startingDir 起始目录
   * @param components 路径组件数组
   * @param isRaw 是否为/.reserved/raw加密路径
   * @return 解析完成的INodesInPath对象
   */
  static INodesInPath resolve(final INodeDirectory startingDir,
      byte[][] components, final boolean isRaw) {
    Preconditions.checkArgument(startingDir.compareTo(components[0]) == 0);

    INode curNode = startingDir;
    int count = 0;
    int inodeNum = 0;
    INode[] inodes = new INode[components.length];
    boolean isSnapshot = false;
    int snapshotId = CURRENT_STATE_ID;

    while (count < components.length && curNode != null) {
      final boolean lastComp = (count == components.length - 1);
      // 将当前节点加入结果数组
      inodes[inodeNum++] = curNode;
      final boolean isRef = curNode.isReference();
      final boolean isDir = curNode.isDirectory();
      final INodeDirectory dir = isDir? curNode.asDirectory(): null;
      // 当前目录非引用且已启用快照功能，更新路径上的最新快照ID
      if (!isRef && isDir && dir.isWithSnapshot()) {
        // 非快照路径，更新为最新快照ID
        if (!isSnapshot && shouldUpdateLatestId(
            dir.getDirectoryWithSnapshotFeature().getLastSnapshotId(),
            snapshotId)) {
          snapshotId = dir.getDirectoryWithSnapshotFeature().getLastSnapshotId();
        }
      } else if (isRef && isDir && !lastComp) {
        // 处理引用节点（重命名快照场景），更新正确的目标快照ID
        // 如果是引用节点且不是最后一个组件，处理快照ID逻辑
        if (!isSnapshot) {
          int dstSnapshotId = curNode.asReference().getDstSnapshotId();
          if (snapshotId == CURRENT_STATE_ID || // 重命名目标树无快照
              (dstSnapshotId != CURRENT_STATE_ID &&
               dstSnapshotId >= snapshotId)) { // 目标快照不早于当前路径快照
            int lastSnapshot = CURRENT_STATE_ID;
            DirectoryWithSnapshotFeature sf;
            if (curNode.isDirectory() && 
                (sf = curNode.asDirectory().getDirectoryWithSnapshotFeature()) != null) {
              lastSnapshot = sf.getLastSnapshotId();
            }
            snapshotId = lastSnapshot;
          }
        }
      }
      // 当前是最后一个组件，或当前节点不是目录，终止遍历
      if (lastComp || !isDir) {
        break;
      }

      // 获取下一个路径组件
      final byte[] childName = components[++count];
      // 检查下一个组件是否是.snapshot目录，且当前目录支持快照
      if (isDotSnapshotDir(childName) && dir.isSnapshottable()) {
        isSnapshot = true;
        // 如果.snapshot是最后一个组件，终止遍历
        if (count == components.length - 1) {
          break;
        }
        // 解析获取指定快照对象
        final Snapshot s = dir.getSnapshot(components[count + 1]);
        if (s == null) {
          curNode = null; // 快照不存在，结果设为null终止遍历
        } else {
          curNode = s.getRoot();
          snapshotId = s.getId();
        }
        // 合并.snapshot和快照名为一个路径组件，保证INode和路径组件一一对应
        byte[][] componentsCopy =
            Arrays.copyOf(components, components.length - 1);
        componentsCopy[count] = DFSUtil.string2Bytes(
            DFSUtil.byteArray2PathString(components, count, 2));
        // 移动后续路径组件到新数组
        int start = count + 2;
        System.arraycopy(components, start, componentsCopy, count + 1,
            components.length - start);
        components = componentsCopy;
        // 调整INode数组长度适配新的组件数
        inodes = Arrays.copyOf(inodes, components.length);
      } else {
        // 普通子节点查找，快照路径使用已解析的快照ID
        curNode = dir.getChild(childName,
            isSnapshot ? snapshotId : CURRENT_STATE_ID);
      }
    }
    return new INodesInPath(inodes, components, isRaw, isSnapshot, snapshotId);
  }

  /**
   * 判断是否需要更新路径最新快照ID：当前无快照或新快照ID更早
   * @param sid 待更新的新快照ID
   * @param snapshotId 当前已保存的快照ID
   * @return 需要更新返回true，否则返回false
   */
  private static boolean shouldUpdateLatestId(int sid, int snapshotId) {
    return snapshotId == CURRENT_STATE_ID || (sid != CURRENT_STATE_ID &&
        ID_INTEGER_COMPARATOR.compare(snapshotId, sid) < 0);
  }

  /**
   * 替换指定位置的INode，生成新的INodesInPath对象，会深拷贝INode数组
   * @param iip 原INodesInPath对象
   * @param pos 待替换的位置
   * @param inode 新的INode对象
   * @return 替换后新的INodesInPath对象
   */
  public static INodesInPath replace(INodesInPath iip, int pos, INode inode) {
    Preconditions.checkArgument(iip.length() > 0 && pos > 0 // no for root
        && pos < iip.length());
    if (iip.getINode(pos) == null) {
      Preconditions.checkState(iip.getINode(pos - 1) != null);
    }
    INode[] inodes = new INode[iip.inodes.length];
    System.arraycopy(iip.inodes, 0, inodes, 0, inodes.length);
    inodes[pos] = inode;
    return new INodesInPath(inodes, iip.path, iip.isRaw,
        iip.isSnapshot, iip.snapshotId);
  }

  /**
   * 在现有INodesInPath末尾追加一个子INode，生成新的INodesInPath对象
   * @param iip 原INodesInPath对象
   * @param child 待追加的子INode
   * @param childName 子节点名称字节数组
   * @return 追加后新的INodesInPath对象
   */
  public static INodesInPath append(INodesInPath iip, INode child,
      byte[] childName) {
    Preconditions.checkArgument(iip.length() > 0);
    Preconditions.checkArgument(iip.getLastINode() != null && iip
        .getLastINode().isDirectory());
    INode[] inodes = new INode[iip.length() + 1];
    System.arraycopy(iip.inodes, 0, inodes, 0, inodes.length - 1);
    inodes[inodes.length - 1] = child;
    byte[][] path = new byte[iip.path.length + 1][];
    System.arraycopy(iip.path, 0, path, 0, path.length - 1);
    path[path.length - 1] = childName;
    return new INodesInPath(inodes, path, iip.isRaw,
        iip.isSnapshot, iip.snapshotId);
  }

  /** 路径组件字节数组，每个元素对应一层目录名称 */
  private final byte[][] path;
  /** 缓存路径字符串，延迟初始化 */
  private volatile String pathname;

  /** 解析得到的INode数组，顺序为从根目录到目标路径 */
  private final INode[] inodes;
  /** 标记当前路径是否为快照路径 */
  private final boolean isSnapshot;

  /** 标记当前路径是否为/.reserved/raw加密路径，原始路径会去除该前缀单独标记 */
  private final boolean isRaw;

  /** 快照ID：快照路径为对应快照ID；非快照路径为路径上找到的最新快照ID；无快照为CURRENT_STATE_ID */
  private final int snapshotId;

  private INodesInPath(INode[] inodes, byte[][] path, boolean isRaw,
      boolean isSnapshot,int snapshotId) {
    Preconditions.checkArgument(inodes != null && path != null);
    this.inodes = inodes;
    this.path = path;
    this.isRaw = isRaw;
    this.isSnapshot = isSnapshot;
    this.snapshotId = snapshotId;
  }

  private INodesInPath(INode[] inodes, byte[][] path) {
    this(inodes, path, false, false, CURRENT_STATE_ID);
  }

  /**
   * 获取非快照路径上找到的最新快照ID，仅对非快照路径有效
   * @return 最新快照ID
   */
  public int getLatestSnapshotId() {
    Preconditions.checkState(!isSnapshot);
    return snapshotId;
  }
  
  /**
   * 获取路径对应的快照ID：快照路径返回对应快照ID，非快照路径返回CURRENT_STATE_ID
   * @return 路径快照ID
   */
  public int getPathSnapshotId() {
    return isSnapshot ? snapshotId : CURRENT_STATE_ID;
  }

  /**
   * 获取指定位置的INode，支持负索引：负索引从末尾向前计数
   * @param i 索引位置，负索引表示从末尾向前
   * @return 指定位置的INode
   */
  public INode getINode(int i) {
    return inodes[(i < 0) ? inodes.length + i : i];
  }

  /**
   * 获取路径最后一个INode
   * @return 最后一个INode
   */
  public INode getLastINode() {
    return getINode(-1);
  }

  /**
   * 获取最后一个路径组件的名称字节数组
   * @return 最后一个路径组件名称
   */
  byte[] getLastLocalName() {
    return path[path.length - 1];
  }

  /**
   * 获取所有路径组件数组
   * @return 路径组件字节数组
   */
  public byte[][] getPathComponents() {
    return path;
  }

  /**
   * 获取指定位置的路径组件
   * @param i 位置索引
   * @return 对应路径组件字节数组
   */
  public byte[] getPathComponent(int i) {
    return path[i];
  }

  /**
   * 获取完整路径字符串，延迟缓存结果
   * @return 完整路径字符串
   */
  public String getPath() {
    if (pathname == null) {
      pathname = DFSUtil.byteArray2PathString(path);
    }
    return pathname;
  }

  /**
   * 获取父目录路径字符串
   * @return 父目录路径字符串
   */
  public String getParentPath() {
    return getPath(path.length - 2);
  }

  /**
   * 获取到指定位置为止的路径字符串
   * @param pos 结束位置索引
   * @return 对应路径字符串
   */
  public String getPath(int pos) {
    return DFSUtil.byteArray2PathString(path, 0, pos + 1); // it's a length...
  }

  /**
   * 获取INode数组长度，即路径的层级深度
   * @return INode数组长度
   */
  public int length() {
    return inodes.length;
  }

  /**
   * 获取INode数组的深拷贝
   * @return INode数组拷贝
   */
  public INode[] getINodesArray() {
    INode[] retArr = new INode[inodes.length];
    System.arraycopy(inodes, 0, retArr, 0, inodes.length);
    return retArr;