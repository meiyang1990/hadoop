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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * PB格式FSImage文件损坏检测器，用于离线检查HDFS fsimage文件中的元数据损坏问题。
 * 生成带分隔符的损坏问题报告，默认使用制表符作为分隔符（避免和inode路径字符冲突）。
 * 核心检测两类损坏：
 *  <ul>
 *    <li>目录引用了不存在的INode id：INode目录节中存在某INode id，但INode节中没有对应记录</li>
 *    <li>目录存在子节点损坏：INode存在，但至少有一个子节点找不到对应记录</li>
 *  </ul>
 * 多层目录损坏时，一个节点可能同时被标记为自身损坏和存在损坏子节点。
 * 注意：当前检测并非覆盖所有损坏类型，仅处理上述常见场景，未来可扩展支持更多损坏类型检测。
 */
public class PBImageCorruptionDetector extends PBImageTextWriter {
  private static final Logger LOG =
      LoggerFactory.getLogger(PBImageCorruptionDetector.class);

  /**
   * 损坏报告输出条目构建器，用于构造符合格式的输出行，isSnapshot为必填字段。
   */
  static class OutputEntryBuilder {
    private static final String MISSING = "Missing";

    private PBImageCorruptionDetector corrDetector;
    private PBImageCorruption corruption;
    private boolean isSnapshot;
    private String parentPath;
    private long parentId;
    private String name;
    private String nodeType;

    OutputEntryBuilder(PBImageCorruptionDetector corrDetector,
        boolean isSnapshot) {
      this.corrDetector = corrDetector;
      this.isSnapshot = isSnapshot;
      this.parentId = -1;
      this.parentPath = "";
      this.name = "";
      this.nodeType = "";
    }

    OutputEntryBuilder setCorruption(PBImageCorruption corr) {
      this.corruption = corr;
      return this;
    }

    OutputEntryBuilder setParentPath(String path) {
      this.parentPath = path;
      return this;
    }

    OutputEntryBuilder setParentId(long id) {
      this.parentId = id;
      return this;
    }

    OutputEntryBuilder setName(String n) {
      this.name = n;
      return this;
    }

    OutputEntryBuilder setNodeType(String nType) {
      this.nodeType = nType;
      return this;
    }

    /**
     * 按格式拼接生成完整的损坏报告行。
     * @return 格式化后的输出行字符串
     */
    public String build() {
      StringBuffer buffer = new StringBuffer();
      // 添加损坏类型
      buffer.append(corruption.getType());
      corrDetector.append(buffer, corruption.getId());
      corrDetector.append(buffer, String.valueOf(isSnapshot));
      corrDetector.append(buffer, parentPath);
      // 父节点ID不存在则输出缺失标记
      if (parentId == -1) {
        corrDetector.append(buffer, MISSING);
      } else {
        corrDetector.append(buffer, parentId);
      }
      corrDetector.append(buffer, name);
      corrDetector.append(buffer, nodeType);
      corrDetector.append(buffer, corruption.getNumOfCorruptChildren());
      return buffer.toString();
    }
  }

  /**
   * INode存在性检查器，用于维护合法INode和INodeReference的ID集合，检查ID是否存在。
   */
  private static class CorruptionChecker {
    private static final String NODE_TYPE = "Node";
    private static final String REF_TYPE = "Ref";
    private static final String UNKNOWN_TYPE = "Unknown";

    /** 所有已存在的普通INode ID集合 */
    private Set<Long> nodeIds;
    /** 所有已存在的INodeReference ID集合 */
    private Set<Long> nodeRefIds;

    CorruptionChecker() {
      nodeIds = new HashSet<>();
    }

    /**
     * 保存一个合法的普通INode ID。
     * @param id INode ID
     */
    void saveNodeId(long id) {
      Preconditions.checkState(nodeIds != null && !nodeIds.contains(id));
      nodeIds.add(id);
    }

    /**
     * 检查给定INode ID是否存在于合法集合中。
     * @param id 待检查的INode ID
     * @return 是否存在
     */
    boolean isNodeIdExist(long id) {
      return nodeIds.contains(id);
    }

    /**
     * 检查给定INodeReference ID是否存在于合法集合中。
     * @param id 待检查的INodeReference ID
     * @return 是否存在
     */
    boolean isNodeRefIdExist(long id) {
      return nodeRefIds.contains(id);
    }

    /**
     * 保存所有INodeReference ID列表。
     * @param nodeRefIdList INodeReference ID列表
     */
    void saveNodeRefIds(List<Long> nodeRefIdList) {
      nodeRefIds = new HashSet<>(nodeRefIdList);
    }

    /**
     * 根据ID获取节点类型。
     * @param id 节点ID
     * @return 节点类型字符串：普通节点/引用节点/未知
     */
    String getTypeOfId(long id) {
      if (isNodeIdExist(id)) {
        return NODE_TYPE;
      } else if (isNodeRefIdExist(id)) {
        return REF_TYPE;
      } else {
        return UNKNOWN_TYPE;
      }
    }
  }

  /** 损坏检查器实例 */
  private final CorruptionChecker corrChecker;
  /** 损坏节点ID到损坏信息的映射表 */
  private final Map<Long, PBImageCorruption> corruptionsMap;

  /**
   * 构造PB格式fsimage损坏检测器实例。
   * @param out 输出流
   * @param delimiter 输出分隔符
   * @param tempPath 临时文件路径
   * @throws IOException IO异常
   */
  PBImageCorruptionDetector(PrintStream out, String delimiter,
        String tempPath) throws IOException {
    super(out, delimiter, tempPath);
    corrChecker = new CorruptionChecker();
    corruptionsMap = new TreeMap<Long, PBImageCorruption>();
  }

  @Override
  /**
   * 获取输出报告表头行。
   * @return 格式化的表头字符串
   */
  public String getHeader() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("CorruptionType");
    append(buffer, "Id");
    append(buffer, "IsSnapshot");
    append(buffer, "ParentPath");
    append(buffer, "ParentId");
    append(buffer, "Name");
    append(buffer, "NodeType");
    append(buffer, "CorruptChildren");
    return buffer.toString();
  }

  @Override
  /**
   * 生成当前INode对应的损坏报告条目，如果该节点没有损坏则返回空字符串。
   * @param parentPath 父节点路径
   * @param inode 当前INode对象
   * @return 格式化的损坏条目，无损坏则返回空
   */
  public String getEntry(String parentPath,
      FsImageProto.INodeSection.INode inode) {
    long id = inode.getId();
    if (corruptionsMap.containsKey(id)) {
      OutputEntryBuilder entryBuilder =
          new OutputEntryBuilder(this, false);
      long parentId = -1;
      try {
        parentId = getParentId(id);
      } catch (IOException ignore) {
      }
      entryBuilder.setCorruption(corruptionsMap.get(id))
          .setParentPath(parentPath)
          .setName(inode.getName().toStringUtf8())
          .setNodeType(corrChecker.getTypeOfId(id));
      if (parentId != -1) {
        entryBuilder.setParentId(parentId);
      }
      corruptionsMap.remove(id);
      return entryBuilder.build();
    } else {
      return "";
    }
  }

  @Override
  /**
   * 处理单个INode节点，保存节点ID到合法集合。
   * @param p INode对象
   * @param numDirs 目录计数器
   * @throws IOException IO异常
   */
  protected void checkNode(FsImageProto.INodeSection.INode p,
        AtomicInteger numDirs) throws IOException {
    super.checkNode(p, numDirs);
    corrChecker.saveNodeId(p.getId());
  }

  /**
   * 添加一个损坏的INode记录到损坏映射表。
   * @param childId 损坏节点的ID
   */
  private void addCorruptedNode(long childId) {
    if (!corruptionsMap.containsKey(childId)) {
      PBImageCorruption c = new PBImageCorruption(childId, false, true, 0);
      corruptionsMap.put(childId, c);
    } else {
      PBImageCorruption c = corruptionsMap.get(childId);
      c.addCorruptNodeCorruption();
      corruptionsMap.put(childId, c);
    }
  }

  /**
   * 添加一个存在损坏子节点的父节点记录到损坏映射表。
   * @param id 父节点ID
   * @param numOfCorruption 损坏子节点数量
   */
  private void addCorruptedParent(long id, int numOfCorruption) {
    if (!corruptionsMap.containsKey(id)) {
      PBImageCorruption c = new PBImageCorruption(id, true, false,
          numOfCorruption);
      corruptionsMap.put(id, c);
    } else {
      PBImageCorruption c = corruptionsMap.get(id);
      c.addMissingChildCorruption();
      c.setNumberOfCorruption(numOfCorruption);
      corruptionsMap.put(id, c);
    }
  }

  /**
   * 扫描INode目录节，构建命名空间并检查损坏。
   * @param in 目录节输入流
   * @param refIdList INodeReference ID列表
   * @throws IOException IO异常
   */
  @Override
  protected void buildNamespace(InputStream in, List<Long> refIdList)
      throws IOException {
    // 保存所有引用节点ID
    corrChecker.saveNodeRefIds(refIdList);
    LOG.debug("Saved INodeReference ids of size {}.", refIdList.size());
    int count = 0;
    // 循环读取每个目录条目
    while (true) {
      FsImageProto.INodeDirectorySection.DirEntry e =
          FsImageProto.INodeDirectorySection.DirEntry.parseDelimitedFrom(in);
      // 读取完毕退出循环
      if (e == null) {
        break;
      }
      count++;
      if (LOG.isDebugEnabled() && count % 10000 == 0) {
        LOG.debug("Scanned {} directories.", count);
      }
      long parentId = e.getParent();
      // 父节点不存在，标记为损坏
      if (!corrChecker.isNodeIdExist(parentId)) {
        LOG.debug("Corruption detected! Parent node is not contained " +
            "in the list of known ids!");
        addCorruptedNode(parentId);
      }
      int numOfCorruption = 0;
      // 遍历所有普通子节点
      for (int i = 0; i < e.getChildrenCount(); i++) {
        long childId = e.getChildren(i);
        // 将子节点关联到父节点元数据
        putDirChildToMetadataMap(parentId, childId);
        // 子节点不存在，标记为损坏
        if (!corrChecker.isNodeIdExist(childId)) {
          addCorruptedNode(childId);
          numOfCorruption++;
        }
      }
      // 当前父节点存在损坏子节点，标记父节点
      if (numOfCorruption > 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("{} corruption detected! Child nodes are missing.",
              numOfCorruption);
        }
        addCorruptedParent(parentId, numOfCorruption);
      }
      // 处理引用类型子节点
      for (int i = e.getChildrenCount();
           i < e.getChildrenCount() + e.getRefChildrenCount(); i++) {
        int refId = e.getRefChildren(i - e.getChildrenCount());
        // 引用节点通过索引获取实际ID，添加父节点关联，不做额外正确性检查
        putDirChildToMetadataMap(parentId, refIdList.get(refId));
      }
    }
    LOG.info("Scanned {} INode directories to build namespace.", count);
  }

  @Override
  /**
   * 所有可解析节点输出完成后，输出无法确定路径的损坏节点。
   * @throws IOException IO异常
   */
  public void afterOutput() throws IOException {
    if (!corruptionsMap.isEmpty()) {
      // 输出所有无法确定路径的损坏节点
      LOG.info("Outputting {} more corrupted nodes.", corruptionsMap.size());
      for (PBImageCorruption c : corruptionsMap.values()) {
        long id = c.getId();
        String name = "";
        long parentId = -1;
        try {
          name = getNodeName(id);
        } catch (IgnoreSnapshotException ignored) {
        }
        try {
          parentId = getParentId(id);
        } catch (IgnoreSnapshotException ignored) {
        }
        OutputEntryBuilder entryBuilder =
            new OutputEntryBuilder(this, true);
        entryBuilder.setCorruption(corruptionsMap.get(id))
            .setName(name)
            .setNodeType(corrChecker.getTypeOfId(id));
        if (parentId != -1) {
          entryBuilder.setParentId(parentId);
        }
        printIfNotEmpty(serialOutStream(), entryBuilder.build());
      }
    }
  }
}