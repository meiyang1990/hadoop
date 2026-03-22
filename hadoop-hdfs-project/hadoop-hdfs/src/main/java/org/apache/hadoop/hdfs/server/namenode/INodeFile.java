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

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
import static org.apache.hadoop.hdfs.protocol.BlockType.CONTIGUOUS;
import static org.apache.hadoop.hdfs.protocol.BlockType.STRIPED;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.NO_SNAPSHOT_ID;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DiffList;
import org.apache.hadoop.hdfs.server.namenode.visitor.NamespaceVisitor;
import org.apache.hadoop.hdfs.util.LongBitFormat;
import org.apache.hadoop.util.StringUtils;
import static org.apache.hadoop.io.erasurecode.ErasureCodeConstants.REPLICATION_POLICY_ID;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

/**
 * HDFS NameNode中已关闭文件的INode实现，存储文件的元数据和块信息，支持副本和纠删码两种存储布局，同时支持快照功能。
 * 作为文件系统目录树的叶子节点，维护文件的存储策略、副本系数、块列表等核心元数据。
 */
@InterfaceAudience.Private
public class INodeFile extends INodeWithAdditionalFields
    implements INodeFileAttributes, BlockCollection {

  /**
   * 纠删码条带化块的默认副本系数固定为1。
   */
  public static final short DEFAULT_REPL_FOR_STRIPED_BLOCKS = 1;

  /**
   * 将INode转换为INodeFile，默认不接受空输入，不存在或不是文件时抛出异常。
   * @param inode 待转换的INode对象
   * @param path 文件路径，用于异常信息
   * @return 转换后的INodeFile对象
   * @throws FileNotFoundException 当inode为空或不是文件类型时抛出
   */
  /** The same as valueOf(inode, path, false). */
  public static INodeFile valueOf(INode inode, String path
      ) throws FileNotFoundException {
    return valueOf(inode, path, false);
  }

  /**
   * 将INode安全转换为INodeFile，类型检查并处理空输入。
   * @param inode 待转换的INode对象
   * @param path 文件路径，用于异常信息
   * @param acceptNull 是否允许输入inode为空，为空时返回null
   * @return 转换后的INodeFile对象
   * @throws FileNotFoundException 当inode不合法（为空且不接受空，或不是文件类型）时抛出
   */
  /** Cast INode to INodeFile. */
  public static INodeFile valueOf(INode inode, String path, boolean acceptNull)
      throws FileNotFoundException {
    if (inode == null) {
      if (acceptNull) {
        return null;
      } else {
        throw new FileNotFoundException("File does not exist: " + path);
      }
    }
    if (!inode.isFile()) {
      throw new FileNotFoundException("Path is not a file: " + path);
    }
    return inode.asFile();
  }

  /**
   * 文件头信息按位编码格式定义，将存储策略ID、块布局、副本系数/纠删码策略ID、首选块大小编码到一个long变量中。
   * 编码格式共64位：
   * [4-bit 存储策略ID][12-bit 块布局与冗余信息][48-bit 首选块大小]
   * 其中12位块布局与冗余信息格式：
   * [1-bit 块类型标识: 0=连续副本块 1=条带纠删码块][11-bit 冗余信息: 副本系数/纠删码策略ID]
   */
  enum HeaderFormat {
    PREFERRED_BLOCK_SIZE(null, 48, 1),
    BLOCK_LAYOUT_AND_REDUNDANCY(PREFERRED_BLOCK_SIZE.BITS,
        HeaderFormat.LAYOUT_BIT_WIDTH + 11, 0),
    STORAGE_POLICY_ID(BLOCK_LAYOUT_AND_REDUNDANCY.BITS,
        BlockStoragePolicySuite.ID_BIT_LENGTH, 0);

    private final LongBitFormat BITS;

    /** 块布局类型占用的比特数 */
    private static final int LAYOUT_BIT_WIDTH = 1;
    /** 冗余信息域最大可存储值 (2^11 - 1) */
    private static final int MAX_REDUNDANCY = (1 << 11) - 1;

    HeaderFormat(LongBitFormat previous, int length, long min) {
      BITS = new LongBitFormat(name(), previous, length, min);
    }

    /**
     * 从文件头中获取副本系数，条带化文件返回默认值。
     * @param header 文件头编码值
     * @return 副本系数
     */
    static short getReplication(long header) {
      if (isStriped(header)) {
        return DEFAULT_REPL_FOR_STRIPED_BLOCKS;
      } else {
        long layoutRedundancy =
            BLOCK_LAYOUT_AND_REDUNDANCY.BITS.retrieve(header);
        return (short) (layoutRedundancy & MAX_REDUNDANCY);
      }
    }

    /**
     * 从文件头中获取纠删码策略ID。
     * @param header 文件头编码值
     * @return 纠删码策略ID
     */
    static byte getECPolicyID(long header) {
      long layoutRedundancy = BLOCK_LAYOUT_AND_REDUNDANCY.BITS.retrieve(header);
      return (byte) (layoutRedundancy & MAX_REDUNDANCY);
    }

    /**
     * 从文件头中获取首选块大小。
     * @param header 文件头编码值
     * @return 首选块大小（字节）
     */
    static long getPreferredBlockSize(long header) {
      return PREFERRED_BLOCK_SIZE.BITS.retrieve(header);
    }

    /**
     * 从文件头中获取存储策略ID。
     * @param header 文件头编码值
     * @return 存储策略ID
     */
    static byte getStoragePolicyID(long header) {
      return (byte)STORAGE_POLICY_ID.BITS.retrieve(header);
    }

    // Union of all the block type masks. Currently there is only
    // BLOCK_TYPE_MASK_STRIPED
    static final long BLOCK_TYPE_MASK = 1 << 11;
    // Mask to determine if the block type is striped.
    static final long BLOCK_TYPE_MASK_STRIPED = 1 << 11;

    /**
     * 判断文件是否为条带化纠删码布局。
     * @param header 文件头编码值
     * @return true表示条带化，false表示连续副本布局
     */
    static boolean isStriped(long header) {
      return getBlockType(header) == STRIPED;
    }

    /**
     * 从文件头中获取块类型。
     * @param header 文件头编码值
     * @return 块类型：CONTIGUOUS（连续副本）或STRIPED（条带纠删）
     */
    static BlockType getBlockType(long header) {
      long layoutRedundancy = BLOCK_LAYOUT_AND_REDUNDANCY.BITS.retrieve(header);
      long blockType = layoutRedundancy & BLOCK_TYPE_MASK;
      if (blockType == BLOCK_TYPE_MASK_STRIPED) {
        return STRIPED;
      } else {
        return CONTIGUOUS;
      }
    }

    /**
     * 根据块类型、副本系数、纠删码策略ID构造块布局冗余编码值。
     * @param blockType 块类型
     * @param replication 副本系数，连续块必填，条带块必须为null
     * @param erasureCodingPolicyID 纠删码策略ID，条带块必填，连续块为副本策略ID
     * @return 编码后的块布局冗余值
     */
    static long getBlockLayoutRedundancy(BlockType blockType,
        Short replication, Byte erasureCodingPolicyID) {
      if (null == erasureCodingPolicyID) {
        erasureCodingPolicyID = REPLICATION_POLICY_ID;
      }
      long layoutRedundancy = 0xFF & erasureCodingPolicyID;
      switch (blockType) {
      case STRIPED:
        if (replication != null) {
          throw new IllegalArgumentException(
              "Illegal replication for STRIPED block type");
        }
        if (erasureCodingPolicyID == REPLICATION_POLICY_ID) {
          throw new IllegalArgumentException(
              "Illegal REPLICATION policy for STRIPED block type");
        }
        if (null == ErasureCodingPolicyManager.getInstance()
            .getByID(erasureCodingPolicyID)) {
          throw new IllegalArgumentException(String.format(
                "Could not find EC policy with ID 0x%02x",
                erasureCodingPolicyID));
        }

        // 为条带块设置类型标记位
        layoutRedundancy |= BLOCK_TYPE_MASK_STRIPED;
        break;
      case CONTIGUOUS:
        if (erasureCodingPolicyID != REPLICATION_POLICY_ID) {
          throw new IllegalArgumentException(String.format(
              "Illegal EC policy 0x%02x for CONTIGUOUS block type",
              erasureCodingPolicyID));
        }
        if (null == replication ||
            replication < 0 || replication > MAX_REDUNDANCY) {
          throw new IllegalArgumentException("Invalid replication value "
              + replication);
        }

        // 为连续块设置副本系数
        layoutRedundancy |= replication;
        break;
      default:
        throw new IllegalArgumentException("Unknown blockType: " + blockType);
      }
      return layoutRedundancy;
    }

    /**
     * 将各个字段组合编码为完整的文件头long值。
     * @param preferredBlockSize 首选块大小
     * @param layoutRedundancy 块布局冗余编码值
     * @param storagePolicyID 存储策略ID
     * @return 完整编码后的文件头
     */
    static long toLong(long preferredBlockSize, long layoutRedundancy,
        byte storagePolicyID) {
      long h = 0;
      if (preferredBlockSize == 0) {
        preferredBlockSize = PREFERRED_BLOCK_SIZE.BITS.getMin();
      }
      h = PREFERRED_BLOCK_SIZE.BITS.combine(preferredBlockSize, h);
      h = BLOCK_LAYOUT_AND_REDUNDANCY.BITS.combine(layoutRedundancy, h);
      h = STORAGE_POLICY_ID.BITS.combine(storagePolicyID, h);
      return h;
    }

  }

  /** 编码后的文件头，存储所有元数据信息 */
  private long header = 0L;

  /** 文件所属的块列表，每个元素是一个BlockInfo对象 */
  private BlockInfo[] blocks;

  /**
   * 构造连续副本布局的INodeFile对象。
   * @param id INode ID
   * @param name 文件名字节数组
   * @param permissions 权限状态
   * @param mtime 修改时间
   * @param atime 访问时间
   * @param blklist 块列表
   * @param replication 副本系数
   * @param preferredBlockSize 首选块大小
   */
  public INodeFile(long id, byte[] name, PermissionStatus permissions, long mtime, long atime,
      BlockInfo[] blklist, short replication, long preferredBlockSize) {
    this(id, name, permissions, mtime, atime, blklist, replication, null,
        preferredBlockSize, (byte) 0, CONTIGUOUS);
  }

  /**
   * 通用构造方法，支持连续副本和条带纠删两种块布局。
   * @param id INode ID
   * @param name 文件名字节数组
   * @param permissions 权限状态
   * @param mtime 修改时间
   * @param atime 访问时间
   * @param blklist 块列表
   * @param replication 副本系数
   * @param ecPolicyID 纠删码策略ID
   * @param preferredBlockSize 首选块大小
   * @param storagePolicyID 存储策略ID
   * @param blockType 块类型
   */
  INodeFile(long id, byte[] name, PermissionStatus permissions, long mtime,
      long atime, BlockInfo[] blklist, Short replication, Byte ecPolicyID,
      long preferredBlockSize, byte storagePolicyID, BlockType blockType) {
    super(id, name, permissions, mtime, atime);
    // 计算块布局冗余编码
    final long layoutRedundancy = HeaderFormat.getBlockLayoutRedundancy(
        blockType, replication, ecPolicyID);
    // 生成完整文件头
    header = HeaderFormat.toLong(preferredBlockSize, layoutRedundancy,
        storagePolicyID);
    // 校验所有块类型与文件类型一致
    if (blklist != null && blklist.length > 0) {
      for (BlockInfo b : blklist) {
        Preconditions.checkArgument(b.getBlockType() == blockType);
      }
    }
    setBlocks(blklist);
  }
  
  /**
   * 拷贝构造方法，基于另一个INodeFile创建新对象。
   * @param that 待拷贝的源INodeFile
   */
  public INodeFile(INodeFile that) {
    super(that);
    this.header = that.header;
    this.features = that.features;
    setBlocks(that.blocks);
  }
  
  /**
   * 判断当前INode是否为文件类型，文件INode始终返回true。
   * @return 始终返回true
   */
  /** @return true unconditionally. */
  @Override
  public final boolean isFile() {
    return true;
  }

  /**
   * 将当前INode转换为INodeFile类型，返回自身。
   * @return 当前INodeFile对象
   */
  /** @return this object. */
  @Override
  public final INodeFile asFile() {
    return this;
  }

  /**
   * 比较两个INodeFile的元数据是否相等，包括文件头、权限、ACL、XAttr。
   * @param other 待比较的另一个INodeFileAttributes
   * @return true表示元数据完全相等，false否则
   */
  @Override
  public boolean metadataEquals(INodeFileAttributes other) {
    return other != null
        && getHeaderLong()== other.getHeaderLong()
        && getPermissionLong() == other.getPermissionLong()
        && getAclFeature() == other.getAclFeature()
        && getXAttrFeature() == other.getXAttrFeature();
  }

  /* Start of Under-Construction Feature */

  /**
   * 获取文件的构建中特征，如果不存在则返回null。
   * @return 构建中特征对象，或null
   */
  /**
   * If the inode contains a {@link FileUnderConstructionFeature}, return it;
   * otherwise, return null.
   */
  public final FileUnderConstructionFeature getFileUnderConstructionFeature() {
    return getFeature(FileUnderConstructionFeature.class);
  }

  /**
   * 判断文件当前是否处于构建中