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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat.ReferenceMap;
import org.apache.hadoop.hdfs.util.XMLUtils;
import org.apache.hadoop.hdfs.util.XMLUtils.InvalidXmlException;
import org.apache.hadoop.hdfs.util.XMLUtils.Stanza;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import org.apache.hadoop.util.Preconditions;

/**
 * 文件级注释：FSImage文件序列化工具类，提供FSImage中各类数据结构的正确格式序列化能力，
 * 供NameNode保存和加载文件系统元数据镜像时使用。部分成员当前为public供离线镜像查看器(OIV)使用，
 * OIV重构后应改为包私有访问权限。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSImageSerialization {

  // 静态工具类，不允许实例化
  private FSImageSerialization() {}
  
  /**
   * 为了减少对象分配，复用静态对象，但保存FSImage是多线程过程，因此使用ThreadLocal保证线程安全。
   */
  static private final ThreadLocal<TLData> TL_DATA =
    new ThreadLocal<TLData>() {
    @Override
    protected TLData initialValue() {
      return new TLData();
    }
  };

  /**
   * 线程局部数据容器，保存每个线程复用的可写对象，避免重复分配。
   */
  static private final class TLData {
    final DeprecatedUTF8 U_STR = new DeprecatedUTF8();
    final ShortWritable U_SHORT = new ShortWritable();
    final IntWritable U_INT = new IntWritable();
    final LongWritable U_LONG = new LongWritable();
    final FsPermission FILE_PERM = new FsPermission((short) 0);
    final BooleanWritable U_BOOLEAN = new BooleanWritable();
  }

  /**
   * 写入INode的权限状态信息到输出流。
   * @param inode 要写入权限的INode
   * @param out 目标输出流
   * @throws IOException 写入异常
   */
  private static void writePermissionStatus(INodeAttributes inode,
      DataOutput out) throws IOException {
    final FsPermission p = TL_DATA.get().FILE_PERM;
    p.fromShort(inode.getFsPermissionShort());
    PermissionStatus.write(out, inode.getUserName(), inode.getGroupName(), p);
  }

  /**
   * 写入块数组到输出流。
   * @param blocks 块数组
   * @param out 目标输出流
   * @throws IOException 写入异常
   */
  private static void writeBlocks(final Block[] blocks,
      final DataOutput out) throws IOException {
    if (blocks == null) {
      out.writeInt(0);
    } else {
      out.writeInt(blocks.length);
      for (Block blk : blocks) {
        blk.write(out);
      }
    }
  }

  /**
   * 从输入流读取一个正在构建中的INodeFile（未完成写入的文件）。
   * @param in 输入流
   * @param fsNamesys FSNamesystem实例
   * @param imgVersion FSImage版本号
   * @return 读取到的构建中INodeFile
   * @throws IOException 读取异常
   */
  static INodeFile readINodeUnderConstruction(
      DataInput in, FSNamesystem fsNamesys, int imgVersion)
      throws IOException {
    byte[] name = readBytes(in);
    long inodeId = NameNodeLayoutVersion.supports(
        LayoutVersion.Feature.ADD_INODE_ID, imgVersion) ? in.readLong()
        : fsNamesys.dir.allocateNewInodeId();
    short blockReplication = in.readShort();
    long modificationTime = in.readLong();
    long preferredBlockSize = in.readLong();

    int numBlocks = in.readInt();

    final BlockInfoContiguous[] blocksContiguous =
        new BlockInfoContiguous[numBlocks];
    Block blk = new Block();
    int i = 0;
    for (; i < numBlocks - 1; i++) {
      blk.readFields(in);
      blocksContiguous[i] = new BlockInfoContiguous(blk, blockReplication);
    }
    // 最后一个块处于构建中状态
    if(numBlocks > 0) {
      blk.readFields(in);
      blocksContiguous[i] = new BlockInfoContiguous(blk, blockReplication);
      blocksContiguous[i].convertToBlockUnderConstruction(
          BlockUCState.UNDER_CONSTRUCTION, null);
    }

    PermissionStatus perm = PermissionStatus.read(in);
    String clientName = readString(in);
    String clientMachine = readString(in);

    // 旧版本存储了最后一个块的位置信息，新版本不再存储，这里直接读取并忽略
    int numLocs = in.readInt();
    assert numLocs == 0 : "Unexpected block locations";

    // 非protobuf格式的镜像不包含lazyPersist标记，默认传false
    INodeFile file = new INodeFile(inodeId, name, perm, modificationTime,
        modificationTime, blocksContiguous, blockReplication, preferredBlockSize);
    file.toUnderConstruction(clientName, clientMachine);
    return file;
  }

  /**
   * 将正在构建中的INodeFile写入输出流。
   * @param out 目标输出流
   * @param cons 构建中的INodeFile
   * @param path 文件路径
   * @throws IOException 写入异常
   */
  static void writeINodeUnderConstruction(DataOutputStream out, INodeFile cons,
      String path) throws IOException {
    writeString(path, out);
    out.writeLong(cons.getId());
    out.writeShort(cons.getFileReplication());
    out.writeLong(cons.getModificationTime());
    out.writeLong(cons.getPreferredBlockSize());

    writeBlocks(cons.getBlocks(), out);
    cons.getPermissionStatus().write(out);

    FileUnderConstructionFeature uc = cons.getFileUnderConstructionFeature();
    writeString(uc.getClientName(), out);
    writeString(uc.getClientMachine(), out);

    out.writeInt(0); // 不存储最后一个块的位置信息
  }

  /**
   * 序列化文件INode到FSImage输出流。
   * @param file 要序列化的文件INode
   * @param out 目标输出流
   * @param writeUnderConstruction 是否写入构建中信息
   * @throws IOException 序列化异常
   */
  public static void writeINodeFile(INodeFile file, DataOutput out,
      boolean writeUnderConstruction) throws IOException {
    writeLocalName(file, out);
    out.writeLong(file.getId());
    out.writeShort(file.getFileReplication());
    out.writeLong(file.getModificationTime());
    out.writeLong(file.getAccessTime());
    out.writeLong(file.getPreferredBlockSize());

    writeBlocks(file.getBlocks(), out);
    SnapshotFSImageFormat.saveFileDiffList(file, out);

    if (writeUnderConstruction) {
      if (file.isUnderConstruction()) {
        out.writeBoolean(true);
        final FileUnderConstructionFeature uc = file.getFileUnderConstructionFeature();
        writeString(uc.getClientName(), out);
        writeString(uc.getClientMachine(), out);
      } else {
        out.writeBoolean(false);
      }
    }

    writePermissionStatus(file, out);
  }

  /**
   * 序列化文件INode属性到输出流。
   * @param file 文件INode属性
   * @param out 目标输出流
   * @throws IOException 序列化异常
   */
  public static void writeINodeFileAttributes(INodeFileAttributes file,
      DataOutput out) throws IOException {
    writeLocalName(file, out);
    writePermissionStatus(file, out);
    out.writeLong(file.getModificationTime());
    out.writeLong(file.getAccessTime());

    out.writeShort(file.getFileReplication());
    out.writeLong(file.getPreferredBlockSize());
  }

  /**
   * 写入配额信息到输出流。
   * @param quota 配额计数对象
   * @param out 目标输出流
   * @throws IOException 写入异常
   */
  private static void writeQuota(QuotaCounts quota, DataOutput out)
      throws IOException {
    out.writeLong(quota.getNameSpace());
    out.writeLong(quota.getStorageSpace());
  }

  /**
   * 序列化目录INode到FSImage输出流。
   * @param node 要序列化的目录INode
   * @param out 目标输出流
   * @throws IOException 序列化异常
   */
  public static void writeINodeDirectory(INodeDirectory node, DataOutput out)
      throws IOException {
    writeLocalName(node, out);
    out.writeLong(node.getId());
    out.writeShort(0);  // 目录不需要副本因子，占位
    out.writeLong(node.getModificationTime());
    out.writeLong(0);   // 目录不需要访问时间，占位
    out.writeLong(0);   // 目录不需要块大小，占位
    out.writeInt(-1);   // 目录没有数据块，特殊标记

    writeQuota(node.getQuotaCounts(), out);

    if (node.isSnapshottable()) {
      out.writeBoolean(true);
    } else {
      out.writeBoolean(false);
      out.writeBoolean(node.isWithSnapshot());
    }

    writePermissionStatus(node, out);
  }

  /**
   * 序列化目录INode属性到输出流。
   * @param a 目录INode属性
   * @param out 目标输出流
   * @throws IOException 序列化异常
   */
  public static void writeINodeDirectoryAttributes(
      INodeDirectoryAttributes a, DataOutput out) throws IOException {
    writeLocalName(a, out);
    writePermissionStatus(a, out);
    out.writeLong(a.getModificationTime());
    writeQuota(a.getQuotaCounts(), out);
  }

  /**
   * 序列化符号链接INode到FSImage输出流。
   * @param node 要序列化的符号链接INode
   * @param out 目标输出流
   * @throws IOException 序列化异常
   */
  private static void writeINodeSymlink(INodeSymlink node, DataOutput out)
      throws IOException {
    writeLocalName(node, out);
    out.writeLong(node.getId());
    out.writeShort(0);  // 符号链接不需要副本因子，占位
    out.writeLong(0);   // 符号链接不需要修改时间，占位
    out.writeLong(0);   // 符号链接不需要访问时间，占位
    out.writeLong(0);   // 符号链接不需要块大小，占位
    out.writeInt(-2);   // 符号链接特殊标记

    Text.writeString(out, node.getSymlinkString());
    writePermissionStatus(node, out);
  }

  /**
   * 序列化引用INode（快照功能使用，引用已存在的INode避免重复存储）到输出流。
   * @param ref 引用INode
   * @param out 目标输出流
   * @param writeUnderConstruction 是否写入构建中信息
   * @param referenceMap 引用映射表
   * @throws IOException 序列化异常
   */
  private static void writeINodeReference(INodeReference ref, DataOutput out,
      boolean writeUnderConstruction, ReferenceMap referenceMap
      ) throws IOException {
    writeLocalName(ref, out);
    out.writeLong(ref.getId());
    out.writeShort(0);  // 引用不需要副本因子，占位
    out.writeLong(0);   // 引用不需要修改时间，占位
    out.writeLong(0);   // 引用不需要访问时间，占位
    out.writeLong(0);   // 引用不需要块大小，占位
    out.writeInt(-3);   // 引用INode特殊标记

    final boolean isWithName = ref instanceof INodeReference.WithName;
    out.writeBoolean(isWithName);

    if (!isWithName) {
      Preconditions.checkState(ref instanceof INodeReference.DstReference);
      // 目标快照ID
      out.writeInt(ref.getDstSnapshotId());
    } else {
      out.writeInt(((INodeReference.WithName) ref).getLastSnapshotId());
    }

    final INodeReference.WithCount withCount
        = (INodeReference.WithCount)ref.getReferredINode();
    referenceMap.writeINodeReferenceWithCount(withCount, out,
        writeUnderConstruction);
  }

  /**
   * 将一个INode保存到FSImage镜像，根据INode类型分发到对应序列化方法。
   * @param node 要保存的INode
   * @param out 目标输出流
   * @param writeUnderConstruction 是否写入构建中信息
   * @param referenceMap 快照引用映射表
   * @throws IOException 保存异常
   */
  public static void saveINode2Image(INode node, DataOutput out,
      boolean writeUnderConstruction, ReferenceMap referenceMap)
      throws IOException {
    if (node.isReference()) {
      writeINodeReference(node.asReference(), out, writeUnderConstruction,
          referenceMap);
    } else if (node.isDirectory()) {
      writeINodeDirectory(node.asDirectory(), out);
    } else if (node.isSymlink()) {
      writeINodeSymlink(node.asSymlink(), out);
    } else if (node.isFile()) {
      writeINodeFile(node.asFile(), out, writeUnderConstruction);
    }
  }

  /**
   * 从输入流读取字符串，公共方法供OIV使用，ImageLoader移入本包后应改为包私有。
   * @param in 输入流
   * @return 读取到的字符串
   * @throws IOException 读取异常
   */
  @SuppressWarnings("deprecation")
  public static String readString(DataInput in) throws IOException {
    DeprecatedUTF8 ustr = TL_DATA.get().U_STR;
    ustr.readFields(in);
    return ustr.toStringChecked();
  }

  /**
   * 从输入流读取字符串，空字符串返回null。
   * @param in 输入流
   * @return 读取到的字符串，空返回null
   * @throws IOException 读取异常
   */
  static String readString_EmptyAsNull(DataInput in) throws IOException {
    final String s = readString(in);
    return s.isEmpty()? null: s;
  }

  /**
   * 将字符串写入输出流。
   * @param str 要写入的字符串
   * @param out 目标输出流
   * @throws IOException 写入异常
   */
  @SuppressWarnings("deprecation")
  public static void writeString(String str, DataOutput out) throws IOException {
    DeprecatedUTF8 ustr = TL_DATA.get().U_STR;
    ustr.set(str);
    ustr.write(out);
  }

  
  /**
   * 从输入流读取长整型。
   * @param in 输入流
   * @return 读取到的长整型
   * @throws IOException 读取异常
   */
  static long readLong(DataInput in) throws IOException {
    LongWritable uLong = TL_DATA.get().U_LONG;
    uLong.readFields(in);
    return uLong.get();
  }

  /**
   * 将长整型写入输出流。
   * @param value 要写入的值
   * @param out 目标输出流
   * @throws IOException 写入异常
   */
  static void writeLong(long value, DataOutputStream out) throws IOException {
    LongWritable uLong =