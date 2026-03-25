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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.LoaderContext;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SaverContext;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeDirectorySection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.AclFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.XAttrCompactProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.XAttrFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.QuotaByStorageTypeEntryProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.QuotaByStorageTypeFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields.PermissionStatusFormat;
import org.apache.hadoop.hdfs.server.namenode.SerialNumberManager.StringTable;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.protobuf.ByteString;

/**
 * 基于Protobuf格式的FSImage中INode节点序列化与反序列化工具类
 * 负责FSImage中INode树结构的加载与保存，支持并行加载提升NameNode启动速度
 */
@InterfaceAudience.Private
public final class FSImageFormatPBINode {
  // ACL条目编码常量：名称掩码
  public static final int ACL_ENTRY_NAME_MASK = (1 << 24) - 1;
  // ACL条目编码常量：名称偏移量
  public static final int ACL_ENTRY_NAME_OFFSET = 6;
  // ACL条目编码常量：类型偏移量
  public static final int ACL_ENTRY_TYPE_OFFSET = 3;
  // ACL条目编码常量：作用域偏移量
  public static final int ACL_ENTRY_SCOPE_OFFSET = 5;
  // ACL条目编码常量：权限掩码
  public static final int ACL_ENTRY_PERM_MASK = 7;
  
  // XAttr编码常量：命名空间掩码
  public static final int XATTR_NAMESPACE_MASK = 3;
  // XAttr编码常量：命名空间偏移量
  public static final int XATTR_NAMESPACE_OFFSET = 30;
  // XAttr编码常量：名称掩码
  public static final int XATTR_NAME_MASK = (1 << 24) - 1;
  // XAttr编码常量：名称偏移量
  public static final int XATTR_NAME_OFFSET = 6;

  /* See the comments in fsimage.proto for an explanation of the following. */
  // XAttr扩展命名空间偏移量
  public static final int XATTR_NAMESPACE_EXT_OFFSET = 5;
  // XAttr扩展命名空间掩码
  public static final int XATTR_NAMESPACE_EXT_MASK = 1;

  private static final Logger LOG =
      LoggerFactory.getLogger(FSImageFormatPBINode.class);

  // 目录条目批量处理大小，控制异步任务粒度
  private static final int DIRECTORY_ENTRY_BATCH_SIZE = 1000;

  /**
   * INode加载器，负责从Protobuf格式FSImage中反序列化INode节点
   * 所有引用序列号的字段都需要通过string表解码
   */
  // the loader must decode all fields referencing serial number based fields
  // via to<Item> methods with the string table.
  public final static class Loader {
    /**
     * 从序列号解码权限信息
     * @param id 权限序列化后的长整型ID
     * @param stringTable 字符串序列号表
     * @return 解码后的权限状态对象
     */
    public static PermissionStatus loadPermission(long id,
        final StringTable stringTable) {
      return PermissionStatusFormat.toPermissionStatus(id, stringTable);
    }

    /**
     * 从Protobuf加载ACL条目列表
     * @param proto ACL特征Protobuf对象
     * @param stringTable 字符串序列号表
     * @return 解码后的ACL条目不可变列表
     */
    public static ImmutableList<AclEntry> loadAclEntries(
        AclFeatureProto proto, final StringTable stringTable) {
      ImmutableList.Builder<AclEntry> b = ImmutableList.builder();
      for (int v : proto.getEntriesList()) {
        b.add(AclEntryStatusFormat.toAclEntry(v, stringTable));
      }
      return b.build();
    }
    
    /**
     * 从Protobuf加载扩展属性XAttr列表
     * @param proto XAttr特征Protobuf对象
     * @param stringTable 字符串序列号表
     * @return 解码后的XAttr列表
     */
    public static List<XAttr> loadXAttrs(
        XAttrFeatureProto proto, final StringTable stringTable) {
      List<XAttr> b = new ArrayList<>();
      for (XAttrCompactProto xAttrCompactProto : proto.getXAttrsList()) {
        int v = xAttrCompactProto.getName();
        byte[] value = null;
        if (xAttrCompactProto.getValue() != null) {
          value = xAttrCompactProto.getValue().toByteArray();
        }
        b.add(XAttrFormat.toXAttr(v, value, stringTable));
      }
      
      return b;
    }

    /**
     * 从Protobuf加载按存储类型划分的配额列表
     * @param proto 存储类型配额特征Protobuf对象
     * @return 解码后的存储类型配额条目列表
     */
    public static ImmutableList<QuotaByStorageTypeEntry> loadQuotaByStorageTypeEntries(
      QuotaByStorageTypeFeatureProto proto) {
      ImmutableList.Builder<QuotaByStorageTypeEntry> b = ImmutableList.builder();
      for (QuotaByStorageTypeEntryProto quotaEntry : proto.getQuotasList()) {
        StorageType type = PBHelperClient.convertStorageType(quotaEntry.getStorageType());
        long quota = quotaEntry.getQuota();
        b.add(new QuotaByStorageTypeEntry.Builder().setStorageType(type)
            .setQuota(quota).build());
      }
      return b.build();
    }

    /**
     * 从Protobuf加载目录INode节点
     * @param n INode Protobuf对象
     * @param state 加载上下文，包含字符串表等状态信息
     * @return 构造完成的目录INode对象
     */
    public static INodeDirectory loadINodeDirectory(INodeSection.INode n,
        LoaderContext state) {
      assert n.getType() == INodeSection.INode.Type.DIRECTORY;
      INodeSection.INodeDirectory d = n.getDirectory();

      final PermissionStatus permissions = loadPermission(d.getPermission(),
          state.getStringTable());
      final INodeDirectory dir = new INodeDirectory(n.getId(), n.getName()
          .toByteArray(), permissions, d.getModificationTime());
      final long nsQuota = d.getNsQuota(), dsQuota = d.getDsQuota();
      // 如果命名空间或存储空间配额有效，添加配额特性
      if (nsQuota >= 0 || dsQuota >= 0) {
        dir.addDirectoryWithQuotaFeature(new DirectoryWithQuotaFeature.Builder().
            nameSpaceQuota(nsQuota).storageSpaceQuota(dsQuota).build());
      }
      EnumCounters<StorageType> typeQuotas = null;
      if (d.hasTypeQuotas()) {
        ImmutableList<QuotaByStorageTypeEntry> qes =
            loadQuotaByStorageTypeEntries(d.getTypeQuotas());
        typeQuotas = new EnumCounters<StorageType>(StorageType.class,
            HdfsConstants.QUOTA_RESET);
        // 遍历配额列表，为每种存储类型设置配额
        for (QuotaByStorageTypeEntry qe : qes) {
          if (qe.getQuota() >= 0 && qe.getStorageType() != null &&
              qe.getStorageType().supportTypeQuota()) {
            typeQuotas.set(qe.getStorageType(), qe.getQuota());
          }
        }

        // 如果存在有效的存储类型配额，添加到目录节点
        if (typeQuotas.anyGreaterOrEqual(0)) {
          DirectoryWithQuotaFeature q = dir.getDirectoryWithQuotaFeature();
          if (q == null) {
            dir.addDirectoryWithQuotaFeature(new DirectoryWithQuotaFeature.
                Builder().typeQuotas(typeQuotas).build());
          } else {
            q.setQuota(typeQuotas);
          }
        }
      }

      // 如果存在ACL特性，添加ACL到目录节点
      if (d.hasAcl()) {
        int[] entries = AclEntryStatusFormat.toInt(loadAclEntries(
            d.getAcl(), state.getStringTable()));
        dir.addAclFeature(new AclFeature(entries));
      }
      // 如果存在XAttr特性，添加XAttr到目录节点
      if (d.hasXAttrs()) {
        dir.addXAttrFeature(new XAttrFeature(
            loadXAttrs(d.getXAttrs(), state.getStringTable())));
      }
      return dir;
    }

    /**
     * 更新块映射表，将文件块信息注册到块管理器
     * @param file 文件INode节点
     * @param bm 块管理器
     */
    public static void updateBlocksMap(INodeFile file, BlockManager bm) {
      // Add file->block mapping
      final BlockInfo[] blocks = file.getBlocks();
      if (blocks != null) {
        for (int i = 0; i < blocks.length; i++) {
          file.setBlock(i, bm.addBlockCollectionWithCheck(blocks[i], file));
        }
      }
    }

    private final FSDirectory dir;
    private final FSNamesystem fsn;
    private final FSImageFormatProtobuf.Loader parent;

    // 单线程异步更新块映射表
    // Update blocks map by single thread asynchronously
    private ExecutorService blocksMapUpdateExecutor;
    // 单线程异步更新名称缓存
    // update name cache by single thread asynchronously.
    private ExecutorService nameCacheUpdateExecutor;

    /**
     * 构造加载器实例
     * @param fsn 文件系统命名空间对象
     * @param parent 父级FSImage Protobuf加载器
     */
    Loader(FSNamesystem fsn, final FSImageFormatProtobuf.Loader parent) {
      this.fsn = fsn;
      this.dir = fsn.dir;
      this.parent = parent;
      // Note: these executors must be SingleThreadExecutor, as they
      // are used to modify structures which are not thread safe.
      // 使用单线程执行器，因为修改的结构非线程安全
      blocksMapUpdateExecutor = Executors.newSingleThreadExecutor();
      nameCacheUpdateExecutor = Executors.newSingleThreadExecutor();
    }

    /**
     * 并行加载INode目录段
     * @param service 并行执行线程池
     * @param sections 目录段子列表
     * @param compressionCodec 压缩编解码器
     * @throws IOException 加载过程中的IO异常
     */
    void loadINodeDirectorySectionInParallel(ExecutorService service,
        ArrayList<FileSummary.Section> sections, String compressionCodec)
        throws IOException {
      LOG.info("Loading the INodeDirectory section in parallel with {} sub-" +
              "sections", sections.size());
      CountDownLatch latch = new CountDownLatch(sections.size());
      final List<IOException> exceptions = Collections.synchronizedList(new ArrayList<>());
      for (FileSummary.Section s : sections) {
        service.submit(() -> {
          InputStream ins = null;
          try {
            ins = parent.getInputStreamForSection(s,
                compressionCodec);
            loadINodeDirectorySection(ins);
          } catch (Exception e) {
            LOG.error("An exception occurred loading INodeDirectories in parallel", e);
            exceptions.add(new IOException(e));
          } finally {
            latch.countDown();
            try {
              if (ins != null) {
                ins.close();
              }
            } catch (IOException ioe) {
              LOG.warn("Failed to close the input stream, ignoring", ioe);
            }
          }
        });
      }
      try {
        // 等待所有子段加载完成
        latch.await();
      } catch (InterruptedException e) {
        LOG.error("Interrupted waiting for countdown latch", e);
        throw new IOException(e);
      }
      if (exceptions.size() != 0) {
        LOG.error("{} exceptions occurred loading INodeDirectories",
            exceptions.size());
        throw exceptions.get(0);
      }
      LOG.info("Completed loading all INodeDirectory sub-sections");
    }

    /**
     * 加载单个INode目录段，将子节点添加到父目录
     * @param in 输入流
     * @throws IOException 加载过程中的IO异常
     */
    void loadINodeDirectorySection(InputStream in) throws IOException {
      final List<INodeReference> refList = parent.getLoaderContext()
          .getRefList();
      while (true) {
        INodeDirectorySection.DirEntry e = INodeDirectorySection.DirEntry
            .parseDelimitedFrom(in);
        // note that in is a LimitedInputStream
        // 解析完整个段后e为null，退出循环
        if (e == null) {
          break;
        }
        // 获取父目录节点
        INodeDirectory p = dir.getInode(e.getParent()).asDirectory();
        // 添加普通子节点到父目录
        for (long id : e.getChildrenList()) {
          INode child = dir.getInode(id);
          if (!addToParent(p, child)) {
            LOG.warn("Failed to add the inode {} to the directory {}",
                child.getId(), p.getId());
          }
        }

        // 添加引用子节点（快照相关）到父目录
        for (int refId : e.getRefChildrenList()) {
          INodeReference ref = refList.get(refId);
          if (!addToParent(p, ref)) {
            LOG.warn("Failed to add the inode reference {} to the directory {}",
                ref.getId(), p.getId());
          }
        }
      }
    }

    /**
     * 将INode添加到批量列表，达到批量大小后提交异步处理
     * @param inodeList 批量INode列表
     * @param inode 待添加的INode节点
     */
    private void fillUpInodeList(ArrayList<INode> inodeList, INode inode) {
      if (inode.isFile()) {
        inodeList.add(inode);
      }
      if (inodeList.size() >= DIRECTORY_ENTRY_BATCH_SIZE) {
        addToCacheAndBlockMap(inodeList);
        inodeList.clear();
      }
    }

    /**
     * 提交INode列表到异步线程，更新名称缓存和块映射表
     * @