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

import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.Loader.loadINodeDirectory;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.Loader.loadPermission;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.Loader.updateBlocksMap;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.Saver.buildINodeDirectory;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.Saver.buildINodeFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockTypeProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.AclEntryStatusFormat;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.LoaderContext;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SectionName;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeReferenceSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SnapshotDiffSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SnapshotDiffSection.CreatedListEntry;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SnapshotDiffSection.DiffEntry.Type;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SnapshotSection;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeMap;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.DstReference;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithName;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.hdfs.server.namenode.QuotaByStorageTypeEntry;
import org.apache.hadoop.hdfs.server.namenode.SaveNamespaceContext;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.Root;
import org.apache.hadoop.hdfs.server.namenode.XAttrFeature;
import org.apache.hadoop.hdfs.util.EnumCounters;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.protobuf.ByteString;

/**
 * Protobuf格式FSImage中快照相关信息的读写处理工具类
 * 提供从ProtoBuf格式镜像加载快照信息、以及将快照信息保存到ProtoBuf格式镜像的能力
 */
@InterfaceAudience.Private
public class FSImageFormatPBSnapshot {
  /**
   * 从Protobuf格式FSImage中加载快照相关信息的加载器
   */
  public final static class Loader {
    private final FSNamesystem fsn;
    private final FSDirectory fsDir;
    private final FSImageFormatProtobuf.Loader parent;
    private final Map<Integer, Snapshot> snapshotMap;

    /**
     * 构造快照信息加载器
     * @param fsn 文件系统命名空间对象
     * @param parent FSImage Protobuf格式加载器父对象
     */
    public Loader(FSNamesystem fsn, FSImageFormatProtobuf.Loader parent) {
      this.fsn = fsn;
      this.fsDir = fsn.getFSDirectory();
      this.snapshotMap = new HashMap<Integer, Snapshot>();
      this.parent = parent;
    }

    /**
     * 加载INode引用段，refList中的顺序必须与fsimage中保存的严格一致
     * @param in 输入流
     * @throws IOException IO异常
     */
    public void loadINodeReferenceSection(InputStream in) throws IOException {
      final List<INodeReference> refList = parent.getLoaderContext()
          .getRefList();
      while (true) {
        INodeReferenceSection.INodeReference e = INodeReferenceSection
            .INodeReference.parseDelimitedFrom(in);
        if (e == null) {
          break;
        }
        INodeReference ref = loadINodeReference(e);
        refList.add(ref);
      }
    }

    private INodeReference loadINodeReference(
        INodeReferenceSection.INodeReference r) {
      long referredId = r.getReferredId();
      INode referred = fsDir.getInode(referredId);
      WithCount withCount = (WithCount) referred.getParentReference();
      if (withCount == null) {
        withCount = new INodeReference.WithCount(null, referred);
      }
      final INodeReference ref;
      if (r.hasDstSnapshotId()) { // DstReference
        ref = new INodeReference.DstReference(null, withCount,
            r.getDstSnapshotId());
      } else {
        ref = new INodeReference.WithName(null, withCount, r.getName()
            .toByteArray(), r.getLastSnapshotId());
      }
      return ref;
    }

    /**
     * 从fsimage加载快照段，并为可快照目录添加快照功能特性
     * @param in 输入流
     * @throws IOException IO异常
     */
    public void loadSnapshotSection(InputStream in) throws IOException {
      SnapshotManager sm = fsn.getSnapshotManager();
      SnapshotSection section = SnapshotSection.parseDelimitedFrom(in);
      int snum = section.getNumSnapshots();
      sm.setNumSnapshots(snum);
      sm.setSnapshotCounter(section.getSnapshotCounter());
      for (long sdirId : section.getSnapshottableDirList()) {
        INodeDirectory dir = fsDir.getInode(sdirId).asDirectory();
        if (!dir.isSnapshottable()) {
          dir.addSnapshottableFeature();
        } else {
          // dir is root, and admin set root to snapshottable before
          dir.setSnapshotQuota(
              DirectorySnapshottableFeature.SNAPSHOT_QUOTA_DEFAULT);
        }
        sm.addSnapshottable(dir);
      }
      loadSnapshots(in, snum);
    }

    private void loadSnapshots(InputStream in, int size) throws IOException {
      for (int i = 0; i < size; i++) {
        SnapshotSection.Snapshot pbs = SnapshotSection.Snapshot
            .parseDelimitedFrom(in);
        INodeDirectory root = loadINodeDirectory(pbs.getRoot(),
            parent.getLoaderContext());
        int sid = pbs.getSnapshotId();
        INodeDirectory parent = fsDir.getInode(root.getId()).asDirectory();
        Snapshot snapshot = new Snapshot(sid, root, parent);
        // add the snapshot to parent, since we follow the sequence of
        // snapshotsByNames when saving, we do not need to sort when loading
        parent.getDirectorySnapshottableFeature().addSnapshot(snapshot);
        snapshotMap.put(sid, snapshot);
      }
    }

    /**
     * 从fsimage加载快照差异段
     * @param in 输入流
     * @throws IOException IO异常
     */
    public void loadSnapshotDiffSection(InputStream in) throws IOException {
      final List<INodeReference> refList = parent.getLoaderContext()
          .getRefList();
      while (true) {
        SnapshotDiffSection.DiffEntry entry = SnapshotDiffSection.DiffEntry
            .parseDelimitedFrom(in);
        if (entry == null) {
          break;
        }
        long inodeId = entry.getInodeId();
        INode inode = fsDir.getInode(inodeId);
        SnapshotDiffSection.DiffEntry.Type type = entry.getType();
        switch (type) {
        case FILEDIFF:
          loadFileDiffList(in, inode.asFile(), entry.getNumOfDiff());
          break;
        case DIRECTORYDIFF:
          loadDirectoryDiffList(in, inode.asDirectory(), entry.getNumOfDiff(),
              refList);
          break;
        }
      }
    }

    /** Load FileDiff list for a file with snapshot feature */
    private void loadFileDiffList(InputStream in, INodeFile file, int size)
        throws IOException {
      final FileDiffList diffs = new FileDiffList();
      final LoaderContext state = parent.getLoaderContext();
      final BlockManager bm = fsn.getBlockManager();
      for (int i = 0; i < size; i++) {
        SnapshotDiffSection.FileDiff pbf = SnapshotDiffSection.FileDiff
            .parseDelimitedFrom(in);
        INodeFileAttributes copy = null;
        if (pbf.hasSnapshotCopy()) {
          INodeSection.INodeFile fileInPb = pbf.getSnapshotCopy();
          PermissionStatus permission = loadPermission(
              fileInPb.getPermission(), state.getStringTable());

          AclFeature acl = null;
          if (fileInPb.hasAcl()) {
            int[] entries = AclEntryStatusFormat
                .toInt(FSImageFormatPBINode.Loader.loadAclEntries(
                    fileInPb.getAcl(), state.getStringTable()));
            acl = new AclFeature(entries);
          }
          XAttrFeature xAttrs = null;
          if (fileInPb.hasXAttrs()) {
            xAttrs = new XAttrFeature(FSImageFormatPBINode.Loader.loadXAttrs(
                fileInPb.getXAttrs(), state.getStringTable()));
          }

          boolean isStriped =
              (fileInPb.getBlockType() == BlockTypeProto .STRIPED);
          Short replication =
              (!isStriped ? (short)fileInPb.getReplication() : null);
          Byte ecPolicyID =
              (isStriped ? (byte)fileInPb.getErasureCodingPolicyID() : null);
          copy = new INodeFileAttributes.SnapshotCopy(pbf.getName()
              .toByteArray(), permission, acl, fileInPb.getModificationTime(),
              fileInPb.getAccessTime(), replication, ecPolicyID,
              fileInPb.getPreferredBlockSize(),
              (byte)fileInPb.getStoragePolicyID(), xAttrs,
              PBHelperClient.convert(fileInPb.getBlockType()));
        }

        FileDiff diff = new FileDiff(pbf.getSnapshotId(), copy, null,
            pbf.getFileSize());
        List<BlockProto> bpl = pbf.getBlocksList();
        // in file diff there can only be contiguous blocks
        BlockInfo[] blocks = new BlockInfo[bpl.size()];
        for(int j = 0, e = bpl.size(); j < e; ++j) {
          Block blk = PBHelperClient.convert(bpl.get(j));
          BlockInfo storedBlock = bm.getStoredBlock(blk);
          if(storedBlock == null) {
            storedBlock = (BlockInfoContiguous) fsn.getBlockManager()
                .addBlockCollectionWithCheck(new BlockInfoContiguous(blk,
                    copy.getFileReplication()), file);
          }
          blocks[j] = storedBlock;
        }
        if(blocks.length > 0) {
          diff.setBlocks(blocks);
        }
        diffs.addFirst(diff);
      }
      file.loadSnapshotFeature(diffs);
      short repl = file.getPreferredBlockReplication();
      for (BlockInfo b : file.getBlocks()) {
        if (b.getReplication() < repl) {
          bm.setReplication(b.getReplication(), repl, b);
        }
      }
    }

    /** Load the created list in a DirectoryDiff */
    private List<INode> loadCreatedList(InputStream in, INodeDirectory dir,
        int size) throws IOException {
      List<INode> clist = new ArrayList<INode>(size);
      for (long c = 0; c < size; c++) {
        CreatedListEntry entry = CreatedListEntry.parseDelimitedFrom(in);
        INode created = SnapshotFSImageFormat.loadCreated(entry.getName()
            .toByteArray(), dir);
        clist.add(created);
      }
      return clist;
    }

    private void addToDeletedList(INode dnode, INodeDirectory parent) {
      dnode.setParent(parent);
      if (dnode.isFile()) {
        updateBlocksMap(dnode.asFile(), fsn.getBlockManager());
      }
    }

    /**
     * Load the deleted list in a DirectoryDiff
     */
    private List<INode> loadDeletedList(final List<INodeReference> refList,
        InputStream in, INodeDirectory dir, List<Long> deletedNodes,
        List<Integer> deletedRefNodes)
        throws IOException {
      List<INode> dlist = new ArrayList<INode>(deletedRefNodes.size()
          + deletedNodes.size());
      // load non-reference inodes
      for (long deletedId : deletedNodes) {
        INode deleted = fsDir.getInode(deletedId);
        dlist.add(deleted);
        addToDeletedList(deleted, dir);
      }
      // load reference nodes in the deleted list
      for (int refId : deletedRefNodes) {
        INodeReference deletedRef = refList.get(refId);
        dlist.add(deletedRef);
        addToDeletedList(deletedRef, dir);
      }

      Collections.sort(dlist, new Comparator<INode>() {
        @Override
        public int compare(INode n1, INode n2) {
          return n1.compareTo(n2.getLocalNameBytes());
        }
      });
      return dlist;
    }

    /** Load DirectoryDiff list for a directory with snapshot feature */
    private void loadDirectoryDiffList(InputStream in, INodeDirectory dir,
        int size, final List<INodeReference> refList) throws IOException {
      if (!dir.isWithSnapshot()) {
        dir.addSnapshotFeature(null);
      }
      DirectoryDiffList diffs = dir.getDiffs();
      final LoaderContext state = parent.getLoaderContext();

      for (int i = 0; i < size; i++) {
        // load a directory diff
        SnapshotDiffSection.DirectoryDiff diffInPb = SnapshotDiffSection.
            DirectoryDiff.parseDelimitedFrom(in);
        final int snapshotId = diffInPb.getSnapshotId();
        final Snapshot snapshot = snapshotMap.get(snapshotId);
        int childrenSize = diffInPb.getChildrenSize();
        boolean useRoot = diffInPb.getIsSnapshotRoot();
        INodeDirectoryAttributes copy = null;
        if (useRoot) {
          copy = snapshot.getRoot();
        } else if (diffInPb.hasSnapshotCopy()) {
          INodeSection.INodeDirectory dirCopyInPb = diffInPb.getSnapshotCopy();
          final byte[] name = diffInPb.getName().toByteArray();
          PermissionStatus permission = loadPermission(
              dirCopyInPb.getPermission(), state.getStringTable());
          AclFeature acl = null;
          if (dirCopyInPb.hasAcl()) {
            int[] entries = AclEntryStatusFormat
                .toInt(FSImageFormatPBINode.Loader.loadAclEntries(