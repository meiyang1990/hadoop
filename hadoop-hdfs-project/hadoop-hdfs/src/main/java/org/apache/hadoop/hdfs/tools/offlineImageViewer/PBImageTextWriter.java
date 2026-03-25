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

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SectionName;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.server.namenode.SerialNumberManager;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Time;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_ERASURECODING_POLICY;

/**
 * PB格式Fsimage文本写入器抽象基类，用于读取Protobuf格式的fsimage，为每个INode生成文本输出。
 * 采用两阶段扫描策略构建文件系统命名空间，支持内存和磁盘(LevelDB)两种元数据存储模式，
 * 子类可重写{@link #getEntry(String, INode)}自定义输出格式。
 * <ol>
 *   <li>第一阶段：扫描INode区域读取目录名称，扫描INode_Dir区域读取目录和子节点关系，构建文件系统命名空间存储到{@link MetadataMap}</li>
 *   <li>第二阶段：重新扫描INode区域，对每个INode从MetadataMap查询父目录路径，生成对应文本输出</li>
 * </ol>
 * 提供两种MetadataMap实现：{@link InMemoryMetadataDB}全量存储在内存，O(n)空间复杂度；{@link LevelDBMetadataMap}存储到LevelDB磁盘，O(1)空间复杂度，用户可根据时空权衡选择。
 */
abstract class PBImageTextWriter implements Closeable {
  private static final Logger LOG =
      LoggerFactory.getLogger(PBImageTextWriter.class);

  static final String DEFAULT_DELIMITER = "\t";
  static final String CRLF = StringUtils.CR + StringUtils.LF;

  /**
   * 元数据存储接口，用于在生成输出前构建文件系统命名空间。
   * 核心保存两种映射关系：子INode到父目录INode的映射、目录INode ID到目录名称的映射
   */
  private static interface MetadataMap extends Closeable {
    /**
     * 建立子INode与父目录INode的关联关系
     * @param parentId 父目录INode ID
     * @param childId 子节点INode ID
     * @throws IOException IO异常
     */
    public void putDirChild(long parentId, long childId) throws IOException;

    /**
     * 保存目录INode信息
     * @param dir 目录INode对象
     * @throws IOException IO异常
     */
    public void putDir(INode dir) throws IOException;

    /**
     * 获取指定INode的父目录完整路径
     * @param inode 目标INode ID
     * @return 父目录完整路径
     * @throws IOException IO异常，快照相关INode抛出IgnoreSnapshotException
     */
    public String getParentPath(long inode) throws IOException;

    /**
     * 同步元数据到持久化存储（仅磁盘存储实现需要）
     * @throws IOException IO异常
     */
    public void sync() throws IOException;

    /**
     * 根据INode ID获取INode名称
     * @param id INode ID
     * @return INode名称
     * @throws IOException IO异常，快照相关INode抛出IgnoreSnapshotException
     */
    String getName(long id) throws IOException;

    /**
     * 根据INode ID获取父INode ID
     * @param id INode ID
     * @return 父INode ID
     * @throws IOException IO异常，快照相关INode抛出IgnoreSnapshotException
     */
    long getParentId(long id) throws IOException;
  }

  /**
   * 全内存元数据存储实现，将所有目录和父子关系存储在JVM内存中，速度快但占用内存随集群规模增长
   */
  private static class InMemoryMetadataDB implements MetadataMap {
    /**
     * 内存中的目录节点表示，保存目录自身信息和父目录引用，缓存完整路径
     */
    private static class Dir {
      private final long inode;
      private Dir parent = null;
      private String name;
      private String path = null;  // cached full path of the directory.

      Dir(long inode, String name) {
        this.inode = inode;
        this.name = name;
      }

      private void setParent(Dir parent) {
        Preconditions.checkState(this.parent == null);
        this.parent = parent;
      }

      /**
       * 递归计算并返回当前目录的完整路径，缓存结果避免重复计算
       * @return 当前目录完整路径
       * @throws IgnoreSnapshotException 非根目录无父节点时抛出，表示该节点来自快照
       */
      String getPath() throws IgnoreSnapshotException {
        if (this.parent == null) {
          if (this.inode == INodeId.ROOT_INODE_ID) {
            return "/";
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Not root inode with id {} having no parent.", inode);
            }
            throw PBImageTextWriter.createIgnoredSnapshotException(inode);
          }
        }
        if (this.path == null) {
          this.path = new Path(parent.getPath(), name.isEmpty() ? "/" : name).
              toString();
        }
        return this.path;
      }

      String getName() throws IgnoreSnapshotException {
        return name;
      }

      long getId() {
        return inode;
      }

      @Override
      public boolean equals(Object o) {
        return o instanceof Dir && inode == ((Dir) o).inode;
      }

      @Override
      public int hashCode() {
        return Long.valueOf(inode).hashCode();
      }
    }

    /**
     * 损坏目录节点，用于处理不存在于INodeSection但存在于目录关系中的节点（来自快照），
     * 除getId外所有操作都会抛出IgnoreSnapshotException，保证查询可以正常处理异常节点
     */
    private static class CorruptedDir extends Dir {
      CorruptedDir(long inode) {
        super(inode, null);
      }

      @Override
      String getPath() throws IgnoreSnapshotException {
        throw PBImageTextWriter.createIgnoredSnapshotException(getId());
      }

      @Override
      String getName() throws IgnoreSnapshotException {
        throw PBImageTextWriter.createIgnoredSnapshotException(getId());
      }
    }

    /** INode Id 到 Dir 对象的映射 */
    private Map<Long, Dir> dirMap = new HashMap<>();

    /** 子INode Id 到父目录 Dir 对象的映射 */
    private Map<Long, Dir> dirChildMap = new HashMap<>();

    InMemoryMetadataDB() {
    }

    @Override
    public void close() throws IOException {
    }

    private Dir getOrCreateCorrupted(long id) {
      Dir dir = dirMap.get(id);
      if (dir == null) {
        dir = new CorruptedDir(id);
        dirMap.put(id, dir);
      }
      return dir;
    }

    @Override
    public void putDirChild(long parentId, long childId) {
      Dir parent = getOrCreateCorrupted(parentId);
      Dir child = getOrCreateCorrupted(childId);
      child.setParent(parent);
      Preconditions.checkState(!dirChildMap.containsKey(childId));
      dirChildMap.put(childId, parent);
    }

    @Override
    public void putDir(INode p) {
      Preconditions.checkState(!dirMap.containsKey(p.getId()));
      Dir dir = new Dir(p.getId(), p.getName().toStringUtf8());
      dirMap.put(p.getId(), dir);
    }

    @Override
    public String getParentPath(long inode) throws IOException {
      if (inode == INodeId.ROOT_INODE_ID) {
        return "/";
      }
      Dir parent = dirChildMap.get(inode);
      if (parent == null) {
        // 该INode是快照生成的INodeReference，脱机镜像查看工具不需要输出快照中的元数据，直接忽略
        throw PBImageTextWriter.createIgnoredSnapshotException(inode);
      }
      return parent.getPath();
    }

    @Override
    public void sync() {
    }

    @Override
    public String getName(long id) throws IgnoreSnapshotException {
      Dir dir = dirMap.get(id);
      if (dir != null) {
        return dir.getName();
      }
      throw PBImageTextWriter.createIgnoredSnapshotException(id);
    }

    @Override
    public long getParentId(long id) throws IgnoreSnapshotException {
      Dir parentDir = dirChildMap.get(id);
      if (parentDir != null) {
        return parentDir.getId();
      }
      throw PBImageTextWriter.createIgnoredSnapshotException(id);
    }
  }

  /**
   * 基于LevelDB磁盘存储的元数据实现，将元数据存储在本地磁盘LevelDB中，
   * 内存占用低适合超大集群fsimage处理，通过LRU缓存目录路径提升查询性能
   */
  private static class LevelDBMetadataMap implements MetadataMap {
    /**
     * LevelDB存储封装，提供批量写入能力，减少IO次数提升写入性能
     */
    private static class LevelDBStore implements Closeable {
      private DB db = null;
      private WriteBatch batch = null;
      private int writeCount = 0;
      private static final int BATCH_SIZE = 1024;

      LevelDBStore(final File dbPath) throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        options.errorIfExists(true);
        db = JniDBFactory.factory.open(dbPath, options);
        batch = db.createWriteBatch();
      }

      @Override
      public void close() throws IOException {
        if (batch != null) {
          IOUtils.cleanupWithLogger(null, batch);
          batch = null;
        }
        IOUtils.cleanupWithLogger(null, db);
        db = null;
      }

      public void put(byte[] key, byte[] value) throws IOException {
        batch.put(key, value);
        writeCount++;
        if (writeCount >= BATCH_SIZE) {
          sync();
        }
      }

      public byte[] get(byte[] key) throws IOException {
        return db.get(key);
      }

      public void sync() throws IOException {
        try {
          db.write(batch);
        } finally {
          batch.close();
          batch = null;
        }
        batch = db.createWriteBatch();
        writeCount = 0;
      }
    }

    /**
     * 目录路径LRU缓存，固定容量16384，缓存最近访问的目录路径，减少递归查询次数提升性能
     * 键为目录INode ID，值为目录完整路径
     */
    private static class DirPathCache extends LinkedHashMap<Long, String> {
      private final static int CAPACITY = 16 * 1024;

      DirPathCache() {
        super(CAPACITY);
      }

      @Override
      protected boolean removeEldestEntry(Map.Entry<Long, String> entry) {
        return super.size() > CAPACITY;
      }
    }

    /** 子INode ID到父目录INode ID的映射存储 */
    private LevelDBStore dirChildMap = null;
    /** 目录INode ID到目录名称的映射存储 */
    private LevelDBStore dirMap = null;
    private DirPathCache dirPathCache = new DirPathCache();

    /**
     * 构造函数，初始化LevelDB存储目录
     * @param baseDir LevelDB存储根目录
     * @throws IOException IO异常，目录已存在或创建失败时抛出
     */
    LevelDBMetadataMap(String baseDir) throws IOException {
      File dbDir = new File(baseDir);
      if (dbDir.exists()) {
        throw new IOException("Folder " + dbDir + " already exists! Delete " +
            "manually or provide another (not existing) directory!");
      }
      if (!dbDir.mkdirs()) {
        throw new IOException("Failed to mkdir on " + dbDir);
      }
      try {
        dirChildMap = new LevelDBStore(new File(dbDir, "dirChildMap"));
        dirMap = new LevelDBStore(new File(dbDir, "dirMap"));
      } catch (IOException e) {
        LOG.error("Failed to open LevelDBs", e);
        IOUtils.cleanupWithLogger(null, this);
      }
    }

    @Override
    public void close() throws IOException {
      IOUtils.cleanupWithLogger(null, dirChildMap, dirMap);
      dirChildMap = null;
      dirMap = null;
    }

    private static byte[] toBytes(long value) {
      return ByteBuffer.allocate(8).putLong(value).array();
    }

    private static byte[] toBytes(String value) {
      return value.getBytes(StandardCharsets.UTF_8);
    }

    private static long toLong(byte[] bytes) {
      Preconditions.checkArgument(bytes.length == 8);
      return ByteBuffer.wrap(bytes).getLong();
    }

    private static String toString(byte[] bytes) throws IOException {
      return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public void putDirChild(long parentId, long childId) throws IOException {
      dirChildMap.put(toBytes(childId), toBytes(parentId));
    }

    @Override
    public void putDir(INode dir) throws IOException {
      Preconditions.checkArgument(dir.hasDirectory(),
          "INode %s (%s) is not a directory.", dir.getId(), dir.getName());
      dirMap.put(toBytes(dir.getId()), toBytes(dir.getName().toStringUtf8));
    }

    private long getFromDirChildMap(long inode) throws IOException {
      byte[] bytes = dirChildMap.get(toBytes(inode));
      if (bytes == null) {
        // 该INode是快照生成的INodeReference，脱机镜像查看工具不需要输出快照中的元数据，直接忽略
        throw PBImageTextWriter.createIgnoredSnapshotException(inode);
      }
      if (bytes.length != 8) {
        throw new IOException(
            "bytes array length error. Actual length is " + bytes.length);
      }
      return toLong(bytes);
    }

    @Override
    public String getParentPath(long inode) throws IOException {
      if (inode == INodeId.ROOT_INODE_ID) {
        return "/";