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
package org.apache.hadoop.hdfs.server.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIOException;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;

import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS存储基础抽象类，负责管理本地存储目录、版本信息和存储状态转换
 * <p>
 * HDFS节点（NameNode/DataNode）的本地存储信息存储在VERSION文件中，
 * 包含节点类型、存储布局版本、命名空间ID、文件系统创建时间等核心元数据。
 * 支持多存储目录，每个存储目录维护一致的VERSION文件，启动时从本地磁盘读取存储信息。
 * <p>
 * 通过文件锁机制保证同一存储目录不会被多个节点同时使用，节点停止时自动释放锁。
 * 支持存储升级、回滚、恢复等状态转换，提供原子性的目录操作保证数据一致性。
 * </p>
 */
@InterfaceAudience.Private
public abstract class Storage extends StorageInfo {

  public static final Logger LOG = LoggerFactory
      .getLogger(Storage.class.getName());

  // 不支持升级的最后一个布局版本
  public static final int LAST_PRE_UPGRADE_LAYOUT_VERSION = -3;
  
  // 对应Hadoop-0.18版本，当前仍支持从该版本升级
  public static final int LAST_UPGRADABLE_LAYOUT_VERSION = -16;
  protected static final String LAST_UPGRADABLE_HADOOP_VERSION = "Hadoop-0.18";
  
  /** 0.20.203版本支持的布局版本列表 */
  public static final int[] LAYOUT_VERSIONS_203 = {-19, -31};

  public    static final String STORAGE_FILE_LOCK     = "in_use.lock";
  public    static final String STORAGE_DIR_CURRENT   = "current";
  public    static final String STORAGE_DIR_PREVIOUS  = "previous";
  public    static final String STORAGE_TMP_REMOVED   = "removed.tmp";
  public    static final String STORAGE_TMP_PREVIOUS  = "previous.tmp";
  public    static final String STORAGE_TMP_FINALIZED = "finalized.tmp";
  public    static final String STORAGE_TMP_LAST_CKPT = "lastcheckpoint.tmp";
  public    static final String STORAGE_PREVIOUS_CKPT = "previous.checkpoint";
  
  /**
   * 1.x及更早版本使用的blocksBeingWritten目录，仅用于兼容旧版本存储
   */
  public static final String STORAGE_1_BBW = "blocksBeingWritten";
  
  /**
   * 存储状态枚举，表示存储目录在不同操作阶段的状态
   */
  public enum StorageState {
    NON_EXISTENT,
    NOT_FORMATTED,
    COMPLETE_UPGRADE,
    RECOVER_UPGRADE,
    COMPLETE_FINALIZE,
    COMPLETE_ROLLBACK,
    RECOVER_ROLLBACK,
    COMPLETE_CHECKPOINT,
    RECOVER_CHECKPOINT,
    NORMAL;
  }
  
  /**
   * 存储目录类型接口，用于标识不同用途的存储目录
   * 不同组件（NameNode/DataNode）可实现该接口定义自身的目录类型
   */
  @InterfaceAudience.Private
  public interface StorageDirType {
    public StorageDirType getStorageDirType();
    public boolean isOfType(StorageDirType type);
  }

  // 当前管理的所有存储目录列表，线程安全支持并发访问
  private final List<StorageDirectory> storageDirs =
      new CopyOnWriteArrayList<>();

  /**
   * 存储目录迭代器实现，支持按类型过滤和共享目录过滤
   */
  private class DirIterator implements Iterator<StorageDirectory> {
    final StorageDirType dirType;
    final boolean includeShared;
    int prevIndex; // 用于remove操作
    int nextIndex; // 用于next操作
    
    DirIterator(StorageDirType dirType, boolean includeShared) {
      this.dirType = dirType;
      this.nextIndex = 0;
      this.prevIndex = 0;
      this.includeShared = includeShared;
    }
    
    @Override
    public boolean hasNext() {
      if (storageDirs.isEmpty() || nextIndex >= storageDirs.size())
        return false;
      // 需要过滤时，查找下一个符合条件的目录
      if (dirType != null || !includeShared) {
        while (nextIndex < storageDirs.size()) {
          if (shouldReturnNextDir())
            break;
          nextIndex++;
        }
        if (nextIndex >= storageDirs.size())
         return false;
      }
      return true;
    }
    
    @Override
    public StorageDirectory next() {
      StorageDirectory sd = getStorageDir(nextIndex);
      prevIndex = nextIndex;
      nextIndex++;
      // 查找下一个符合条件的目录位置
      if (dirType != null || !includeShared) {
        while (nextIndex < storageDirs.size()) {
          if (shouldReturnNextDir())
            break;
          nextIndex++;
        }
      }
      return sd;
    }
    
    @Override
    public void remove() {
      nextIndex = prevIndex; // 恢复迭代状态
      storageDirs.remove(prevIndex); // 移除上一次返回的元素
      hasNext(); // 重置nextIndex到正确位置
    }
    
    /**
     * 判断当前索引的目录是否符合过滤条件
     */
    private boolean shouldReturnNextDir() {
      StorageDirectory sd = getStorageDir(nextIndex);
      return (dirType == null || sd.getStorageDirType().isOfType(dirType)) &&
          (includeShared || !sd.isShared());
    }
  }
  
  /**
   * 获取所有存储目录中指定文件名的文件列表
   * @param dirType 存储目录类型，null表示不过滤
   * @param fileName 要获取的文件名
   * @return 所有符合条件的文件对象列表
   */
  public List<File> getFiles(StorageDirType dirType, String fileName) {
    ArrayList<File> list = new ArrayList<File>();
    Iterator<StorageDirectory> it =
      (dirType == null) ? dirIterator() : dirIterator(dirType);
    for ( ;it.hasNext(); ) {
      File currentDir = it.next().getCurrentDir();
      if (currentDir != null) {
        list.add(new File(currentDir, fileName));
      }
    }
    return list;
  }


  /**
   * 获取所有存储目录的迭代器，不过滤类型，包含共享目录
   * @return 存储目录迭代器
   */
  public Iterator<StorageDirectory> dirIterator() {
    return dirIterator(null);
  }
  
  /**
   * 获取指定类型存储目录的迭代器，包含共享目录
   * @param dirType 要过滤的存储目录类型
   * @return 符合类型的存储目录迭代器
   */
  public Iterator<StorageDirectory> dirIterator(StorageDirType dirType) {
    return dirIterator(dirType, true);
  }
  
  /**
   * 获取所有存储目录的迭代器，可选择是否包含共享目录
   * @param includeShared 是否包含共享目录
   * @return 存储目录迭代器
   */
  public Iterator<StorageDirectory> dirIterator(boolean includeShared) {
    return dirIterator(null, includeShared);
  }
  
  /**
   * 获取指定类型存储目录的迭代器，可选择是否包含共享目录
   * @param dirType 要过滤的存储目录类型，null表示不过滤
   * @param includeShared 是否包含共享目录
   * @return 符合条件的存储目录迭代器
   */
  public Iterator<StorageDirectory> dirIterator(StorageDirType dirType,
      boolean includeShared) {
    return new DirIterator(dirType, includeShared);
  }
  
  /**
   * 获取指定类型存储目录的可迭代对象，用于for-each循环
   * @param dirType 要过滤的存储目录类型
   * @return 可迭代对象
   */
  public Iterable<StorageDirectory> dirIterable(final StorageDirType dirType) {
    return new Iterable<StorageDirectory>() {
      @Override
      public Iterator<StorageDirectory> iterator() {
        return dirIterator(dirType);
      }
    };
  }
  
  
  /**
   * 生成所有存储目录的调试信息字符串
   * @return 存储目录列表调试字符串
   */
  public String listStorageDirectories() {
    StringBuilder buf = new StringBuilder();
    for (StorageDirectory sd : storageDirs) {
      buf.append(sd.getRoot() + "(" + sd.getStorageDirType() + ");");
    }
    return buf.toString();
  }
  
  /**
   * 单个存储目录描述类，封装存储目录路径、锁、类型等信息
   * 负责单个存储目录的状态检查、加解锁、目录操作等功能
   */
  @InterfaceAudience.Private
  public static class StorageDirectory implements FormatConfirmable {
    final File root;              // 存储目录根路径
    // 标识该目录是否被多个节点共享：HA场景下被两个NameNode共享，联邦场景下被多个块池共享
    final boolean isShared;
    final StorageDirType dirType; // 存储目录类型
    FileLock lock;                // 存储目录独占锁对象
    private final FsPermission permission; // 目录权限配置

    private String storageUuid = null;      // 存储目录唯一标识符
    
    private final StorageLocation location; // 存储位置描述对象

    /**
     * 构造方法，创建未指定类型的非共享存储目录
     * @param dir 存储目录根路径
     */
    public StorageDirectory(File dir) {
      this(dir, null, false);
    }
    
    /**
     * 构造方法，从存储位置创建存储目录
     * @param location 存储位置对象
     */
    public StorageDirectory(StorageLocation location) {
      this(null, false, location);
    }

    /**
     * 构造方法，创建指定类型的非共享存储目录
     * @param dir 存储目录根路径
     * @param dirType 存储目录类型
     */
    public StorageDirectory(File dir, StorageDirType dirType) {
      this(dir, dirType, false);
    }
    
    public void setStorageUuid(String storageUuid) {
      this.storageUuid = storageUuid;
    }

    public String getStorageUuid() {
      return storageUuid;
    }

    /**
     * 构造方法，创建指定类型和共享属性的存储目录
     * @param dir 存储目录根路径
     * @param dirType 存储目录类型
     * @param isShared 是否共享，共享目录会禁用加锁
     */
    public StorageDirectory(File dir, StorageDirType dirType, boolean isShared) {
      this(dir, dirType, isShared, null);
    }

    /**
     * 构造方法，指定权限配置
     */
    public StorageDirectory(File dir, StorageDirType dirType,
                            boolean isShared, FsPermission permission) {
      this(dir, dirType, isShared, null, permission);
    }

    /**
     * 构造方法，从存储位置创建指定类型和共享属性的存储目录
     * @param dirType 存储目录类型
     * @param isShared 是否共享
     * @param location 存储位置对象
     */
    public StorageDirectory(StorageDirType dirType, boolean isShared,
        StorageLocation location) {
      this(getStorageLocationFile(location), dirType, isShared, location, null);
    }

    /**
     * 构造方法，为指定块池创建存储目录
     * @param bpid 块池ID
     * @param dirType 存储目录类型
     * @param isShared 是否共享
     * @param location 存储位置对象
     */
    public StorageDirectory(String bpid, StorageDirType dirType,
        boolean isShared, StorageLocation location) {
      this(getBlockPoolCurrentDir(bpid, location), dirType,
          isShared, location, null);
    }

    /**
     * 获取块池当前目录路径
     */
    private static File getBlockPoolCurrentDir(String bpid,
        StorageLocation location) {
      if (location == null ||
          location.getStorageType() == StorageType.PROVIDED) {
        return null;
      } else {
        return new File(location.getBpURI(bpid, STORAGE_DIR_CURRENT));
      }
    }

    /**
     * 私有核心构造方法，所有构造方法最终调用这里
     */
    private StorageDirectory(File dir, StorageDirType dirType,
        boolean isShared, StorageLocation location, FsPermission permission) {
      this.root = dir;
      this.lock = null;
      // 默认目录类型为UNDEFINED
      this.dirType = (dirType == null ? NameNodeDirType.UNDEFINED : dirType);
      this.isShared = isShared;
      this.location = location;
      this.permission = permission;
      assert location == null || dir == null ||
          dir.getAbsolutePath().startsWith(
              new File(location.getUri()).getAbsolutePath()):
            "The storage location and directory should be equal";
    }

    /**
     * 从存储位置对象获取根目录文件
     */
    private static File getStorageLocationFile(StorageLocation location) {
      if (location == null ||
          location.getStorageType() == StorageType.PROVIDED) {
        return null;
      }
      try {
        return new File(location.getUri());
      } catch (IllegalArgumentException e) {
        // 位置不指向本地文件时返回null
        return null;
      }
    }

    /**
     * 获取存储目录根路径
     * @return 根目录文件对象
     */
    public File getRoot() {
      return root;
    }

    /**
     * 获取存储目录类型
     * @return 存储目录类型对象
     */
    public StorageDirType getStorageDirType() {
      return dirType;
    }    

    /**
     * 计算存储目录总大小
     * @return 目录大小，单位字节
     */
    public long getDirecorySize() {
      try {
        if (!isShared() && root != null && root.exists()) {
          return FileUtils.sizeOfDirectory(root);
        }
      } catch (Exception e) {
        LOG.warn("Failed to get directory size : {}", root, e);
      }
      return 0;
    }

    /**
     * 从当前存储目录读取VERSION文件属性，设置到Storage对象
     * @param from VERSION文件路径
     * @param storage 要设置属性的Storage对象
     * @throws IOException IO异常
     */
    public void read(File from, Storage storage) throws IOException {
      Properties props = readPropertiesFile(from);
      storage.setFieldsFromProperties(props, this);
    }

    /**
     * 清空并重建存储目录的current目录
     * <p>
     * 删除current目录所有内容，重建空目录。不会写入VERSION文件，
     * VERSION文件需要在所有其他存储相关文件写入完成后最后写入，保证原子性。
     * </p>
     * @throws IOException IO异常
     */
    public void clearDirectory() throws IOException {
      File curDir = this.getCurrentDir();
      if (curDir == null