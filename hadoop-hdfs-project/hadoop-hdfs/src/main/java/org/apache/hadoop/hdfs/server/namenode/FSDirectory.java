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

import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ReencryptionInfoProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo.UpdatedReplicationInfo;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfyManager;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

import static org.apache.hadoop.fs.CommonConfigurationKeys.FS_PROTECTED_DIRECTORIES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESS_CONTROL_ENFORCER_REPORTING_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESS_CONTROL_ENFORCER_REPORTING_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_QUOTA_BY_STORAGETYPE_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROTECTED_SUBDIRECTORIES_ENABLE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROTECTED_SUBDIRECTORIES_ENABLE_DEFAULT;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_ENCRYPTION_ZONE;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.SECURITY_XATTR_UNREADABLE_BY_SUPERUSER;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_SATISFY_STORAGE_POLICY;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;

/**
 * 文件系统目录树管理器，维护HDFS命名空间的内存inode树结构，提供路径解析、配额管理、权限检查等核心命名空间操作
 * 本类所有操作均在内存中完成，持久化由FSNamesystem负责写入磁盘
 * @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem
 **/
@InterfaceAudience.Private
public class FSDirectory implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(FSDirectory.class);

  /**
   * 创建根目录inode节点，初始化配额和快照功能
   * @param namesystem 所属的FSNamesystem实例
   * @return 初始化完成的根目录inode
   */
  private static INodeDirectory createRoot(FSNamesystem namesystem) {
    final INodeDirectory r = new INodeDirectory(
        INodeId.ROOT_INODE_ID,
        INodeDirectory.ROOT_NAME,
        namesystem.createFsOwnerPermissions(new FsPermission((short) 0755)),
        0L);
    r.addDirectoryWithQuotaFeature(
        new DirectoryWithQuotaFeature.Builder().
            nameSpaceQuota(DirectoryWithQuotaFeature.DEFAULT_NAMESPACE_QUOTA).
            storageSpaceQuota(DirectoryWithQuotaFeature.DEFAULT_STORAGE_SPACE_QUOTA).
            build());
    r.addSnapshottableFeature();
    r.setSnapshotQuota(0);
    return r;
  }

  @VisibleForTesting
  static boolean CHECK_RESERVED_FILE_NAMES = true;
  public final static String DOT_RESERVED_STRING =
      HdfsConstants.DOT_RESERVED_STRING;
  public final static String DOT_RESERVED_PATH_PREFIX =
      HdfsConstants.DOT_RESERVED_PATH_PREFIX;
  public final static byte[] DOT_RESERVED = 
      DFSUtil.string2Bytes(DOT_RESERVED_STRING);
  private final static String RAW_STRING = "raw";
  private final static byte[] RAW = DFSUtil.string2Bytes(RAW_STRING);
  public final static String DOT_INODES_STRING =
      HdfsConstants.DOT_INODES_STRING;
  public final static byte[] DOT_INODES = 
      DFSUtil.string2Bytes(DOT_INODES_STRING);
  private final static byte[] DOT_DOT =
      DFSUtil.string2Bytes("..");

  public final static HdfsFileStatus DOT_RESERVED_STATUS =
      new HdfsFileStatus.Builder()
        .isdir(true)
        .perm(new FsPermission((short) 01770))
        .build();

  public final static HdfsFileStatus DOT_SNAPSHOT_DIR_STATUS =
      new HdfsFileStatus.Builder()
        .isdir(true)
        .build();

  INodeDirectory rootDir;
  private final FSNamesystem namesystem;
  private volatile boolean skipQuotaCheck = false; //skip while consuming edits
  private final int maxComponentLength;
  private volatile int maxDirItems;
  private final int lsLimit;  // max list limit
  private final int contentCountLimit; // max content summary counts per run
  private final long contentSleepMicroSec;
  private final INodeMap inodeMap; // Synchronized by dirLock
  private long yieldCount = 0; // keep track of lock yield count.
  private int quotaInitThreads;

  private final int inodeXAttrsLimit; //inode xattrs max limit

  // 受保护目录集合，这些目录非空时无法被删除，所有路径均已标准化
  private volatile SortedSet<String> protectedDirectories;
  private final boolean isProtectedSubDirectoriesEnable;

  private final boolean isPermissionEnabled;
  private final boolean isPermissionContentSummarySubAccess;
  /**
   * ACL功能开关，配置关闭时NameNode会拒绝所有ACL相关操作
   */
  private final boolean aclsEnabled;
  /** 访问控制检查耗时超过该阈值会打印警告日志 */
  private final long accessControlEnforcerReportingThresholdMs;
  /**
   * POSIX ACL继承功能开关，非final用于测试
   */
  private boolean posixAclInheritanceEnabled;
  private final boolean xattrsEnabled;
  private final int xattrMaxSize;

  // 访问时间精度，0表示禁用访问时间更新
  private final long accessTimePrecision;
  // 是否开启按存储类型统计配额
  private final boolean quotaByStorageTypeEnabled;

  private final String fsOwnerShortUserName;
  private final String supergroup;
  private final INodeId inodeId;

  private final FSEditLog editLog;

  private HdfsFileStatus[] reservedStatuses;

  private INodeAttributeProvider attributeProvider;

  // 需要绕过外部属性提供者的用户主体集合
  private HashSet<String> usersToBypassExtAttrProvider = null;

  // 是否使用带上下文的新授权API，取决于外部提供者是否支持该接口
  private boolean useAuthorizationWithContextAPI = false;

  // 单个目录最大条目数限制，由于PB消息大小限制设定为640万
  private static final int MAX_DIR_ITEMS = 64 * 100 * 1000;

  /**
   * 设置外部inode属性提供者，检查提供者是否支持新的授权API
   * @param provider 外部属性提供者实例，可为空
   */
  public void setINodeAttributeProvider(
      @Nullable INodeAttributeProvider provider) {
    attributeProvider = provider;

    if (attributeProvider == null) {
      // 关机时会设置为null，直接返回
      return;
    }

    // 检查外部访问控制执行者是否支持checkPermissionWithContext方法
    // 仅在NameNode初始化时检查一次，减少运行时开销
    Class[] cArg = new Class[1];
    cArg[0] = INodeAttributeProvider.AuthorizationContext.class;

    INodeAttributeProvider.AccessControlEnforcer enforcer =
        attributeProvider.getExternalAccessControlEnforcer(null);

    // 外部执行者为空则使用默认实现，默认支持新API
    if (enforcer == null) {
      useAuthorizationWithContextAPI = true;
      return;
    }

    try {
      Class<?> clazz = enforcer.getClass();
      clazz.getDeclaredMethod("checkPermissionWithContext", cArg);
      useAuthorizationWithContextAPI = true;
      LOG.info("Use the new authorization provider API");
    } catch (NoSuchMethodException e) {
      useAuthorizationWithContextAPI = false;
      LOG.info("Fallback to the old authorization provider API because " +
          "the expected method is not found.");
    }
  }

  /**
   * 获取目录读锁，原dirLock已移除，此处仅做占位断言检查，实际依赖FSNamesystem的读锁
   */
  void readLock() {
    assert namesystem.hasReadLock(RwLockMode.FS) :
        "Should hold read lock of namesystem FSLock";
  }

  /**
   * 释放目录读锁，仅做占位断言检查
   */
  void readUnlock() {
    assert namesystem.hasReadLock(RwLockMode.FS) :
        "Should hold read lock of namesystem FSLock";
  }

  /**
   * 获取目录写锁，仅做占位断言检查，实际依赖FSNamesystem的写锁
   */
  void writeLock() {
    assert namesystem.hasWriteLock(RwLockMode.FS) :
        "Should hold write lock of namesystem FSLock";
  }

  /**
   * 释放目录写锁，仅做占位断言检查
   */
  void writeUnlock() {
    assert namesystem.hasWriteLock(RwLockMode.FS) :
        "Should hold write lock of namesystem FSLock";
  }

  /**
   * 检查当前是否持有FS写锁
   * @return 是否持有写锁
   */
  boolean hasWriteLock() {
    return namesystem.hasWriteLock(RwLockMode.FS);
  }

  /**
   * 检查当前是否持有FS读锁
   * @return 是否持有读锁
   */
  boolean hasReadLock() {
    return namesystem.hasReadLock(RwLockMode.FS);
  }

  /**
   * 获取目录列表最大返回条数限制
   * @return 列表限制值
   */
  public int getListLimit() {
    return lsLimit;
  }

  @VisibleForTesting
  public final EncryptionZoneManager ezManager;

  /**
   * 文件名缓存，缓存频繁使用的文件名字节数组，减少堆内存占用
   */
  private final NameCache<ByteArray> nameCache;

  /** 路径解析操作类型，定义不同场景下的路径检查规则 */
  // used to specify path resolution type. *_LINK will return symlinks instead
  // of throwing an unresolved exception
  public enum DirOp {
    /** 读取操作，正常解析符号链接 */
    READ,
    /** 读取操作，不解析符号链接直接返回 */
    READ_LINK,
    /** 写入操作，禁止操作快照路径 */
    WRITE,  // disallows snapshot paths.
    /** 写入操作，不解析符号链接 */
    WRITE_LINK,
    /** 创建操作，在写入基础上额外检查非法路径 */
    CREATE, // like write, but also blocks invalid path names.
    /** 创建操作，不解析符号链接 */
    CREATE_LINK;
  };

  /**
   * 构造FSDirectory实例，从配置初始化各项功能参数
   * @param ns 所属FSNamesystem
   * @param conf Hadoop配置
   * @throws IOException 初始化失败时抛出异常
   */
  FSDirectory(FSNamesystem ns, Configuration conf) throws IOException {
    this.inodeId = new INodeId();
    rootDir = createRoot(ns);
    inodeMap = INodeMap.newInstance(rootDir);
    this.isPermissionEnabled = conf.getBoolean(
      DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY,
      DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT);
    this.isPermissionContentSummarySubAccess = conf.getBoolean(
        DFSConfigKeys.DFS_PERMISSIONS_CONTENT_SUMMARY_SUBACCESS_KEY,
        DFSConfigKeys.DFS_PERMISSIONS_CONTENT_SUMMARY_SUBACCESS_DEFAULT);
    this.fsOwnerShortUserName =
      UserGroupInformation.getCurrentUser().getShortUserName();
    this.supergroup = conf.get(
      DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
      DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
    this.aclsEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_DEFAULT);
    LOG.info("ACLs enabled? " + aclsEnabled);
    this.posixAclInheritanceEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_POSIX_ACL_INHERITANCE_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_POSIX_ACL_INHERITANCE_ENABLED_DEFAULT);
    LOG.info("POSIX ACL inheritance enabled? " + posixAclInheritanceEnabled);
    this.xattrsEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_DEFAULT);
    LOG.info("XAttrs enabled? " + xattrsEnabled);
    this.xattrMaxSize = (int) conf.getLongBytes(
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_DEFAULT);
    Preconditions.checkArgument(xattrMaxSize > 0,
        "The maximum size of an xattr should be > 0: (%s).",
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY);
    Preconditions.checkArgument(xattrMaxSize <=
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_HARD_LIMIT,
        "The maximum size of an xattr should be <= maximum size"
        + " hard limit " + DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_HARD_LIMIT
        + ": (%s).", DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY);

    this.accessTimePrec