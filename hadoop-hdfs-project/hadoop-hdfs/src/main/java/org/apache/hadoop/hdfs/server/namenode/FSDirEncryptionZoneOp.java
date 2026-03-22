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

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_FILE_ENCRYPTION_INFO;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;
import java.util.AbstractMap;
import java.util.concurrent.ExecutorService;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ReencryptionInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ZoneEncryptionInfoProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.ReencryptionUpdater.FileEdekInfo;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Time;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_ENCRYPTION_ZONE;
import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * 文件系统加密区域操作工具类，提供HDFS加密区域的创建、查询、重加密等核心操作的封装
 * 是NameNode处理加密区域相关请求的核心工具类，所有操作都围绕加密区域的生命周期管理展开
 */
final class FSDirEncryptionZoneOp {

  /**
   * 私有构造方法，禁止实例化，该类为静态工具类
   */
  private FSDirEncryptionZoneOp() {}

  /**
   * 调用KeyProvider接口为加密区域生成加密的数据加密密钥(EDEK)
   * 调用该方法时不能持有任何锁，避免长时间阻塞其他操作
   *
   * @param fsd 文件目录命名空间对象
   * @param ezKeyName 加密区域的密钥名称
   * @return 生成的加密数据加密密钥，若密钥名称为空则返回null
   * @throws IOException 生成过程中发生IO或密钥服务异常
   */
  private static EncryptedKeyVersion generateEncryptedDataEncryptionKey(
      final FSDirectory fsd, final String ezKeyName) throws IOException {
    // 必须在不持有锁的情况下执行该操作
    assert !fsd.getFSNamesystem().hasReadLock(RwLockMode.FS);
    assert !fsd.getFSNamesystem().hasWriteLock(RwLockMode.FS);
    if (ezKeyName == null) {
      return null;
    }
    long generateEDEKStartTime = monotonicNow();
    // 使用登录用户(hdfs)身份生成EDEK，避免KMS额外配置代理权限
    EncryptedKeyVersion edek = SecurityUtil.doAsLoginUser(
        new PrivilegedExceptionAction<EncryptedKeyVersion>() {
          @Override
          public EncryptedKeyVersion run() throws IOException {
            try {
              return fsd.getProvider().generateEncryptedKey(ezKeyName);
            } catch (GeneralSecurityException e) {
              throw new IOException(e);
            }
          }
        });
    long generateEDEKTime = monotonicNow() - generateEDEKStartTime;
    NameNode.getNameNodeMetrics().addGenerateEDEKTime(generateEDEKTime);
    Preconditions.checkNotNull(edek);
    return edek;
  }

  /**
   * 确保创建加密区域使用的密钥已在KeyProvider中完成初始化，预热EDEK缓存
   *
   * @param fsd 文件目录命名空间对象
   * @param keyName 待检查的密钥名称
   * @param src 目标加密区域路径
   * @return 密钥元数据
   * @throws IOException 密钥不存在、KeyProvider不可用或初始化失败
   */
  static KeyProvider.Metadata ensureKeyIsInitialized(final FSDirectory fsd,
      final String keyName, final String src) throws IOException {
    KeyProviderCryptoExtension provider = fsd.getProvider();
    if (provider == null) {
      throw new IOException("Can't create an encryption zone for " + src
          + " since no key provider is available.");
    }
    if (keyName == null || keyName.isEmpty()) {
      throw new IOException("Must specify a key name when creating an "
          + "encryption zone");
    }
    EncryptionFaultInjector.getInstance().ensureKeyIsInitialized();
    KeyProvider.Metadata metadata = provider.getMetadata(keyName);
    if (metadata == null) {
      throw new IOException("Key " + keyName + " doesn't exist.");
    }
    // 如果KeyProvider支持EDEK缓存池，预热填充对应密钥的缓存池
    provider.warmUpEncryptedKeys(keyName);
    return metadata;
  }

  /**
   * 在指定空目录上创建加密区域，绑定给定密钥
   *
   * @param fsd 文件目录命名空间对象
   * @param srcArg 加密区域根目录路径，必须为空目录
   * @param pc 权限检查器，用于检查访问权限
   * @param cipher 加密算法套件名称
   * @param keyName 加密区域使用的密钥名称，必须已存在于KeyProvider中
   * @param logRetryCache 是否在编辑日志中记录RPC ID用于重试缓存重建
   * @return 创建完成后加密区域根目录的文件状态
   * @throws IOException 创建过程中发生权限、路径或存储异常
   */
  static FileStatus createEncryptionZone(final FSDirectory fsd,
      final String srcArg, final FSPermissionChecker pc, final String cipher,
      final String keyName, final boolean logRetryCache) throws IOException {
    final CipherSuite suite = CipherSuite.convert(cipher);
    List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    // 当前仅支持一种加密区域协议版本，硬编码处理
    final CryptoProtocolVersion version =
        CryptoProtocolVersion.ENCRYPTION_ZONES;

    final INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, srcArg, DirOp.WRITE);
      final XAttr ezXAttr = fsd.ezManager.createEncryptionZone(iip, suite,
          version, keyName);
      xAttrs.add(ezXAttr);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logSetXAttrs(iip.getPath(), xAttrs, logRetryCache);
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * 根据指定路径获取所在的加密区域信息
   *
   * @param fsd 文件目录命名空间对象
   * @param srcArg 待查询的文件或目录路径
   * @param pc 权限检查器，用于检查访问权限
   * @return 加密区域信息与对应路径文件状态的键值对，若不在加密区域则加密区域为null
   * @throws IOException 路径解析或权限检查失败
   */
  static Map.Entry<EncryptionZone, FileStatus> getEZForPath(
      final FSDirectory fsd, final String srcArg, final FSPermissionChecker pc)
      throws IOException {
    final INodesInPath iip;
    final EncryptionZone ret;
    fsd.readLock();
    try {
      iip = fsd.resolvePath(pc, srcArg, DirOp.READ);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.READ);
      }
      ret = fsd.ezManager.getEZINodeForPath(iip);
    } finally {
      fsd.readUnlock();
    }
    FileStatus auditStat = fsd.getAuditFileInfo(iip);
    return new AbstractMap.SimpleImmutableEntry<>(ret, auditStat);
  }

  /**
   * 根据已解析的路径节点获取所在加密区域信息
   *
   * @param fsd 文件目录命名空间对象
   * @param iip 已解析的路径节点列表
   * @return 对应加密区域信息，若不在加密区域则返回null
   * @throws IOException 获取过程中发生异常
   */
  static EncryptionZone getEZForPath(final FSDirectory fsd,
      final INodesInPath iip) throws IOException {
    fsd.readLock();
    try {
      return fsd.ezManager.getEZINodeForPath(iip);
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * 分页列出集群中所有加密区域
   *
   * @param fsd 文件目录命名空间对象
   * @param prevId 上一页最后一个加密区域的ID，用于分页，从头开始传0
   * @return 当前页的加密区域列表
   * @throws IOException 列出加密区域过程中发生异常
   */
  static BatchedListEntries<EncryptionZone> listEncryptionZones(
      final FSDirectory fsd, final long prevId) throws IOException {
    fsd.readLock();
    try {
      return fsd.ezManager.listEncryptionZones(prevId);
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * 发起加密区域的重加密操作，使用新密钥版本重新加密所有文件
   *
   * @param fsd 文件目录命名空间对象
   * @param iip 加密区域根路径节点
   * @param keyVersionName 目标新密钥版本名称
   * @return 更新后的加密区域XAttr列表
   * @throws IOException 发起重加密过程中发生异常
   */
  static List<XAttr> reencryptEncryptionZone(final FSDirectory fsd,
      final INodesInPath iip, final String keyVersionName) throws IOException {
    assert keyVersionName != null;
    return fsd.ezManager.reencryptEncryptionZone(iip, keyVersionName);
  }

  /**
   * 取消加密区域正在进行的重加密操作
   *
   * @param fsd 文件目录命名空间对象
   * @param iip 加密区域根路径节点
   * @return 更新后的加密区域XAttr列表
   * @throws IOException 取消重加密过程中发生异常
   */
  static List<XAttr> cancelReencryptEncryptionZone(final FSDirectory fsd,
      final INodesInPath iip) throws IOException {
    return fsd.ezManager.cancelReencryptionEncryptionZone(iip);
  }

  /**
   * 分页列出所有加密区域的重加密状态
   *
   * @param fsd 文件目录命名空间对象
   * @param prevId 上一页最后一个加密区域的ID，用于分页，从头开始传0
   * @return 当前页的重加密状态列表
   * @throws IOException 列出状态过程中发生异常
   */
  static BatchedListEntries<ZoneReencryptionStatus> listReencryptionStatus(
      final FSDirectory fsd, final long prevId)
      throws IOException {
    fsd.readLock();
    try {
      return fsd.ezManager.listReencryptionStatus(prevId);
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * 更新重加密任务已提交状态，修改加密区域的扩展属性
   * 调用方需要在方法外、释放锁后执行logSync持久化日志
   *
   * @param fsd 文件目录命名空间对象
   * @param iip 加密区域根路径节点
   * @param ezKeyVersionName 提交的目标密钥版本名称
   * @return 更新后的加密区域XAttr
   * @throws IOException 更新过程中发生异常
   */
  static XAttr updateReencryptionSubmitted(final FSDirectory fsd,
      final INodesInPath iip, final String ezKeyVersionName)
      throws IOException {
    assert fsd.hasWriteLock();
    Preconditions.checkNotNull(ezKeyVersionName, "ezKeyVersionName is null.");
    final ZoneEncryptionInfoProto zoneProto = getZoneEncryptionInfoProto(iip);
    Preconditions.checkNotNull(zoneProto, "ZoneEncryptionInfoProto is null.");

    final ReencryptionInfoProto newProto = PBHelperClient
        .convert(ezKeyVersionName, Time.now(), false, 0, 0, null, null);
    final ZoneEncryptionInfoProto newZoneProto = PBHelperClient
        .convert(PBHelperClient.convert(zoneProto.getSuite()),
            PBHelperClient.convert(zoneProto.getCryptoProtocolVersion()),
            zoneProto.getKeyName(), newProto);

    final XAttr xattr = XAttrHelper
        .buildXAttr(CRYPTO_XATTR_ENCRYPTION_ZONE, newZoneProto.toByteArray());
    final List<XAttr> xattrs = Lists.newArrayListWithCapacity(1);
    xattrs.add(xattr);
    FSDirXAttrOp.unprotectedSetXAttrs(fsd, iip, xattrs,
        EnumSet.of(XAttrSetFlag.REPLACE));
    return xattr;
  }

  /**
   * 更新重加密进度，包括开始执行和检查点进度更新
   * 调用方需要在方法外、释放锁后执行logSync持久化日志
   *
   * @param fsd 文件目录命名空间对象
   * @param zoneNode 加密区域根节点
   * @param origStatus 原始重加密状态，用于保留已有信息
   * @param lastFile 当前已处理到的最后一个文件路径
   * @param numReencrypted 本次批次完成重加密的文件数量
   * @param numFailures 本次批次重加密失败的文件数量
   * @return 更新后的加密区域XAttr
   * @throws IOException 更新过程中发生异常
   */
  static XAttr updateReencryptionProgress(final FSDirectory fsd,
      final INode zoneNode, final ZoneReencryptionStatus origStatus,
      final String lastFile, final long numReencrypted, final long numFailures)
      throws IOException {
    assert fsd.hasWriteLock();
    Preconditions.checkNotNull(zoneNode, "Zone node is null");
    INodesInPath iip = INodesInPath.fromINode(zoneNode);
    final ZoneEncryptionInfoProto zoneProto = getZoneEncryptionInfoProto(iip);
    Preconditions.checkNotNull(zoneProto, "ZoneEncryptionInfoProto is null.");
    Preconditions.checkNotNull(origStatus, "Null status for " + iip.getPath());

    final ReencryptionInfoProto newProto = PBHelperClient
        .convert(origStatus.getEzKeyVersionName(),
            origStatus.getSubmissionTime(), false,
            origStatus.getFilesReencrypted() + numReencrypted,
            origStatus.getNumReencryptionFailures() + numFailures, null,
            lastFile);

    final ZoneEncryptionInfoProto newZoneProto = PBHelperClient
        .convert(PBHelperClient.convert(zoneProto.getSuite()),
            PBHelperClient.convert(zoneProto.getCryptoProtocolVersion()),
            zoneProto.getKeyName(), newProto);

    final XAttr xattr = XAttrHelper
        .buildXAttr(CRYPTO_XATTR_ENCRYPTION_ZONE, newZoneProto.toByteArray());
    final List<XAttr> xattrs = Lists.newArrayListWithCapacity(1);
    xattrs.add(xattr);
    FSDirXAttrOp.unprotectedSetXAttrs(fsd, iip, xattrs,
        EnumSet.of(XAttrSetFlag.REPLACE));
    return xattr;
  }

  /**
   * 更新重加密完成状态（正常完成或取消完成），持久化完成信息到编辑日志
   * 调用方需要在方法外、释放锁后执行logSync持久化日志
   *
   * @param fsd 文件目录命名空间对象
   * @param zoneIIP 加密区域根路径节点
   * @param origStatus 原始重加密状态，包含完成原因等信息
   * @return 更新后的加密区域XAttr列表
   * @throws IOException 更新过程中发生异常
   */
  static List<XAttr> updateReencryptionFinish(final FSDirectory fsd,