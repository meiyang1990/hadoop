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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.NoECPolicySetException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.erasurecode.CodecRegistry;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_ERASURECODING_POLICY;

/**
 * 文件系统目录纠删码操作工具类，提供纠删码策略的增删改查等目录级操作，属于NameNode核心服务层，负责处理目录纠删码策略的元数据管理。
 */
final class FSDirErasureCodingOp {

  /**
   * 私有构造方法，禁止实例化本工具类。
   */
  private FSDirErasureCodingOp() {}

  /**
   * 根据策略名称查询已启用的纠删码策略，包含REPLICATION策略。
   * @param fsn 文件系统命名空间对象
   * @param ecPolicyName 待查询的纠删码策略名称
   * @return 有效的已启用纠删码策略对象
   * @throws IOException 如果查询过程发生IO异常
   */
  static ErasureCodingPolicy getEnabledErasureCodingPolicyByName(
      final FSNamesystem fsn, final String ecPolicyName) throws IOException {
    assert fsn.hasReadLock(RwLockMode.FS);
    ErasureCodingPolicy ecPolicy = fsn.getErasureCodingPolicyManager()
        .getEnabledPolicyByName(ecPolicyName);
    if (ecPolicy == null) {
      // 拼接当前所有已启用策略名称，用于错误提示
      final String sysPolicies =
          Arrays.asList(
              fsn.getErasureCodingPolicyManager().getEnabledPolicies())
              .stream()
              .map(ErasureCodingPolicy::getName)
              .collect(Collectors.joining(", "));
      final String message = String.format("Policy '%s' does not match any " +
              "enabled erasure" +
              " coding policies: [%s]. An erasure coding policy can be" +
              " enabled by enableErasureCodingPolicy API.",
          ecPolicyName,
          sysPolicies
      );
      throw new HadoopIllegalArgumentException(message);
    }
    return ecPolicy;
  }

  /**
   * 根据策略名称查询存在的纠删码策略，包含REPLICATION策略，不要求已启用。
   * @param fsn 文件系统命名空间对象
   * @param ecPolicyName 待查询的纠删码策略名称
   * @return 有效的纠删码策略对象
   * @throws IOException 如果查询过程发生IO异常
   */
  static ErasureCodingPolicy getErasureCodingPolicyByName(
      final FSNamesystem fsn, final String ecPolicyName) throws IOException {
    assert fsn.hasReadLock(RwLockMode.FS);
    ErasureCodingPolicy ecPolicy = fsn.getErasureCodingPolicyManager()
        .getErasureCodingPolicyByName(ecPolicyName);
    if (ecPolicy == null) {
      throw new HadoopIllegalArgumentException(
          "The given erasure coding " + "policy " + ecPolicyName
              + " does not exist.");
    }
    return ecPolicy;
  }

  /**
   * 在指定目录上设置纠编编码策略，将策略信息存储为目录的XAttr扩展属性。
   *
   * @param fsn 文件系统命名空间对象
   * @param srcArg 目标目录路径
   * @param ecPolicyName 要设置的纠删码策略名称
   * @param pc 权限检查器
   * @param logRetryCache 是否在编辑日志中记录RPC ID用于重试缓存重建
   * @return 目标目录的FileStatus对象
   * @throws IOException 如果操作过程发生IO异常
   * @throws HadoopIllegalArgumentException 如果策略不存在或未启用
   * @throws AccessControlException 如果用户没有目标路径的写权限
   */
  static FileStatus setErasureCodingPolicy(final FSNamesystem fsn,
      final String srcArg, final String ecPolicyName,
      final FSPermissionChecker pc, final boolean logRetryCache)
      throws IOException, AccessControlException {
    assert fsn.hasWriteLock(RwLockMode.FS);

    String src = srcArg;
    FSDirectory fsd = fsn.getFSDirectory();
    final INodesInPath iip;
    List<XAttr> xAttrs;
    // 获取目录写锁
    fsd.writeLock();
    try {
      // 检查并获取有效策略
      ErasureCodingPolicy ecPolicy = getEnabledErasureCodingPolicyByName(fsn,
          ecPolicyName);
      // 解析路径得到INodes链表
      iip = fsd.resolvePath(pc, src, DirOp.WRITE_LINK);
      // 检查写权限
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }
      src = iip.getPath();
      // 设置纠删码策略到目录XAttr
      xAttrs = setErasureCodingPolicyXAttr(fsn, iip, ecPolicy);
    } finally {
      fsd.writeUnlock();
    }
    // 记录操作到编辑日志
    fsn.getEditLog().logSetXAttrs(src, xAttrs, logRetryCache);
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * 构造纠删码策略XAttr并更新到目录INode，不做权限检查。
   * @param fsn 文件系统命名空间对象
   * @param srcIIP 目标路径的INodes链表
   * @param ecPolicy 待设置的纠删码策略
   * @return 包含纠删码策略XAttr的列表
   * @throws IOException 如果序列化或操作过程发生异常
   */
  private static List<XAttr> setErasureCodingPolicyXAttr(final FSNamesystem fsn,
      final INodesInPath srcIIP, ErasureCodingPolicy ecPolicy) throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    assert fsd.hasWriteLock();
    Preconditions.checkNotNull(srcIIP, "INodes cannot be null");
    Preconditions.checkNotNull(ecPolicy, "EC policy cannot be null");
    String src = srcIIP.getPath();
    final INode inode = srcIIP.getLastINode();
    // 路径不存在检查
    if (inode == null) {
      throw new FileNotFoundException("Path not found: " + srcIIP.getPath());
    }
    // 只能给目录设置策略检查
    if (!inode.isDirectory()) {
      throw new IOException("Attempt to set an erasure coding policy " +
          "for a file " + src);
    }

    final XAttr ecXAttr;
    DataOutputStream dOut = null;
    try {
      // 将策略名称序列化为字节数组
      ByteArrayOutputStream bOut = new ByteArrayOutputStream();
      dOut = new DataOutputStream(bOut);
      WritableUtils.writeString(dOut, ecPolicy.getName());
      // 构建纠删码策略XAttr
      ecXAttr = XAttrHelper.buildXAttr(XATTR_ERASURECODING_POLICY,
          bOut.toByteArray());
    } finally {
      IOUtils.closeStream(dOut);
    }
    // 检查当前目录是否已有策略，确定操作标志位
    final Boolean hasEcXAttr =
        getErasureCodingPolicyXAttrForINode(fsn, inode) == null ? false : true;
    final List<XAttr> xattrs = Lists.newArrayListWithCapacity(1);
    xattrs.add(ecXAttr);
    final EnumSet<XAttrSetFlag> flag = hasEcXAttr ?
        EnumSet.of(XAttrSetFlag.REPLACE) : EnumSet.of(XAttrSetFlag.CREATE);
    // 调用XAttr操作更新属性
    FSDirXAttrOp.unprotectedSetXAttrs(fsd, srcIIP, xattrs, flag);
    return xattrs;
  }

  /**
   * 从指定目录移除已经设置的纠删码策略。
   *
   * @param fsn 文件系统命名空间对象
   * @param srcArg 目标目录路径
   * @param pc 权限检查器
   * @param logRetryCache 是否在编辑日志中记录RPC ID用于重试缓存重建
   * @return 目标目录的FileStatus对象
   * @throws IOException 如果操作过程发生IO异常
   * @throws AccessControlException 如果用户没有目标路径的写权限
   */
  static FileStatus unsetErasureCodingPolicy(final FSNamesystem fsn,
      final String srcArg, final FSPermissionChecker pc,
      final boolean logRetryCache) throws IOException {
    assert fsn.hasWriteLock(RwLockMode.FS);

    String src = srcArg;
    FSDirectory fsd = fsn.getFSDirectory();
    final INodesInPath iip;
    List<XAttr> xAttrs;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE_LINK);
      // 检查写权限
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }
      src = iip.getPath();
      // 移除目录的纠删码策略XAttr
      xAttrs = removeErasureCodingPolicyXAttr(fsn, iip);
    } finally {
      fsd.writeUnlock();
    }
    if (xAttrs != null) {
      // 记录移除操作到编辑日志
      fsn.getEditLog().logRemoveXAttrs(src, xAttrs, logRetryCache);
    } else {
      // 当前目录没有显式设置策略，抛出异常
      throw new NoECPolicySetException(
          "No erasure coding policy explicitly set on " + src);
    }
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * 向系统中添加新的纠删码策略。
   *
   * @param fsn 文件系统命名空间对象
   * @param policy 待添加的新纠删码策略
   * @param logRetryCache 是否在编辑日志中记录RPC ID用于重试缓存重建
   * @return 添加完成后的策略对象
   */
  static ErasureCodingPolicy addErasureCodingPolicy(final FSNamesystem fsn,
      ErasureCodingPolicy policy, final boolean logRetryCache) {
    Preconditions.checkNotNull(policy);
    ErasureCodingPolicy retPolicy =
        fsn.getErasureCodingPolicyManager().addPolicy(policy);
    // 记录添加操作到编辑日志
    fsn.getEditLog().logAddErasureCodingPolicy(policy, logRetryCache);
    return retPolicy;
  }

  /**
   * 从系统中移除指定名称的纠删码策略。
   *
   * @param fsn 文件系统命名空间对象
   * @param ecPolicyName 待移除的纠删码策略名称
   * @param logRetryCache 是否在编辑日志中记录RPC ID用于重试缓存重建
   * @throws IOException 如果操作过程发生IO异常
   */
  static void removeErasureCodingPolicy(final FSNamesystem fsn,
      String ecPolicyName, final boolean logRetryCache) throws IOException {
    Preconditions.checkNotNull(ecPolicyName);
    fsn.getErasureCodingPolicyManager().removePolicy(ecPolicyName);
    // 记录移除操作到编辑日志
    fsn.getEditLog().logRemoveErasureCodingPolicy(ecPolicyName, logRetryCache);
  }

  /**
   * 启用系统中已存在的指定名称纠删码策略，启用后用户才能将该策略设置到目录。
   *
   * @param fsn 文件系统命名空间对象
   * @param ecPolicyName 待启用的纠删码策略名称
   * @param logRetryCache 是否在编辑日志中记录RPC ID用于重试缓存重建
   * @return 启用成功返回true，策略已启用返回false
   * @throws IOException 如果操作过程发生IO异常
   */
  static boolean enableErasureCodingPolicy(final FSNamesystem fsn,
      String ecPolicyName, final boolean logRetryCache) throws IOException {
    Preconditions.checkNotNull(ecPolicyName);
    boolean success =
        fsn.getErasureCodingPolicyManager().enablePolicy(ecPolicyName);
    if (success) {
      // 记录启用操作到编辑日志
      fsn.getEditLog().logEnableErasureCodingPolicy(ecPolicyName,
          logRetryCache);
    }
    return success;
  }

  /**
   * 禁用系统中已启用的指定名称纠删码策略，禁用后无法将该策略设置到新目录。
   *
   * @param fsn 文件系统命名空间对象
   * @param ecPolicyName 待禁用的纠删码策略名称
   * @param logRetryCache 是否在编辑日志中记录RPC ID用于重试缓存重建
   * @return 禁用成功返回true，策略已禁用返回false
   * @throws IOException 如果操作过程发生IO异常
   */
  static boolean disableErasureCodingPolicy(final FSNamesystem fsn,
      String ecPolicyName, final boolean logRetryCache) throws IOException {
    Preconditions.checkNotNull(ecPolicyName);
    boolean success =
        fsn.getErasureCodingPolicyManager().disablePolicy(ecPolicyName);
    if (success) {
      // 记录禁用操作到编辑日志
      fsn.getEditLog().logDisableErasureCodingPolicy(ecPolicyName,
          logRetryCache);
    }
    return success;
  }

  /**
   * 移除指定目录的纠删码策略XAttr，不做权限检查。
   * @param fsn 文件系统命名空间对象
   * @param srcIIP 目标路径的INodes链表
   * @return 包含被移除XAttr的列表，目录没有策略返回null
   * @throws IOException 如果操作过程发生异常
   */
  private static List<XAttr> removeErasureCodingPolicyXAttr(
      final FSNamesystem fsn, final INodesInPath srcIIP) throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    assert fsd.hasWriteLock();
    Preconditions.checkNotNull(srcIIP, "INodes cannot be null");
    String src = srcIIP.getPath();
    final INode inode = srcIIP.getLastINode();
    if (inode == null) {
      throw new FileNotFoundException("Path not found: " + srcIIP.getPath());
    }
    // 只能从目录移除策略检查
    if (!inode.isDirectory()) {
      throw new IOException("Cannot unset an erasure coding policy " +
          "on a file " + src);
    }

    // 检查当前目录是否有显式设置的策略
    final XAttr ecXAttr = getErasureCodingPolicyXAttrForINode(fsn, inode);
    if (ecXAttr == null) {
      return null;
    }

    final List<XAttr> xattrs = Lists.newArrayListWithCapacity(1);
    xattrs.add(ecXAttr);
    // 调用XAttr操作移除属性
    return FSDirXAttrOp.unprotectedRemoveXAttrs(fsd, srcIIP, xattrs);
  }

  /**
   * 获取指定路径生效的纠删码策略，从当前目录向上追溯直到找到显式设置的策略。
   *
   * @param fsn 文件系统命名空间对象
   * @param src 目标路径
   * @param pc 权限检查器
   * @return 找到的纠删码策略，没有设置或策略是REPLICATION返回null
   * @throws IOException 如果查询过程发生IO异常
   * @throws File