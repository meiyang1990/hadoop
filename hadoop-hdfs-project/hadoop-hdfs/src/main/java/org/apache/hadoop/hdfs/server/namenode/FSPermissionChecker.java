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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Stack;
import java.util.function.LongFunction;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider.AccessControlEnforcer;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider.AuthorizationContext;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.hdfs.util.RwLock;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

/** 
 * 文件系统权限检查器，负责HDFS文件系统所有访问权限的校验。
 * 本类状态只读，无需同步，所有检查方法必须持有FSNamesystem的读锁才能调用。
 * 支持自定义外部权限控制扩展，集成ACL权限检查和粘滞位权限规则。
 */
public class FSPermissionChecker implements AccessControlEnforcer {
  static final Logger LOG = LoggerFactory.getLogger(UserGroupInformation.class);

  /**
   * 从路径字节数组构造完整路径字符串。
   * @param components 路径各部分字节数组
   * @param start 起始索引
   * @param end 结束索引
   * @return 完整路径字符串
   */
  private static String getPath(byte[][] components, int start, int end) {
    return DFSUtil.byteArray2PathString(components, start, end - start + 1);
  }

  /**
   * 生成权限拒绝异常的错误信息字符串。
   * @param inodeAttrib 被访问inode的属性
   * @param path 被访问路径
   * @param access 请求的访问权限
   * @return 格式化后的异常信息字符串
   */
  private String toAccessControlString(INodeAttributes inodeAttrib, String path,
      FsAction access) {
    return toAccessControlString(inodeAttrib, path, access, false);
  }

  /**
   * 生成权限拒绝异常的错误信息字符串，支持标识是否由ACL拒绝。
   * @param inodeAttrib 被访问inode的属性
   * @param path 被访问路径
   * @param access 请求的访问权限
   * @param deniedFromAcl 是否由ACL规则拒绝
   * @return 格式化后的异常信息字符串
   */
  private String toAccessControlString(INodeAttributes inodeAttrib,
      String path, FsAction access, boolean deniedFromAcl) {
    StringBuilder sb = new StringBuilder("Permission denied: ")
      .append("user=").append(getUser()).append(", ")
      .append("access=").append(access).append(", ")
      .append("inode=\"").append(path).append("\":")
      .append(inodeAttrib.getUserName()).append(':')
      .append(inodeAttrib.getGroupName()).append(':')
      .append(inodeAttrib.isDirectory() ? 'd' : '-')
      .append(inodeAttrib.getFsPermission());
    if (deniedFromAcl) {
      sb.append("+");
    }
    return sb.toString();
  }

  // 文件系统所有者用户名
  private final String fsOwner;
  // 超级用户组名
  private final String supergroup;
  // 调用者用户组信息
  private final UserGroupInformation callerUgi;

  // 当前用户名
  private final String user;
  // 当前用户所属组集合
  private final Collection<String> groups;
  // 当前用户是否是超级用户
  private final boolean isSuper;
  // inode属性提供者，支持外部扩展权限属性
  private final INodeAttributeProvider attributeProvider;
  // 权限检查执行器，支持外部自定义实现
  private final AccessControlEnforcer accessControlEnforcer;
  // 是否使用带上下文的授权API
  private final boolean authorizeWithContext;
  // 权限检查慢操作告警阈值，单位毫秒
  private final long accessControlEnforcerReportingThresholdMs;

  // 线程本地存储，保存当前操作类型，用于审计日志
  private static ThreadLocal<String> operationType = new ThreadLocal<>();

  /**
   * 构造权限检查器。
   * @param fsOwner 文件系统所有者用户名
   * @param supergroup 超级用户组名
   * @param callerUgi 调用者用户组信息
   * @param attributeProvider inode属性提供者
   */
  protected FSPermissionChecker(String fsOwner, String supergroup,
      UserGroupInformation callerUgi,
      INodeAttributeProvider attributeProvider) {
    this(fsOwner, supergroup, callerUgi, attributeProvider, false, 0);
  }

  /**
   * 构造权限检查器，支持配置是否使用带上下文的授权API和慢操作阈值。
   * @param fsOwner 文件系统所有者用户名
   * @param supergroup 超级用户组名
   * @param callerUgi 调用者用户组信息
   * @param attributeProvider inode属性提供者
   * @param useAuthorizationWithContextAPI 是否使用带上下文的授权API
   * @param accessControlEnforcerReportingThresholdMs 慢操作告警阈值
   */
  protected FSPermissionChecker(String fsOwner, String supergroup,
      UserGroupInformation callerUgi,
      INodeAttributeProvider attributeProvider,
      boolean useAuthorizationWithContextAPI,
      long accessControlEnforcerReportingThresholdMs) {
    this.fsOwner = fsOwner;
    this.supergroup = supergroup;
    this.callerUgi = callerUgi;
    this.groups = callerUgi.getGroupsSet();
    user = callerUgi.getShortUserName();
    isSuper = user.equals(fsOwner) || groups.contains(supergroup);
    this.attributeProvider = attributeProvider;
    this.accessControlEnforcer = initAccessControlEnforcer();

    if (attributeProvider == null) {
      // 没有属性提供者时，使用默认实现，默认支持带上下文授权
      authorizeWithContext = true;
      LOG.debug("Default authorization provider supports the new authorization" +
          " provider API");
    } else {
      authorizeWithContext = useAuthorizationWithContextAPI;
    }
    this.accessControlEnforcerReportingThresholdMs
        = accessControlEnforcerReportingThresholdMs;
  }

  /**
   * 检查外部权限执行器是否执行过慢，生成告警信息。
   * @param elapsedMs 执行耗时
   * @param ace 权限执行器实例
   * @param checkSuperuser 是否检查超级用户权限
   * @param context 授权上下文
   * @return 慢操作告警信息，无则返回null
   */
  private String checkAccessControlEnforcerSlowness(
      long elapsedMs, AccessControlEnforcer ace,
      boolean checkSuperuser, AuthorizationContext context) {
    return checkAccessControlEnforcerSlowness(elapsedMs,
        accessControlEnforcerReportingThresholdMs, ace.getClass(), checkSuperuser,
        context.getPath(), context.getOperationName(),
        context.getCallerContext());
  }

  /**
   * 静态工具方法，检查权限检查是否超时，超时则打告警日志。
   * @param elapsedMs 执行耗时
   * @param thresholdMs 告警阈值
   * @param clazz 权限执行器类
   * @param checkSuperuser 是否检查超级用户权限
   * @param path 被访问路径
   * @param op 操作类型
   * @param caller 调用者信息
   * @return 告警信息，无则返回null
   */
  static String checkAccessControlEnforcerSlowness(
      long elapsedMs, long thresholdMs, Class<? extends AccessControlEnforcer> clazz,
      boolean checkSuperuser, String path, String op, Object caller) {
    if (!LOG.isWarnEnabled()) {
      return null;
    }
    if (thresholdMs <= 0) {
      return null;
    }
    if (elapsedMs > thresholdMs) {
      final String message = clazz + " ran for "
          + elapsedMs + "ms (threshold=" + thresholdMs + "ms) to check "
          + (checkSuperuser ? "superuser" : "permission")
          + " on " + path + " for " + op + " from caller " + caller;
      LOG.warn(message, new Throwable("TRACE"));
      return message;
    }
    return null;
  }

  /**
   * 设置当前线程的操作类型，用于权限审计。
   * @param opType 操作类型名称
   */
  public static void setOperationType(String opType) {
    operationType.set(opType);
  }

  /**
   * 检查当前用户是否属于指定组。
   * @param group 待检查组名
   * @return true表示属于该组，false反之
   */
  public boolean isMemberOfGroup(String group) {
    return groups.contains(group);
  }

  /**
   * 获取当前检查器的用户名。
   * @return 当前用户名
   */
  public String getUser() {
    return user;
  }

  /**
   * 检查当前用户是否是超级用户。
   * @return true表示超级用户，false反之
   */
  public boolean isSuperUser() {
    return isSuper;
  }

  /**
   * 获取属性提供者。
   * @return 属性提供者实例
   */
  public INodeAttributeProvider getAttributesProvider() {
    return attributeProvider;
  }

  /**
   * 权限检查函数式接口，用于包装权限检查逻辑统计耗时。
   */
  @FunctionalInterface
  interface CheckPermission {
    void run() throws AccessControlException;
  }

  /**
   * 执行权限检查并统计执行耗时，返回慢操作告警信息。
   * @param checker 权限检查逻辑
   * @param checkElapsedMs 耗时检查函数
   * @return 慢操作告警信息，无则返回null
   * @throws AccessControlException 权限检查失败抛出
   */
  static String runCheckPermission(CheckPermission checker,
      LongFunction<String> checkElapsedMs) throws AccessControlException {
    final String message;
    final long start = Time.monotonicNow();
    try {
      checker.run();
    } finally {
      final long end = Time.monotonicNow();
      message = checkElapsedMs.apply(end - start);
    }
    return message;
  }

  /**
   * 初始化访问控制执行器，如果有外部提供者则包装并添加慢检查告警。
   * @return 初始化后的访问控制执行器
   */
  private AccessControlEnforcer initAccessControlEnforcer() {
    final AccessControlEnforcer e = Optional.ofNullable(attributeProvider)
        .map(p -> p.getExternalAccessControlEnforcer(this))
        .orElse(this);
    if (e == this) {
      return this;
    }
    // 对外部访问控制执行器添加慢检查告警包装
    return new AccessControlEnforcer() {
      @Override
      public void checkPermission(
          String filesystemOwner, String superGroup, UserGroupInformation ugi,
          INodeAttributes[] inodeAttrs, INode[] inodes, byte[][] pathByNameArr,
          int snapshotId, String path, int ancestorIndex, boolean doCheckOwner,
          FsAction ancestorAccess, FsAction parentAccess, FsAction access,
          FsAction subAccess, boolean ignoreEmptyDir)
          throws AccessControlException {
        runCheckPermission(
            () -> e.checkPermission(filesystemOwner, superGroup, ugi,
                inodeAttrs, inodes, pathByNameArr, snapshotId, path,
                ancestorIndex, doCheckOwner, ancestorAccess, parentAccess,
                access, subAccess, ignoreEmptyDir),
            elapsedMs -> checkAccessControlEnforcerSlowness(elapsedMs,
                accessControlEnforcerReportingThresholdMs,
                e.getClass(), false, path, operationType.get(),
                CallerContext.getCurrent()));
      }

      @Override
      public void checkPermissionWithContext(AuthorizationContext context)
          throws AccessControlException {
        runCheckPermission(
            () -> e.checkPermissionWithContext(context),
            elapsedMs -> checkAccessControlEnforcerSlowness(elapsedMs,
                e, false, context));
      }

      @Override
      public void checkSuperUserPermissionWithContext(
          AuthorizationContext context) throws AccessControlException {
        runCheckPermission(
            () -> e.checkSuperUserPermissionWithContext(context),
            elapsedMs -> checkAccessControlEnforcerSlowness(elapsedMs,
                e, true, context));
      }
    };
  }

  /**
   * 构造超级用户权限检查的授权上下文。
   * @param path 被访问路径
   * @return 构造完成的授权上下文
   */
  private AuthorizationContext getAuthorizationContextForSuperUser(
      String path) {
    String opType = operationType.get();

    AuthorizationContext.Builder builder =
        new INodeAttributeProvider.AuthorizationContext.Builder();
    builder.fsOwner(fsOwner).
        supergroup(supergroup).
        callerUgi(callerUgi).
        operationName(opType).
        callerContext(CallerContext.getCurrent());

    // 非空路径才添加到上下文
    if (path != null && !path.isEmpty()) {
      builder.path(path);
    }

    return builder.build();
  }

  /**
   * 向后兼容的超级用户权限检查方法，不指定路径。
   * @throws AccessControlException 非超级用户抛出
   */
  public void checkSuperuserPrivilege() throws AccessControlException {
    checkSuperuserPrivilege(null);
  }

  /**
   * 检查调用者是否拥有超级用户权限，无则抛出异常。
   * @param path 请求访问的资源路径
   * @throws AccessControlException 非超级用户抛出
   */
  public void checkSuperuserPrivilege(String path)
      throws AccessControlException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("SUPERUSER ACCESS CHECK: " + this
          + ", operationName=" + FSPermissionChecker.operationType.get()
          + ", path=" + path);
    }
    accessControlEnforcer.checkSuperUserPermissionWithContext(
        getAuthorizationContextForSuperUser(path));
  }

  /**
   * 拒绝用户访问，调用外部执行器审计后抛出权限异常。
   * @param path 请求访问的资源路径
   * @param errorMessage 异常错误信息
   * @throws AccessControlException 总是抛出该异常
   */
  public void denyUserAccess(String path, String errorMessage)
      throws AccessControlException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("DENY USER ACCESS: " + this
          + ", operationName=" + FSPermissionChecker.operationType.get()
          + ", path=" + path);
    }
    accessControlEnforcer.denyUserAccess(
        getAuthorizationContextForSuperUser(path), errorMessage);
  }

  /**
   * 检查当前用户对指定路径是否拥有所需访问权限，会逐级检查路径所有祖先的执行权限。
   * 必须持有FSNamesystem的读锁才能调用本方法。
   * @param inodesInPath 路径解析得到的inode数组
   * @param doCheckOwner 是否要求用户必须是路径所有者
   * @param ancestorAccess 路径最近现有祖先目录所需权限
   * @param parentAccess 路径父目录所需权限
   * @param access 路径本身所需权限
   * @param subAccess 如果路径是目录，子目录所需权限
   * @param ignoreEmptyDir 是否忽略空目录的权限检查
   * @throws AccessControlException 权限检查不通过抛出
   */
  void checkPermission(INodesInPath inodesInPath, boolean doCheckOwner,
      FsAction ancestorAccess, FsAction parentAccess, FsAction access,
      FsAction subAccess, boolean ignoreEmptyDir)
      throws AccessControlException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("ACCESS CHECK: " + this
          + ", doCheckOwner=" + doCheckOwner
          + ", ancestorAccess=" + ancestorAccess
          + ", parentAccess=" + parentAccess
          + ", access=" + access
          + ", subAccess=" + subAccess
          + ", ignoreEmptyDir=" + ignoreEmptyDir);
    }
    // 获取快照ID
    final int snapshotId = inodesInPath.getPathSnapshotId();
    // 获取解析后的inode数组
    final IN