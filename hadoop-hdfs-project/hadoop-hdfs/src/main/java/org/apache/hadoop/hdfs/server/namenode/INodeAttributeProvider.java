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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Arrays;

/**
 * HDFS INode属性与访问控制扩展提供者抽象基类，
 * 允许第三方实现自定义INode属性获取和权限访问控制逻辑，
 * 用于扩展NameNode原生的权限检查和属性管理能力。
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class INodeAttributeProvider {

  /**
   * 权限授权上下文类，封装一次权限检查所需的全部上下文信息，
   * 替代原有长参数列表，提供更清晰的参数传递方式。
   */
  public static class AuthorizationContext {
    private String fsOwner;
    private String supergroup;
    private UserGroupInformation callerUgi;
    private INodeAttributes[] inodeAttrs;
    private INode[] inodes;
    private byte[][] pathByNameArr;
    private int snapshotId;
    private String path;
    private int ancestorIndex;
    private boolean doCheckOwner;
    private FsAction ancestorAccess;
    private FsAction parentAccess;
    private FsAction access;
    private FsAction subAccess;
    private boolean ignoreEmptyDir;
    private String operationName;
    private CallerContext callerContext;

    /** 获取文件系统所有者用户名 */
    public String getFsOwner() {
      return fsOwner;
    }

    public void setFsOwner(String fsOwner) {
      this.fsOwner = fsOwner;
    }

    /** 获取超级用户组名称 */
    public String getSupergroup() {
      return supergroup;
    }

    public void setSupergroup(String supergroup) {
      this.supergroup = supergroup;
    }

    /** 获取调用者用户组信息 */
    public UserGroupInformation getCallerUgi() {
      return callerUgi;
    }

    public void setCallerUgi(UserGroupInformation callerUgi) {
      this.callerUgi = callerUgi;
    }

    /** 获取路径各层级INode属性数组 */
    public INodeAttributes[] getInodeAttrs() {
      return inodeAttrs;
    }

    public void setInodeAttrs(INodeAttributes[] inodeAttrs) {
      this.inodeAttrs = inodeAttrs;
    }

    /** 获取路径各层级INode对象数组 */
    public INode[] getInodes() {
      return inodes;
    }

    public void setInodes(INode[] inodes) {
      this.inodes = inodes;
    }

    /** 获取路径各层级名称字节数组 */
    public byte[][] getPathByNameArr() {
      return pathByNameArr;
    }

    public void setPathByNameArr(byte[][] pathByNameArr) {
      this.pathByNameArr = pathByNameArr;
    }

    /** 获取快照ID */
    public int getSnapshotId() {
      return snapshotId;
    }

    public void setSnapshotId(int snapshotId) {
      this.snapshotId = snapshotId;
    }

    /** 获取请求路径字符串 */
    public String getPath() {
      return path;
    }

    public void setPath(String path) {
      this.path = path;
    }

    /** 获取祖先节点索引 */
    public int getAncestorIndex() {
      return ancestorIndex;
    }

    public void setAncestorIndex(int ancestorIndex) {
      this.ancestorIndex = ancestorIndex;
    }

    /** 是否需要检查所有者权限 */
    public boolean isDoCheckOwner() {
      return doCheckOwner;
    }

    public void setDoCheckOwner(boolean doCheckOwner) {
      this.doCheckOwner = doCheckOwner;
    }

    /** 获取祖先节点所需访问权限 */
    public FsAction getAncestorAccess() {
      return ancestorAccess;
    }

    public void setAncestorAccess(FsAction ancestorAccess) {
      this.ancestorAccess = ancestorAccess;
    }

    /** 获取父节点所需访问权限 */
    public FsAction getParentAccess() {
      return parentAccess;
    }

    public void setParentAccess(FsAction parentAccess) {
      this.parentAccess = parentAccess;
    }

    /** 获取当前节点所需访问权限 */
    public FsAction getAccess() {
      return access;
    }

    public void setAccess(FsAction access) {
      this.access = access;
    }

    /** 获取子节点所需访问权限 */
    public FsAction getSubAccess() {
      return subAccess;
    }

    public void setSubAccess(FsAction subAccess) {
      this.subAccess = subAccess;
    }

    /** 是否忽略空目录权限检查 */
    public boolean isIgnoreEmptyDir() {
      return ignoreEmptyDir;
    }

    public void setIgnoreEmptyDir(boolean ignoreEmptyDir) {
      this.ignoreEmptyDir = ignoreEmptyDir;
    }

    /** 获取当前操作名称 */
    public String getOperationName() {
      return operationName;
    }

    public void setOperationName(String operationName) {
      this.operationName = operationName;
    }

    /** 获取调用者上下文信息 */
    public CallerContext getCallerContext() {
      return callerContext;
    }

    public void setCallerContext(CallerContext callerContext) {
      this.callerContext = callerContext;
    }

    /**
     * AuthorizationContext建造器类，用于构造AuthorizationContext对象
     */
    public static class Builder {
      private String fsOwner;
      private String supergroup;
      private UserGroupInformation callerUgi;
      private INodeAttributes[] inodeAttrs;
      private INode[] inodes;
      private byte[][] pathByNameArr;
      private int snapshotId;
      private String path;
      private int ancestorIndex;
      private boolean doCheckOwner;
      private FsAction ancestorAccess;
      private FsAction parentAccess;
      private FsAction access;
      private FsAction subAccess;
      private boolean ignoreEmptyDir;
      private String operationName;
      private CallerContext callerContext;

      /**
       * 构造完整的AuthorizationContext对象
       * @return 构造完成的授权上下文对象
       */
      public AuthorizationContext build() {
        return new AuthorizationContext(this);
      }

      public Builder fsOwner(String val) {
        this.fsOwner = val;
        return this;
      }

      public Builder supergroup(String val) {
        this.supergroup = val;
        return this;
      }

      public Builder callerUgi(UserGroupInformation val) {
        this.callerUgi = val;
        return this;
      }

      public Builder inodeAttrs(INodeAttributes[] val) {
        this.inodeAttrs = val;
        return this;
      }

      public Builder inodes(INode[] val) {
        this.inodes = val;
        return this;
      }

      public Builder pathByNameArr(byte[][] val) {
        this.pathByNameArr = val;
        return this;
      }

      public Builder snapshotId(int val) {
        this.snapshotId = val;
        return this;
      }

      public Builder path(String val) {
        this.path = val;
        return this;
      }

      public Builder ancestorIndex(int val) {
        this.ancestorIndex = val;
        return this;
      }

      public Builder doCheckOwner(boolean val) {
        this.doCheckOwner = val;
        return this;
      }

      public Builder ancestorAccess(FsAction val) {
        this.ancestorAccess = val;
        return this;
      }

      public Builder parentAccess(FsAction val) {
        this.parentAccess = val;
        return this;
      }

      public Builder access(FsAction val) {
        this.access = val;
        return this;
      }

      public Builder subAccess(FsAction val) {
        this.subAccess = val;
        return this;
      }

      public Builder ignoreEmptyDir(boolean val) {
        this.ignoreEmptyDir = val;
        return this;
      }

      public Builder operationName(String val) {
        this.operationName = val;
        return this;
      }

      public Builder callerContext(CallerContext val) {
        this.callerContext = val;
        return this;
      }
    }

    /**
     * 通过Builder构造AuthorizationContext对象
     * @param builder 建造器对象
     */
    public AuthorizationContext(Builder builder) {
      this.setFsOwner(builder.fsOwner);
      this.setSupergroup(builder.supergroup);
      this.setCallerUgi(builder.callerUgi);
      this.setInodeAttrs(builder.inodeAttrs);
      this.setInodes(builder.inodes);
      this.setPathByNameArr(builder.pathByNameArr);
      this.setSnapshotId(builder.snapshotId);
      this.setPath(builder.path);
      this.setAncestorIndex(builder.ancestorIndex);
      this.setDoCheckOwner(builder.doCheckOwner);
      this.setAncestorAccess(builder.ancestorAccess);
      this.setParentAccess(builder.parentAccess);
      this.setAccess(builder.access);
      this.setSubAccess(builder.subAccess);
      this.setIgnoreEmptyDir(builder.ignoreEmptyDir);
      this.setOperationName(builder.operationName);
      this.setCallerContext(builder.callerContext);
    }

    @VisibleForTesting
    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      AuthorizationContext other = (AuthorizationContext)obj;
      return getFsOwner().equals(other.getFsOwner()) &&
          getSupergroup().equals(other.getSupergroup()) &&
          getCallerUgi().equals(other.getCallerUgi()) &&
          Arrays.deepEquals(getInodeAttrs(), other.getInodeAttrs()) &&
          Arrays.deepEquals(getInodes(), other.getInodes()) &&
          Arrays.deepEquals(getPathByNameArr(), other.getPathByNameArr()) &&
          getSnapshotId() == other.getSnapshotId() &&
          getPath().equals(other.getPath()) &&
          getAncestorIndex() == other.getAncestorIndex() &&
          isDoCheckOwner() == other.isDoCheckOwner() &&
          getAncestorAccess() == other.getAncestorAccess() &&
          getParentAccess() == other.getParentAccess() &&
          getAccess() == other.getAccess() &&
          getSubAccess() == other.getSubAccess() &&
          isIgnoreEmptyDir() == other.isIgnoreEmptyDir();
    }

    @Override
    public int hashCode() {
      assert false : "hashCode not designed";
      return 42; // any arbitrary constant will do
    }
  }

  /**
   * 访问控制强制检查器接口，允许自定义实现替代HDFS默认的权限检查逻辑
   */
  public interface AccessControlEnforcer {

    /**
     * 检查文件系统对象的访问权限，权限不满足时抛出异常，该方法已废弃
     * @param fsOwner 文件系统所有者（NameNode用户）
     * @param supergroup 超级用户组
     * @param callerUgi 调用者用户组信息
     * @param inodeAttrs 路径各层级INode属性数组
     * @param inodes 路径各层级INode对象数组
     * @param pathByNameArr 路径各层级名称字节数组
     * @param snapshotId 请求路径对应的快照ID
     * @param path 请求路径字符串
     * @param ancestorIndex 祖先节点索引
     * @param doCheckOwner 是否需要检查所有权
     * @param ancestorAccess 祖先节点所需访问权限
     * @param parentAccess 父节点所需访问权限
     * @param access 当前节点所需访问权限
     * @param subAccess 当前节点是目录时，子目录所需访问权限
     * @param ignoreEmptyDir 是否忽略空目录权限检查
     * @throws AccessControlException 权限检查不通过时抛出
     * @deprecated use{@link #checkPermissionWithContext(AuthorizationContext)}} instead
     */
    public abstract void checkPermission(String fsOwner, String supergroup,
        UserGroupInformation callerUgi, INodeAttributes[] inodeAttrs,
        INode[] inodes, byte[][] pathByNameArr, int snapshotId, String path,
        int ancestorIndex, boolean doCheckOwner, FsAction ancestorAccess,
        FsAction parentAccess, FsAction access, FsAction subAccess,
        boolean ignoreEmptyDir)
            throws AccessControlException;

    /**
     * 检查文件系统对象的访问权限，权限不满足时抛出异常
     * @param authzContext 封装了所有授权所需参数的上下文对象
     * @throws AccessControlException 权限检查不通过时抛出
     */
    default void checkPermissionWithContext(AuthorizationContext authzContext)
        throws AccessControlException {
      throw new AccessControlException("The authorization provider does not "
          + "implement the checkPermissionWithContext(AuthorizationContext) "
          + "API.");
    }

    /**
     * 检查调用者是否拥有超级用户权限，不满足则抛出异常
     * @param authzContext 封装了所有授权所需参数的上下文对象
     * @throws AccessControlException 调用者不是超级用户时抛出
     */
    default void checkSuperUserPermissionWithContext(
        AuthorizationContext authzContext)
        throws AccessControlException {
      UserGroupInformation callerUgi = authzContext.getCallerUgi();
      boolean isSuperUser =
          callerUgi.getShortUserName().equals(authzContext.getFsOwner()) ||
          callerUgi.getGroupsSet().contains(authzContext.getSupergroup());
      if (!isSuperUser) {
        throw new AccessControlException("Access denied for user " +
            callerUgi.getShortUserName() + ". Superuser privilege is " +
            "required for operation " + authzContext.getOperationName());
      }
    }

    /**
     * 当拒绝用户访问时调用该方法，用于外部强制检查器记录审计日志
     * @param authzContext 封装了所有授权所需参数的上下文对象
     * @param errorMessage 错误信息
     * @throws AccessControlException 抛出访问控制异常拒绝请求
     */
    default void denyUserAccess(AuthorizationContext authzContext,
                                String errorMessage)
        throws AccessControlException {
      throw new AccessControlException(errorMessage);
    }
  }

  /**
   * 初始化提供者，在NameNode启动时调用
   */
  public abstract void start();

  /**
   * 关闭提供者，在NameNode关闭时调用
   */
  public abstract void stop();

  @Deprecated
  String[] getPathElements(String path) {
    path = path.trim();
    if (path.charAt(0) != Path.SEPARATOR_CHAR) {
      throw new IllegalArgumentException("It must be an absolute path: " +
          path);
    }
    int numOfElements = StringUtils.countMatches(path, Path.SEPARATOR);
    if (path.length() > 1 && path.endsWith(Path.SEPARATOR)) {
      numOfElements--;
    }
    String[] pathElements = new String[numOfElements];
    int elementIdx = 0;
    int idx = 0;
    int found = path.indexOf(Path.SEPARATOR_CHAR, idx);
    while (found > -1) {
      if (found > idx) {
        pathElements[elementIdx++] = path.substring(idx, found);
      }
      idx = found + 1;
      found = path.indexOf(Path.SEPARATOR_CHAR, idx);
    }
    if (idx < path.length()) {
      pathElements[elementIdx] = path.substring(idx);
    }
    return pathElements;
  }

  @Deprecated
  public INodeAttributes getAttributes(String fullPath, INodeAttributes inode) {
    return getAttributes(getPathElements(fullPath), inode);
  }

  /**
   * 获取INode的自定义属性，由实现类提供自定义逻辑
   * @param pathElements 路径各层级元素数组
   * @param inode 原始INode属性
   * @return 自定义INode属性
   */
  public abstract INodeAttributes getAttributes(String[] pathElements,
      INodeAttributes inode);

  /**
   * 获取INode的自定义属性，将字节数组路径转换为字符串