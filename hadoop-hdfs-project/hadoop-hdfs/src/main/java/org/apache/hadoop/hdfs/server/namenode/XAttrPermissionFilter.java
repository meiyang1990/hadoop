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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.util.Preconditions;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.SECURITY_XATTR_UNREADABLE_BY_SUPERUSER;

/**
 * 文件扩展属性(XAttr)权限检查与过滤工具，负责验证用户API调用时的XAttr访问权限。
 * 
 * HDFS定义了五种不同命名空间的扩展属性，不同命名空间的XAttr有不同的访问权限规则：
 * <br>
 * USER - 用户扩展属性：可由用户附加在文件/目录上存储自定义信息，权限由文件权限位控制
 * <br>
 * TRUSTED - 可信扩展属性：仅超级用户可访问修改
 * <br>
 * SECURITY - 安全扩展属性：供HDFS核心内部使用，不开放给用户/管理员API访问
 * <br>
 * SYSTEM - 系统扩展属性：供HDFS核心内部使用，不开放给用户/管理员API访问
 * <br>
 * RAW - 原始系统扩展属性：仅当访问/.reserved/raw路径下才可被拥有读权限的用户访问
 * <br>
 * 该类负责基于上述规则的权限检查与结果过滤，确保只有符合权限要求的XAttr可以被访问。
 */
@InterfaceAudience.Private
public class XAttrPermissionFilter {
  
  /**
   * 检查单个XAttr通过用户API访问的权限，无权限则抛出访问控制异常
   * @param pc 权限检查器，包含当前用户信息与权限上下文
   * @param xAttr 待检查的扩展属性
   * @param isRawPath 当前访问路径是否为/.reserved/raw原生路径
   * @throws AccessControlException 权限不足时抛出异常
   */
  static void checkPermissionForApi(FSPermissionChecker pc, XAttr xAttr,
      boolean isRawPath)
      throws AccessControlException {
    final boolean isSuperUser = pc.isSuperUser();
    final String xAttrString =
        "XAttr [ns=" + xAttr.getNameSpace() + ", name=" + xAttr.getName() + "]";
    // 允许USER命名空间对所有用户开放，TRUSTED命名空间仅对超级用户开放
    if (xAttr.getNameSpace() == XAttr.NameSpace.USER || 
        (xAttr.getNameSpace() == XAttr.NameSpace.TRUSTED && isSuperUser)) {
      if (isSuperUser) {
        // 调用权限检查器记录超级用户操作，用于审计
        pc.checkSuperuserPrivilege(xAttrString);
      }
      return;
    }
    // RAW命名空间在/.reserved/raw路径下允许访问
    if (xAttr.getNameSpace() == XAttr.NameSpace.RAW && isRawPath) {
      return;
    }
    // 特殊安全XATTR：安全命名空间中不可被超级用户读取的扩展属性
    if (XAttrHelper.getPrefixedName(xAttr).
        equals(SECURITY_XATTR_UNREADABLE_BY_SUPERUSER)) {
      // 该XATTR不允许设置值，禁止操作
      if (xAttr.getValue() != null) {
        // 拒绝访问并记录审计日志
        String errorMessage = "Attempt to set a value for '" +
            SECURITY_XATTR_UNREADABLE_BY_SUPERUSER +
            "'. Values are not allowed for this xattr.";
        pc.denyUserAccess(xAttrString, errorMessage);
      }
      return;
    }
    // 不符合任何允许规则，拒绝访问并记录审计日志
    pc.denyUserAccess(xAttrString, "User doesn't have permission for xattr: "
            + XAttrHelper.getPrefixedName(xAttr));
  }

  /**
   * 批量检查多个XAttr通过用户API访问的权限，任意一个无权限则抛出异常
   * @param pc 权限检查器，包含当前用户信息与权限上下文
   * @param xAttrs 待检查的扩展属性列表
   * @param isRawPath 当前访问路径是否为/.reserved/raw原生路径
   * @throws AccessControlException 任意一个XAttr权限不足时抛出异常
   */
  static void checkPermissionForApi(FSPermissionChecker pc,
      List<XAttr> xAttrs, boolean isRawPath) throws AccessControlException {
    Preconditions.checkArgument(xAttrs != null);
    if (xAttrs.isEmpty()) {
      return;
    }

    for (XAttr xAttr : xAttrs) {
      checkPermissionForApi(pc, xAttr, isRawPath);
    }
  }

  /**
   * 过滤出当前用户有权限访问的XAttr列表，只返回符合权限规则的XAttr
   * @param pc 权限检查器，包含当前用户信息与权限上下文
   * @param xAttrs 待过滤的扩展属性列表
   * @param isRawPath 当前访问路径是否为/.reserved/raw原生路径
   * @return 过滤后仅包含当前用户有权限访问的XAttr列表
   */
  static List<XAttr> filterXAttrsForApi(FSPermissionChecker pc,
      List<XAttr> xAttrs, boolean isRawPath) {
    assert xAttrs != null : "xAttrs can not be null";
    if (xAttrs.isEmpty()) {
      return xAttrs;
    }
    
    List<XAttr> filteredXAttrs = Lists.newArrayListWithCapacity(xAttrs.size());
    final boolean isSuperUser = pc.isSuperUser();
    for (XAttr xAttr : xAttrs) {
      if (xAttr.getNameSpace() == XAttr.NameSpace.USER) {
        filteredXAttrs.add(xAttr);
      } else if (xAttr.getNameSpace() == XAttr.NameSpace.TRUSTED && 
          isSuperUser) {
        filteredXAttrs.add(xAttr);
      } else if (xAttr.getNameSpace() == XAttr.NameSpace.RAW && isRawPath) {
        filteredXAttrs.add(xAttr);
      } else if (XAttrHelper.getPrefixedName(xAttr).
          equals(SECURITY_XATTR_UNREADABLE_BY_SUPERUSER)) {
        filteredXAttrs.add(xAttr);
      }
    }
    return filteredXAttrs;
  }
}