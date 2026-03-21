// 这个文件已经全部加上中文注释
/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.executor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 封装以指定用户身份执行删除操作所需的上下文信息，用于NodeManager的删除任务执行
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class DeletionAsUserContext {
  private final String user;
  private final Path subDir;
  private final List<Path> basedirs;

  /**
   * DeletionAsUserContext的构建器，采用Builder模式构造上下文对象
   */
  public static final class Builder {
    private String user;
    private Path subDir;
    private List<Path> basedirs;

    public Builder() {
    }

    /**
     * 设置执行删除操作的用户名
     * @param user 用户名
     * @return 当前Builder实例
     */
    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    /**
     * 设置需要删除的子目录路径
     * @param subDir 子目录路径
     * @return 当前Builder实例
     */
    public Builder setSubDir(Path subDir) {
      this.subDir = subDir;
      return this;
    }

    /**
     * 设置删除操作的基础目录列表，删除操作将在这些基础目录下执行
     * @param basedirs 基础目录数组
     * @return 当前Builder实例
     */
    public Builder setBasedirs(Path... basedirs) {
      this.basedirs = Arrays.asList(basedirs);
      return this;
    }

    /**
     * 构造最终的DeletionAsUserContext上下文对象
     * @return 构造完成的上下文对象
     */
    public DeletionAsUserContext build() {
      return new DeletionAsUserContext(this);
    }
  }

  private DeletionAsUserContext(Builder builder) {
    this.user = builder.user;
    this.subDir = builder.subDir;
    this.basedirs = builder.basedirs;
  }

  /**
   * 获取执行删除操作的用户名
   * @return 用户名
   */
  public String getUser() {
    return this.user;
  }

  /**
   * 获取需要删除的子目录路径
   * @return 子目录路径
   */
  public Path getSubDir() {
    return this.subDir;
  }

  /**
   * 获取不可修改的基础目录列表，删除操作将在这些目录下执行
   * @return 不可修改的基础目录列表，如果未设置则返回null
   */
  public List<Path> getBasedirs() {
    if (this.basedirs != null) {
      return Collections.unmodifiableList(this.basedirs);
    } else {
      return null;
    }
  }
}