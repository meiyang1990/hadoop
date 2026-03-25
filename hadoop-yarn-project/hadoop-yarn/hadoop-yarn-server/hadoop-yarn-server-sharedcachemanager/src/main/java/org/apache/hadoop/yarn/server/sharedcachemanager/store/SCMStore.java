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

package org.apache.hadoop.yarn.server.sharedcachemanager.store;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.sharedcachemanager.AppChecker;

import org.apache.hadoop.classification.VisibleForTesting;


// 共享缓存管理器服务使用的抽象数据存储类。所有方法实现必须是线程安全和原子性的
/**
 * YARN共享缓存管理器使用的数据存储抽象基类，所有实现必须保证线程安全和操作原子性。
 */
@Private
@Evolving
public abstract class SCMStore extends CompositeService {

  protected AppChecker appChecker;

  protected SCMStore(String name) {
    super(name);
  }

  @VisibleForTesting
  SCMStore(String name, AppChecker appChecker) {
    super(name);
    this.appChecker = appChecker;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 未注入AppChecker时通过配置创建
    if (this.appChecker == null) {
      this.appChecker = createAppCheckerService(conf);
    }
    // 添加AppChecker作为子服务
    addService(appChecker);
    super.serviceInit(conf);
  }

  /**
   * 添加资源到共享缓存。资源通过唯一键标识，若键已存在则返回已有资源文件名；若不存在则添加资源、设置访问时间并返回文件名。
   * 
   * @param key 资源唯一标识符
   * @param fileName 资源文件名
   * @return 缓存中存储的资源文件名
   */
  @Private
  public abstract String addResource(String key, String fileName);

  /**
   * 从共享缓存移除资源。
   * 
   * @param key 资源唯一标识符
   * @return 资源已移除/不存在返回true；资源存在且包含至少一个引用无法移除返回false
   */
  @Private
  public abstract boolean removeResource(String key);

  /**
   * 为资源添加引用并更新资源访问时间。
   * 
   * @param key 资源唯一标识符
   * @param ref 要添加的资源引用
   * @return 添加成功/引用已存在返回资源文件名；资源不存在返回null
   */
  @Private
  public abstract String addResourceReference(String key,
      SharedCacheResourceReference ref);

  /**
   * 获取资源关联的所有资源引用。
   * 
   * @param key 资源唯一标识符
   * @return 不可修改的资源引用集合，资源不存在返回空集合
   */
  @Private
  public abstract Collection<SharedCacheResourceReference> getResourceReferences(
      String key);

  /**
   * 从资源移除一个资源引用。
   * 
   * @param key 资源唯一标识符
   * @param ref 要移除的资源引用
   * @param updateAccessTime 是否更新资源访问时间
   * @return 引用移除成功返回true，否则返回false
   */
  @Private
  public abstract boolean removeResourceReference(String key,
      SharedCacheResourceReference ref, boolean updateAccessTime);

  /**
   * 从资源移除一批资源引用。
   * 
   * @param key 资源唯一标识符
   * @param refs 要移除的资源引用集合
   * @param updateAccessTime 是否更新资源访问时间
   */
  @Private
  public abstract void removeResourceReferences(String key,
      Collection<SharedCacheResourceReference> refs, boolean updateAccessTime);

  /**
   * 清理资源中所有指向已结束应用的引用，资源不存在则不做操作。
   *
   * @param key 资源唯一标识符
   * @throws YarnException 清理过程发生异常
   */
  @Private
  public void cleanResourceReferences(String key) throws YarnException {
    // 获取当前资源所有引用
    Collection<SharedCacheResourceReference> refs = getResourceReferences(key);
    if (!refs.isEmpty()) {
      // 收集需要移除的失效引用
      Set<SharedCacheResourceReference> refsToRemove =
          new HashSet<SharedCacheResourceReference>();
      for (SharedCacheResourceReference r : refs) {
        // 应用已不活跃，引用失效需要移除
        if (!appChecker.isApplicationActive(r.getAppId())) {
          refsToRemove.add(r);
        }
      }
      // 批量移除失效引用
      if (refsToRemove.size() > 0) {
        removeResourceReferences(key, refsToRemove, false);
      }
    }
  }

  /**
   * 根据存储配置的缓存淘汰策略判断资源是否可以被淘汰。
   * 
   * @param key 资源唯一标识符
   * @param file 资源在文件系统中的FileStatus对象
   * @return 可淘汰返回true，否则返回false
   */
  @Private
  public abstract boolean isResourceEvictable(String key, FileStatus file);

  /**
   * 根据配置参数通过反射创建AppChecker服务实例。
   * 
   * @param conf 配置对象
   * @return AppChecker服务实例
   */
  @Private
  @SuppressWarnings("unchecked")
  public static AppChecker createAppCheckerService(Configuration conf) {
    Class<? extends AppChecker> defaultCheckerClass;
    try {
      // 加载默认实现类
      defaultCheckerClass =
          (Class<? extends AppChecker>) Class
              .forName(YarnConfiguration.DEFAULT_SCM_APP_CHECKER_CLASS);
    } catch (Exception e) {
      throw new YarnRuntimeException("Invalid default scm app checker class"
          + YarnConfiguration.DEFAULT_SCM_APP_CHECKER_CLASS, e);
    }

    // 根据配置创建实例，使用默认实现作为兜底
    AppChecker checker =
        ReflectionUtils.newInstance(conf.getClass(
            YarnConfiguration.SCM_APP_CHECKER_CLASS, defaultCheckerClass,
            AppChecker.class), conf);
    return checker;
  }
}