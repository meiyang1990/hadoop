// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.LocalizationState;
import org.apache.hadoop.yarn.api.records.LocalizationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 容器请求的所有本地化资源集合，管理资源按本地化状态和可见性的分类存储与状态流转。
 */
public class ResourceSet {

  private static final Logger LOG =
       LoggerFactory.getLogger(ResourceSet.class);

  // 按本地化状态分组存储资源：已完成本地化
  private Map<String, Path> localizedResources =
      new ConcurrentHashMap<>();
  // 按本地化状态分组存储资源：等待本地化
  private Map<LocalResourceRequest, Set<String>> pendingResources =
      new ConcurrentHashMap<>();
  // 按本地化状态分组存储资源：本地化失败
  private final List<LocalizationStatus> resourcesFailedToBeLocalized =
      new ArrayList<>();

  // 按可见性分组存储资源：公共资源
  private final List<LocalResourceRequest> publicRsrcs =
      new ArrayList<>();
  // 按可见性分组存储资源：私有资源
  private final List<LocalResourceRequest> privateRsrcs =
      new ArrayList<>();
  // 按可见性分组存储资源：应用私有资源
  private final List<LocalResourceRequest> appRsrcs =
      new ArrayList<>();

  // 需要上传到共享缓存的资源集合
  private final Map<LocalResourceRequest, Path> resourcesToBeUploaded =
      new ConcurrentHashMap<>();
  // 资源是否需要上传到共享缓存的策略集合
  private final Map<LocalResourceRequest, Boolean> resourcesUploadPolicies =
      new ConcurrentHashMap<>();

  /**
   * 添加容器申请的本地资源，按可见性分类并加入等待本地化队列。
   * @param localResourceMap 资源ID到本地资源定义的映射
   * @return 按可见性分组的资源请求集合，输入为空时返回null
   * @throws URISyntaxException 资源路径语法错误时抛出
   */
  public Map<LocalResourceVisibility, Collection<LocalResourceRequest>>
      addResources(Map<String, LocalResource> localResourceMap)
      throws URISyntaxException {
    if (localResourceMap == null || localResourceMap.isEmpty()) {
      return null;
    }
    Map<LocalResourceRequest, Set<String>> allResources = new HashMap<>();
    List<LocalResourceRequest> publicList = new ArrayList<>();
    List<LocalResourceRequest> privateList = new ArrayList<>();
    List<LocalResourceRequest> appList = new ArrayList<>();

    // 遍历所有输入资源，封装请求并按可见性分类
    for (Map.Entry<String, LocalResource> rsrc : localResourceMap.entrySet()) {
      LocalResource resource = rsrc.getValue();
      LocalResourceRequest req = new LocalResourceRequest(rsrc.getValue());
      allResources.putIfAbsent(req, new HashSet<>());
      allResources.get(req).add(rsrc.getKey());
      // 存储共享缓存上传策略
      storeSharedCacheUploadPolicy(req,
          resource.getShouldBeUploadedToSharedCache());
      // 按可见性分组
      switch (resource.getVisibility()) {
      case PUBLIC:
        publicList.add(req);
        break;
      case PRIVATE:
        privateList.add(req);
        break;
      case APPLICATION:
        appList.add(req);
        break;
      default:
        break;
      }
    }
    Map<LocalResourceVisibility, Collection<LocalResourceRequest>> req =
        new LinkedHashMap<>();
    // 将分类后的资源添加到全局集合，并返回待本地化请求
    if (!publicList.isEmpty()) {
      publicRsrcs.addAll(publicList);
      req.put(LocalResourceVisibility.PUBLIC, publicList);
    }
    if (!privateList.isEmpty()) {
      privateRsrcs.addAll(privateList);
      req.put(LocalResourceVisibility.PRIVATE, privateList);
    }
    if (!appList.isEmpty()) {
      appRsrcs.addAll(appList);
      req.put(LocalResourceVisibility.APPLICATION, appList);
    }
    if (!allResources.isEmpty()) {
      this.pendingResources.putAll(allResources);
    }
    return req;
  }

  /**
   * 资源本地化完成后的处理：移出等待队列，添加到已完成集合。
   * @param request 本地化资源请求
   * @param location 本地化完成后的资源路径
   * @return 需要创建的软链接列表，资源不存在时返回null
   */
  public Set<String> resourceLocalized(LocalResourceRequest request,
      Path location) {
    Set<String> symlinks = pendingResources.remove(request);
    if (symlinks == null) {
      return null;
    } else {
      for (String symlink : symlinks) {
        localizedResources.put(symlink, location);
      }
      return symlinks;
    }
  }

  /**
   * 处理资源本地化失败，记录失败状态到失败集合。
   * @param request 本地化失败的资源请求
   * @param diagnostics 失败诊断信息
   */
  public void resourceLocalizationFailed(LocalResourceRequest request,
      String diagnostics) {
    // 运行中容器本地化失败时请求可能为null，直接跳过
    if (request == null) {
      return;
    }
    Set<String> keys = pendingResources.remove(request);
    if (keys != null) {
      synchronized (resourcesFailedToBeLocalized) {
        keys.forEach(key ->
            resourcesFailedToBeLocalized.add(LocalizationStatus.newInstance(key,
                LocalizationState.FAILED, diagnostics)));
      }
    }
  }

  /**
   * 获取按可见性分组的所有资源请求集合。
   * @return 可见性到对应资源请求集合的映射
   */
  public synchronized Map<LocalResourceVisibility,
      Collection<LocalResourceRequest>> getAllResourcesByVisibility() {

    Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrc =
        new HashMap<>();
    if (!publicRsrcs.isEmpty()) {
      rsrc.put(LocalResourceVisibility.PUBLIC, publicRsrcs);
    }
    if (!privateRsrcs.isEmpty()) {
      rsrc.put(LocalResourceVisibility.PRIVATE, privateRsrcs);
    }
    if (!appRsrcs.isEmpty()) {
      rsrc.put(LocalResourceVisibility.APPLICATION, appRsrcs);
    }
    return rsrc;
  }

  /**
   * Store the resource's shared cache upload policies
   * Given LocalResourceRequest can be shared across containers in
   * LocalResourcesTrackerImpl, we preserve the upload policies here.
   * In addition, it is possible for the application to create several
   * "identical" LocalResources as part of
   * ContainerLaunchContext.setLocalResources with different symlinks.
   * There is a corner case where these "identical" local resources have
   * different upload policies. For that scenario, upload policy will be set to
   * true as long as there is at least one LocalResource entry with
   * upload policy set to true.
   */
  private void storeSharedCacheUploadPolicy(
      LocalResourceRequest resourceRequest, Boolean uploadPolicy) {
    Boolean storedUploadPolicy = resourcesUploadPolicies.get(resourceRequest);
    // 策略合并规则：只要存在一个请求要求上传，就设置为需要上传
    if (storedUploadPolicy == null || (!storedUploadPolicy && uploadPolicy)) {
      resourcesUploadPolicies.put(resourceRequest, uploadPolicy);
    }
  }

  /**
   * 获取已完成本地化的资源，按资源路径分组软链接。
   * @return 本地化路径到对应软链接列表的映射
   */
  public Map<Path, List<String>> getLocalizedResources() {
    Map<Path, List<String>> map = new HashMap<>();
    for (Map.Entry<String, Path> entry : localizedResources.entrySet()) {
      map.putIfAbsent(entry.getValue(), new ArrayList<>());
      map.get(entry.getValue()).add(entry.getKey());
    }
    return map;
  }

  public Map<LocalResourceRequest, Path> getResourcesToBeUploaded() {
    return resourcesToBeUploaded;
  }

  public Map<LocalResourceRequest, Boolean> getResourcesUploadPolicies() {
    return resourcesUploadPolicies;
  }

  public Map<LocalResourceRequest, Set<String>> getPendingResources() {
    return pendingResources;
  }

  /**
   * 合并多个ResourceSet为一个新的ResourceSet。
   * @param resourceSets 待合并的ResourceSet数组
   * @return 合并后的新ResourceSet
   */
  public static ResourceSet merge(ResourceSet... resourceSets) {
    ResourceSet merged = new ResourceSet();
    for (ResourceSet rs : resourceSets) {
      // 相同软链接会覆盖已有条目
      merged.localizedResources.putAll(rs.localizedResources);

      merged.resourcesToBeUploaded.putAll(rs.resourcesToBeUploaded);
      merged.resourcesUploadPolicies.putAll(rs.resourcesUploadPolicies);

      // TODO : START : Should we de-dup here ?
      merged.publicRsrcs.addAll(rs.publicRsrcs);
      merged.privateRsrcs.addAll(rs.privateRsrcs);
      merged.appRsrcs.addAll(rs.appRsrcs);
      // TODO : END
    }
    return merged;
  }

  /**
   * 获取所有资源的本地化状态列表。
   * @return 所有资源本地化状态集合，包含完成、等待、失败三种状态
   */
  public List<LocalizationStatus> getLocalizationStatuses() {
    List<LocalizationStatus> statuses = new ArrayList<>();
    // 添加已完成本地化的资源状态
    localizedResources.forEach((key, path) -> {
      LocalizationStatus status = LocalizationStatus.newInstance(key,
          LocalizationState.COMPLETED);
      statuses.add(status);
    });

    // 添加等待本地化的资源状态
    pendingResources.forEach((lrReq, keys) ->
        keys.forEach(key -> {
          LocalizationStatus status = LocalizationStatus.newInstance(key,
              LocalizationState.PENDING);
          statuses.add(status);
        }));

    // 添加本地化失败的资源状态
    synchronized (resourcesFailedToBeLocalized) {
      statuses.addAll(resourcesFailedToBeLocalized);
    }
    return statuses;
  }

}