// 这个文件已经全部加上中文注释
/*
 *
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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.LocalResource;

import java.io.IOException;
import java.util.List;

/**
 * 文件级说明：Runc容器运行时OCI镜像清单转YARN本地资源的扩展插件接口
 * 
 * This class is a plugin interface for the
 * {@link org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.RuncContainerRuntime}
 * that maps OCI Image Manifests into associated config and layers.
 * 核心职责：将OCI镜像清单转换为YARN容器可以使用的本地资源（配置和镜像层）
 */
@InterfaceStability.Unstable
public interface RuncManifestToResourcesPlugin extends Service {
  /**
   * 获取镜像层对应的本地资源列表
   * @param manifest OCI镜像清单对象
   * @return 按清单中原有顺序排列的镜像层本地资源列表
   * @throws IOException 资源获取失败时抛出异常
   */
  //The layers should be returned in the order in which they
  // appear in the manifest
  List<LocalResource> getLayerResources(ImageManifest manifest)
      throws IOException;

  /**
   * 获取镜像配置对应的本地资源
   * @param manifest OCI镜像清单对象
   * @return 镜像配置本地资源
   * @throws IOException 资源获取失败时抛出异常
   */
  LocalResource getConfigResource(ImageManifest manifest) throws IOException;
}