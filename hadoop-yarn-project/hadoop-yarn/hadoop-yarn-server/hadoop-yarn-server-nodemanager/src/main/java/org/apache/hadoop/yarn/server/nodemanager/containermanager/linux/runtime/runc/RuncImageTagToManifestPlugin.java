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

import java.io.IOException;

/**
 * Runc容器运行时的镜像标签转清单插件接口，属于YARN NodeManager runC容器运行时扩展模块。
 * 负责将OCI镜像标签转换为对应的镜像清单信息，支持runc容器按需拉取指定镜像。
 * 实现类可通过扩展该接口接入不同的镜像仓库服务，无需修改核心runc运行逻辑。
 */
@InterfaceStability.Unstable
public interface RuncImageTagToManifestPlugin extends Service {
  /**
   * 根据镜像标签获取对应的OCI镜像清单信息。
   * @param imageTag 镜像标签，通常为镜像名称加版本号，如nginx:latest
   * @return 解析后的OCI镜像清单对象
   * @throws IOException 当镜像拉取或解析失败时抛出异常
   */
  ImageManifest getManifestFromImageTag(String imageTag) throws IOException;

  /**
   * 根据镜像标签获取对应的镜像内容哈希值。
   * @param imageTag 镜像标签，通常为镜像名称加版本号，如nginx:latest
   * @return 镜像内容的哈希字符串，用于校验镜像完整性
   */
  String getHashFromImageTag(String imageTag);
}