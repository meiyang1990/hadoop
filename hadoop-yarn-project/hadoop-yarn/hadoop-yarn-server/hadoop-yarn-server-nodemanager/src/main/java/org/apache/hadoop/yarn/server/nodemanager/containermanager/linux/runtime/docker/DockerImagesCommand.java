// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import org.apache.hadoop.util.Preconditions;

/**
 * Docker images命令封装类，封装了docker列出镜像命令及其命令行参数。
 * 用于YARN NodeManager执行Docker镜像查询操作，对应docker images命令。
 */
public class DockerImagesCommand extends DockerCommand {
  private static final String IMAGES_COMMAND = "images";

  /**
   * 构造Docker images命令对象。
   */
  public DockerImagesCommand() {
    super(IMAGES_COMMAND);
  }

  /**
   * 添加查询单个镜像状态的参数，仅获取指定镜像的信息。
   * @param imageName 要查询的镜像名称
   * @return 当前命令对象，支持链式调用
   */
  public DockerImagesCommand getSingleImageStatus(String imageName) {
    Preconditions.checkNotNull(imageName, "imageName");
    super.addCommandArguments("image", imageName);
    return this;
  }
}