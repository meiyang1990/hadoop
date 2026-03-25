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
package org.apache.hadoop.yarn.server.webapp.dao;

/**
 * 日志聚合远程日志路径信息实体，用于在Web UI中传递日志文件控制器对应的远程路径信息
 * <p>
 * 存储格式为：
 * <pre>
 *   {@code <ROOT_PATH>/%USER/<SUFFIX>}
 * </pre>
 * </p>
 */
public class RemoteLogPathEntry {
  private String fileController;
  private String path;

  // JAXB序列化/反序列化需要无参构造函数
  public RemoteLogPathEntry() {}

  /**
   * 构造远程日志路径实体
   * @param fileController 日志文件控制器名称
   * @param path 远程日志根路径
   */
  public RemoteLogPathEntry(String fileController, String path) {
    this.fileController = fileController;
    this.path = path;
  }

  public String getFileController() {
    return fileController;
  }

  public void setFileController(String fileController) {
    this.fileController = fileController;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }
}