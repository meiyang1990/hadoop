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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * YARN Web REST API 数据传输对象，用于封装远程日志目录路径查询结果列表
 * 存储批量RemoteLogPathEntry，用于序列化返回给前端调用方
 */
@XmlRootElement(name = "remoteLogDirPathResult")
@XmlAccessorType(XmlAccessType.FIELD)
public class RemoteLogPaths {

  @XmlElement(name = "paths")
  private List<RemoteLogPathEntry> paths;

  //JAXB needs this
  public RemoteLogPaths() {}

  /**
   * 构造方法，传入预填充的远程日志路径条目列表
   * @param paths 远程日志路径条目列表
   */
  public RemoteLogPaths(List<RemoteLogPathEntry> paths) {
    this.paths = paths;
  }

  public List<RemoteLogPathEntry> getPaths() {
    return paths;
  }

  public void setPaths(List<RemoteLogPathEntry> paths) {
    this.paths = paths;
  }
}