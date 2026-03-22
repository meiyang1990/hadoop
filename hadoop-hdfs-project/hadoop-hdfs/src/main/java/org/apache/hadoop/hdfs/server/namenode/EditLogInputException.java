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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 编辑日志加载异常类，当NameNode加载 edits 日志时从磁盘读取编辑日志操作失败抛出此异常
 * 保存了加载失败前已经成功加载的编辑日志数量，便于问题排查
 */
@InterfaceAudience.Private
public class EditLogInputException extends IOException {

  private static final long serialVersionUID = 1L;
  
  /** 异常抛出前已经成功加载的编辑日志操作数量 */
  private final long numEditsLoaded;

  /**
   * 构造编辑日志加载异常
   * @param message 异常描述信息
   * @param cause 原始异常原因
   * @param numEditsLoaded 加载失败前已成功加载的编辑日志数量
   */
  public EditLogInputException(String message, Throwable cause,
      long numEditsLoaded) {
    super(message, cause);
    this.numEditsLoaded = numEditsLoaded;
  }
  
  /**
   * 获取加载失败前已成功加载的编辑日志数量
   * @return 已加载的编辑日志操作数量
   */
  public long getNumEditsLoaded() {
    return numEditsLoaded;
  }

}