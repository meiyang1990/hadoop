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
package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;

/**
 * 离线编辑日志查看器的访问者接口，定义了遍历HDFS编辑日志结构的标准协议，
 * 不同实现类可以针对遍历到的编辑日志操作执行不同的处理逻辑。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
abstract public interface OfflineEditsVisitor {
  /**
   * 开始遍历编辑日志，提供访问者初始化机会，在遍历所有操作前执行。
   * 
   * @param version 编辑日志版本号
   */
  abstract void start(int version) throws IOException;

  /**
   * 结束遍历编辑日志，提供访问者资源清理机会，在所有操作遍历完成后执行。
   * 
   * @param error 如果遍历因输入流不可恢复错误终止，此处为对应的异常；正常完成则为null
   */
  abstract void close(Throwable error) throws IOException;

  /**
   * 访问一条编辑日志操作，对遍历到的单个编辑操作执行自定义处理。
   *
   * @param op 待访问的编辑日志操作对象
   */
  abstract void visitOp(FSEditLogOp op)
     throws IOException;
}