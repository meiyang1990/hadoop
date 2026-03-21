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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * 调度器动态编辑异常，用于表示调度器配置动态修改过程中发生的错误
 */
public class SchedulerDynamicEditException extends YarnException {

  private static final long serialVersionUID = 7100374511387193257L;

  /**
   * 构造带有错误消息的异常实例
   * @param string 错误描述信息
   */
  public SchedulerDynamicEditException(String string) {
    super(string);
  }

}