// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * 分配标签查询参数非法时抛出的异常，用于YARN调度约束处理流程
 * 当用户或程序在执行容器放置标签相关查询时传入了无效参数，会抛出此异常
 */
public class InvalidAllocationTagsQueryException extends YarnException {
  private static final long serialVersionUID = 12312831974894L;

  /**
   * 带错误消息的构造方法
   * @param msg 错误描述信息
   */
  public InvalidAllocationTagsQueryException(String msg) {
    super(msg);
  }

  /**
   * 包装原始YarnException的构造方法
   * @param e 原始YARN异常
   */
  public InvalidAllocationTagsQueryException(YarnException e) {
    super(e);
  }
}