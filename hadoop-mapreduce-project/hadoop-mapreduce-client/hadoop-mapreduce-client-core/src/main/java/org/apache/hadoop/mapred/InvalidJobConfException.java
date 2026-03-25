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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 作业配置异常类，当作业配置缺少必填属性或属性值非法时抛出此异常
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class InvalidJobConfException
    extends IOException {

  private static final long serialVersionUID = 1L;

  /**
   * 构造一个空的InvalidJobConfException异常对象
   */
  public InvalidJobConfException() {
    super();
  }

  /**
   * 构造带指定错误消息的InvalidJobConfException异常对象
   * @param msg 错误消息
   */
  public InvalidJobConfException(String msg) {
    super(msg);
  }
  
  /**
   * 构造带指定错误消息和根异常的InvalidJobConfException异常对象
   * @param msg 错误消息
   * @param t 根异常
   */
  public InvalidJobConfException(String msg, Throwable t) {
    super(msg, t);
  }

  /**
   * 构造包装指定根异常的InvalidJobConfException异常对象
   * @param t 根异常
   */
  public InvalidJobConfException(Throwable t) {
    super(t);
  }

}