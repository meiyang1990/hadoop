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
package org.apache.hadoop.yarn.server.utils;

import java.lang.annotation.Documented;

/**
 * 文件级注释：YARN服务器端锁顺序标记注解，用于标识代码需要遵循的锁获取顺序，避免死锁
 *
 * Annotation to document locking order.
 * 用于文档化记录锁的获取顺序，帮助开发者遵守锁顺序避免死锁
 */
@Documented public @interface Lock {
  /**
   * 定义锁的期望获取顺序，数组中按顺序存放需要获取的锁对应的类
   * @return 按获取顺序排列的锁关联类数组
   */
  @SuppressWarnings({ "rawtypes" })
  Class[] value();
  
  /**
   * 表示无锁的标记类，用于标注不需要加锁的场景
   */
  public class NoLock{}
}