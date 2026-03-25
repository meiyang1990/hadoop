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
package org.apache.hadoop.hdfs.util;

/**
 * 文件粒度锁(FGL, File Granularity Locking)的读写锁作用范围枚举
 * 定义了HDFS元数据操作中不同层级锁的锁定范围，用于控制锁的粒度，提升并发性能
 */
public enum RwLockMode {
  /** 全局范围锁，锁定整个文件系统所有元数据 */
  GLOBAL,
  /** 文件系统层范围锁，仅锁定文件系统目录树元数据 */
  FS,
  /** 块管理层范围锁，仅锁定块映射信息元数据 */
  BM
}