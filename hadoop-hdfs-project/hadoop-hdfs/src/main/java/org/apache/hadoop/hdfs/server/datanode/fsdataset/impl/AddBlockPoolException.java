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

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

/**
 * 文件级注释：DataNode添加块池和扫描卷过程中抛出的异常，汇总所有IO异常并保留异常与对应卷的关联关系
 *
 * 本异常类收集添加块池、扫描卷过程中抛出的所有IOException，保存每个异常对应的卷信息，
 * 用于集中报告多个数据目录的故障，方便后续处理不健康的数据目录。
 *
 */
public class AddBlockPoolException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  // 存储不健康数据目录及其对应异常
  private Map<FsVolumeSpi, IOException> unhealthyDataDirs;

  /**
   * 构造方法，使用已有的不健康数据目录集合初始化异常
   * @param unhealthyDataDirs 不健康数据目录与对应异常的映射
   */
  public AddBlockPoolException(Map<FsVolumeSpi, IOException>
      unhealthyDataDirs) {
    this.unhealthyDataDirs = unhealthyDataDirs;
  }

  /**
   * 构造方法，创建空的异常对象，内部初始化空的并发映射表存储异常
   */
  public AddBlockPoolException() {
    this.unhealthyDataDirs = new ConcurrentHashMap<FsVolumeSpi, IOException>();
  }

  /**
   * 合并另一个AddBlockPoolException中的异常信息，合并时保留已有卷的原有异常，不覆盖
   * @param e 待合并的异常对象
   */
  public void mergeException(AddBlockPoolException e) {
    if (e == null) {
      return;
    }
    for(FsVolumeSpi v : e.unhealthyDataDirs.keySet()) {
      // If there is already an exception for this volume, keep the original
      // exception and discard the new one. It is likely the first
      // exception caused the second or they were both due to the disk issue
      if (!unhealthyDataDirs.containsKey(v)) {
        unhealthyDataDirs.put(v, e.unhealthyDataDirs.get(v));
      }
    }
  }

  /**
   * 检查当前是否存在异常
   * @return true表示存在至少一个失败卷，false表示所有卷都正常
   */
  public boolean hasExceptions() {
    return !unhealthyDataDirs.isEmpty();
  }

  /**
   * 获取所有失败卷及其对应异常的映射
   * @return 失败卷与异常的映射表
   */
  public Map<FsVolumeSpi, IOException> getFailingVolumes() {
    return unhealthyDataDirs;
  }

  @Override
  public String toString() {
    return getClass().getName() + ": " + unhealthyDataDirs.toString();
  }
}