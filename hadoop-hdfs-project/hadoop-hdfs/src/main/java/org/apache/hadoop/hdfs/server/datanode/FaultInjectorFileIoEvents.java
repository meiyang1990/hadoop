// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

import javax.annotation.Nullable;

/**
 * 文件路径：hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/FaultInjectorFileIoEvents.java
 * <p>
 * DataNode卷元数据和数据IO操作的故障注入基类，用于测试场景触发各类IO错误，验证故障处理逻辑。
 * 在测试中可通过扩展此类模拟各类IO故障，验证DataNode的容错能力。
 */
@InterfaceAudience.Private
public class FaultInjectorFileIoEvents {

  // 故障注入功能是否启用标识
  private final boolean isEnabled;

  /**
   * 构造故障注入器，从配置中读取故障注入功能启用状态。
   * @param conf Hadoop配置对象，可为null
   */
  public FaultInjectorFileIoEvents(@Nullable Configuration conf) {
    if (conf != null) {
      // 从配置中读取故障注入功能开关，使用默认值false
      isEnabled = conf.getBoolean(DFSConfigKeys
          .DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_KEY, DFSConfigKeys
          .DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_DEFAULT);
    } else {
      // 配置为null时默认禁用故障注入
      isEnabled = false;
    }
  }

  /**
   * 元数据操作执行前的回调钩子，子类可实现自定义故障注入逻辑。
   * @param volume 操作对应的DataNode卷，可为null
   * @param op 即将执行的操作类型
   */
  public void beforeMetadataOp(
      @Nullable FsVolumeSpi volume, FileIoProvider.OPERATION op) {
  }

  /**
   * 文件IO操作执行前的回调钩子，子类可实现自定义故障注入逻辑。
   * @param volume 操作对应的DataNode卷，可为null
   * @param op 即将执行的操作类型
   * @param len 本次IO操作的数据长度
   */
  public void beforeFileIo(
      @Nullable FsVolumeSpi volume, FileIoProvider.OPERATION op, long len) {
  }
}