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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;

/**
 * 文件级注释：FSImage镜像存储检查器抽象接口，负责检查NameNode的多个存储目录，
 * 并制定从这些存储目录加载命名空间元数据的方案，是NameNode启动阶段加载FSImage的核心抽象
 */
/**
 * Interface responsible for inspecting a set of storage directories and devising
 * a plan to load the namespace from them.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
abstract class FSImageStorageInspector {
  /**
   * 检查指定存储目录的内容，解析其中的FSImage文件信息
   * @param sd 待检查的存储目录
   * @throws IOException 检查过程中发生IO异常时抛出
   */
  abstract void inspectDirectory(StorageDirectory sd) throws IOException;

  /**
   * 检查所有存储目录的升级是否都已完成最终化
   * @return false 如果任意存储目录存在未完成最终化的升级
   */
  abstract boolean isUpgradeFinalized();
  
  /**
   * 获取所有存储目录中最新可用的FSImage文件列表，用于加载命名空间
   * @return 最新的FSImage文件列表
   * @throws IOException 当没有足够可用的FSImage文件（例如所有目录都未找到有效镜像）时抛出
   */
  abstract List<FSImageFile> getLatestImages() throws IOException;

  /** 
   * 获取当前检查到的所有镜像中最大的事务ID，加载镜像后需要回放该ID之前的所有 edits 日志
   * @return 最大已可见事务ID
   */
  abstract long getMaxSeenTxId();

  /**
   * 检查当前存储目录状态是否需要在加载完成后重新保存FSImage镜像
   * @return true 加载完成后需要重新保存FSImage
   */
  abstract boolean needToSave();

  /**
   * FSImage文件信息记录类，保存已定位并解析完成的FSImage文件的元信息
   */
  static class FSImageFile {
    // 该镜像所在的存储目录
    final StorageDirectory sd;    
    // 该镜像对应的检查点事务ID
    final long txId;
    // 镜像文件的文件对象
    private final File file;
    
    /**
     * 构造FSImageFile实例
     * @param sd 存储目录
     * @param file 镜像文件对象
     * @param txId 检查点事务ID
     */
    FSImageFile(StorageDirectory sd, File file, long txId) {
      assert txId >= 0 || txId == HdfsServerConstants.INVALID_TXID
        : "Invalid txid on " + file +": " + txId;
      
      this.sd = sd;
      this.txId = txId;
      this.file = file;
    } 
    
    /**
     * 获取镜像文件对象
     * @return 镜像文件
     */
    File getFile() {
      return file;
    }

    /**
     * 获取该镜像对应的检查点事务ID
     * @return 检查点事务ID
     */
    public long getCheckpointTxId() {
      return txId;
    }
    
    @Override
    public String toString() {
      return String.format("FSImageFile(file=%s, cpktTxId=%019d)", 
                           file.toString(), txId);
    }
  }

}