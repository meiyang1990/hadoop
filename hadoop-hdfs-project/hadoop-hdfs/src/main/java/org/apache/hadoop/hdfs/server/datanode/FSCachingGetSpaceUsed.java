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

package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CachingGetSpaceUsed;
import org.apache.hadoop.fs.GetSpaceUsed;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件路径：hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode
 * HDFS数据节点缓存已用空间计算的抽象基类，提供快速准确的存储容量使用统计能力
 * 继承通用缓存类实现定时缓存更新机制，为不同实现提供统一接口
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class FSCachingGetSpaceUsed extends CachingGetSpaceUsed {
  /** 日志记录器 */
  static final Logger LOG =
      LoggerFactory.getLogger(FSCachingGetSpaceUsed.class);

  /**
   * 构造方法，通过Builder对象初始化缓存已用空间计算器
   * @param builder 构建器对象，包含配置参数
   * @throws IOException 初始化失败时抛出IO异常
   */
  public FSCachingGetSpaceUsed(Builder builder) throws IOException {
    super(builder);
  }

  /**
   * FSCachingGetSpaceUsed的构建器类，用于构造FSCachingGetSpaceUsed实例
   * 扩展通用GetSpaceUsed构建器，添加HDFS卷和块池ID配置能力
   */
  public static class Builder extends GetSpaceUsed.Builder {
    private FsVolumeImpl volume;
    private String bpid;

    /**
     * 获取当前构建器绑定的HDFS卷对象
     * @return 绑定的FsVolumeImpl卷实例
     */
    public FsVolumeImpl getVolume() {
      return volume;
    }

    /**
     * 设置当前构建器绑定的HDFS卷对象
     * @param fsVolume 要绑定的FsVolumeImpl卷实例
     * @return 当前构建器实例，支持链式调用
     */
    public Builder setVolume(FsVolumeImpl fsVolume) {
      this.volume = fsVolume;
      return this;
    }

    /**
     * 获取当前构建器绑定的块池ID
     * @return 块池ID字符串
     */
    public String getBpid() {
      return bpid;
    }

    /**
     * 设置当前构建器绑定的块池ID
     * @param bpid 块池ID字符串
     * @return 当前构建器实例，支持链式调用
     */
    public Builder setBpid(String bpid) {
      this.bpid = bpid;
      return this;
    }

    @Override
    /**
     * 构建GetSpaceUsed实例，完成FSCachingGetSpaceUsed特定的构造参数检查与初始化
     * @return 构造完成的GetSpaceUsed实例
     * @throws IOException 构建失败时抛出IO异常
     */
    public GetSpaceUsed build() throws IOException {
      Class clazz = getKlass();
      // 检查目标类是否是FSCachingGetSpaceUsed的子类
      if (FSCachingGetSpaceUsed.class.isAssignableFrom(clazz)) {
        try {
          // 获取带Builder参数的构造方法并保存
          setCons(clazz.getConstructor(Builder.class));
        } catch (NoSuchMethodException e) {
          throw new RuntimeException(e);
        }
      }
      // 调用父类完成最终构建
      return super.build();
    }
  }
}