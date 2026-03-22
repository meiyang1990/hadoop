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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;

/**
 * 文件概要：FsDatasetImpl实现类的工厂类，用于HDFS DataNode创建默认文件数据集实例
 * 所属模块：HDFS DataNode，负责DataNode本地数据存储实例的构建
 * 核心功能：实现FsDatasetSpi.Factory工厂接口，创建默认的FsDatasetImpl文件数据集对象
 *
 * A factory for creating {@link FsDatasetImpl} objects.
 */
public class FsDatasetFactory extends FsDatasetSpi.Factory<FsDatasetImpl> {
  /**
   * 创建一个新的FsDatasetImpl文件数据集实例
   * @param datanode 所属DataNode实例
   * @param storage DataNode存储管理实例
   * @param conf Hadoop配置对象
   * @return 初始化完成的FsDatasetImpl实例
   * @throws IOException 初始化过程中IO异常
   */
  @Override
  public FsDatasetImpl newInstance(DataNode datanode,
      DataStorage storage, Configuration conf) throws IOException {
    return new FsDatasetImpl(datanode, storage, conf);
  }
}