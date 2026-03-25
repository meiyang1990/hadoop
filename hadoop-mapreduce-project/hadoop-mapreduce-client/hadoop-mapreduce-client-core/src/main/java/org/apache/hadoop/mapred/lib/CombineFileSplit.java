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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

/**
 * CombineFileInputFormat使用的合并文件切片实现（旧MAPRED API版本）
 * 核心职责：将多个小文件合并为一个输入切片，减少Map任务数量，提升小文件处理效率
 * 继承新版本API的CombineFileSplit实现，适配旧版MAPRED API的InputSplit接口
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CombineFileSplit extends 
    org.apache.hadoop.mapreduce.lib.input.CombineFileSplit 
    implements InputSplit {

  private JobConf job;

  /**
   * 空构造方法，供反序列化使用
   */
  public CombineFileSplit() {
  }
  
  /**
   * 构造合并文件切片
   * @param job 作业配置对象
   * @param files 合并到本切片的文件路径数组
   * @param start 每个文件的起始偏移量数组
   * @param lengths 每个文件的切片长度数组
   * @param locations 每个文件所在数据节点位置数组
   */
  public CombineFileSplit(JobConf job, Path[] files, long[] start, 
          long[] lengths, String[] locations) {
    super(files, start, lengths, locations);
    this.job = job;
  }

  /**
   * 构造合并文件切片，起始偏移量默认从0开始
   * @param job 作业配置对象
   * @param files 合并到本切片的文件路径数组
   * @param lengths 每个文件的切片长度数组
   */
  public CombineFileSplit(JobConf job, Path[] files, long[] lengths) {
    super(files, lengths);
    this.job = job;
  }
  
  /**
   * 拷贝构造方法，从已有CombineFileSplit复制生成新对象
   * @param old 原始CombineFileSplit对象
   * @throws IOException 拷贝过程中可能出现IO异常
   */
  public CombineFileSplit(CombineFileSplit old) throws IOException {
    super(old);
  }

  /**
   * 获取当前切片关联的作业配置
   * @return 作业配置对象JobConf
   */
  public JobConf getJob() {
    return job;
  }
}