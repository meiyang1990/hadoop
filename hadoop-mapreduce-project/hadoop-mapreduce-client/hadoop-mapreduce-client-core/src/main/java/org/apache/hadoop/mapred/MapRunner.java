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
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件说明：MapReduce旧API框架中默认的Map任务执行器实现
 * 核心职责：负责驱动Map任务执行，从输入读取键值对并调用用户自定义Mapper处理每条记录
 * 是MapRunnable接口的默认实现，负责Map任务的主循环执行逻辑
 */
/** Default {@link MapRunnable} implementation.*/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MapRunner<K1, V1, K2, V2>
    implements MapRunnable<K1, V1, K2, V2> {
  
  private Mapper<K1, V1, K2, V2> mapper;
  private boolean incrProcCount;

  /**
   * 配置MapRunner，初始化Mapper实例并配置错误跳过统计开关
   * @param job 作业配置对象
   */
  @SuppressWarnings("unchecked")
  public void configure(JobConf job) {
    // 通过反射实例化用户配置的Mapper类
    this.mapper = ReflectionUtils.newInstance(job.getMapperClass(), job);
    // 仅在启用坏记录跳过功能时开启处理记录计数
    this.incrProcCount = SkipBadRecords.getMapperMaxSkipRecords(job)>0 && 
      SkipBadRecords.getAutoIncrMapperProcCount(job);
  }

  /**
   * 执行Map任务主循环，读取输入记录并调用Mapper处理
   * @param input 输入记录读取器
   * @param output 输出收集器
   * @param reporter 任务进度报告器
   * @throws IOException 读取输入或输出写入时抛出IO异常
   */
  public void run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output,
                  Reporter reporter)
    throws IOException {
    try {
      // 分配可复用的键值对象，所有记录复用同一实例减少对象创建
      K1 key = input.createKey();
      V1 value = input.createValue();
      
      // 循环读取下一条输入记录
      while (input.next(key, value)) {
        // 调用Mapper处理当前键值对
        mapper.map(key, value, output, reporter);
        // 如果开启计数，增加已处理Map记录计数器
        if(incrProcCount) {
          reporter.incrCounter(SkipBadRecords.COUNTER_GROUP, 
              SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS, 1);
        }
      }
    } finally {
      // 关闭Mapper释放资源
      mapper.close();
    }
  }

  /**
   * 获取当前MapRunner持有的Mapper实例
   * @return 当前配置的Mapper对象
   */
  protected Mapper<K1, V1, K2, V2> getMapper() {
    return mapper;
  }
}