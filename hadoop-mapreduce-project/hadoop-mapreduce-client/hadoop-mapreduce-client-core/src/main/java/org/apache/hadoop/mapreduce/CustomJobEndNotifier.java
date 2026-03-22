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

package org.apache.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;

import java.net.URL;

/**
 * 文件说明：MapReduce作业结束自定义通知器扩展接口
 * 
 * 自定义作业结束通知器接口，允许用户替换默认实现，定制作业结束通知逻辑。
 * 默认内置实现使用标准HTTP连接发送通知，用户可通过实现此接口并配置
 * {@link MRJobConfig#MR_JOB_END_NOTIFICATION_CUSTOM_NOTIFIER_CLASS} 参数
 * 注入自定义通知实现，支持自定义HTTP客户端、认证方式、SSL配置等。
 * 当前仅支持HTTP/HTTPS类型的通知URL，使用时仍需配置
 * {@link MRJobConfig#MR_JOB_END_NOTIFICATION_URL} 参数指定通知地址。
 */
public interface CustomJobEndNotifier {

  /**
   * 执行单次作业结束通知
   * 
   * 实现类需保证该方法仅执行一次通知尝试，重试逻辑由框架上层控制，
   * 框架会根据配置的重试参数{@link MRJobConfig#MR_JOB_END_RETRY_ATTEMPTS}
   * 和{@link MRJobConfig#MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS}决定是否重新调用该方法。
   * 
   * @param url 通知目标URL，来自配置项{@link MRJobConfig#MR_JOB_END_NOTIFICATION_URL}
   * @param jobConf 当前作业的配置对象
   * @return 通知成功返回true，失败返回false
   * @throws Exception 通知过程中发生异常则抛出
   */
  boolean notifyOnce(URL url, Configuration jobConf) throws Exception;

}