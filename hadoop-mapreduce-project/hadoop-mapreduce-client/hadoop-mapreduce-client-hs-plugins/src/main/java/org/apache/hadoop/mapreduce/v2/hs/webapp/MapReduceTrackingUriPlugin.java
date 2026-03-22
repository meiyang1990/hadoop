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

/**
 * MapReduce历史服务器追踪URI插件，为YARN应用提供MapReduce作业的历史追踪地址生成能力
 * 属于MapReduce历史服务器Web模块，负责将YARN应用ID转换为对应的MapReduce作业历史访问地址
 */
package org.apache.hadoop.mapreduce.v2.hs.webapp;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.TrackingUriPlugin;

/**
 * MapReduce追踪URI插件实现类
 * 继承YARN的TrackingUriPlugin，实现Configurable接口，为YARN上运行的MapReduce应用
 * 生成对应的作业历史服务器访问追踪地址，供YARN ResourceManager页面跳转使用
 */
public class MapReduceTrackingUriPlugin extends TrackingUriPlugin implements
    Configurable {

  /**
   * 设置插件配置，加载MapReduce相关配置
   * @param conf 基础配置对象
   */
  @Override
  public void setConf(Configuration conf) {
    Configuration jobConf = null;
    // 强制加载MapReduce作业配置
    if (conf != null) {
      jobConf = new JobConf(conf);
    } else {
      jobConf = new JobConf();
    }
    super.setConf(jobConf);
  }

  /**
   * Gets the URI to access the given application on MapReduce history server
   * @param id the ID for which a URI is returned
   * @return the tracking URI
   * @throws URISyntaxException
   */
  /**
   * 根据应用ID生成MapReduce作业在历史服务器上的追踪访问地址
   * @param id YARN应用ID
   * @return 完整的作业历史访问URI
   * @throws URISyntaxException URI语法异常
   */
  @Override
  public URI getTrackingUri(ApplicationId id) throws URISyntaxException {
    // 将YARN应用ID格式从application_xxx转换为job_xxx，匹配MapReduce作业ID格式
    String jobSuffix = id.toString().replaceFirst("^application_", "job_");
    // 获取历史服务器的完整Web地址
    String historyServerAddress =
        MRWebAppUtil.getJHSWebappURLWithScheme(getConf());
    // 拼接生成完整的作业历史访问URI并返回
    return new URI(historyServerAddress + "/jobhistory/job/"+ jobSuffix);
  }
}