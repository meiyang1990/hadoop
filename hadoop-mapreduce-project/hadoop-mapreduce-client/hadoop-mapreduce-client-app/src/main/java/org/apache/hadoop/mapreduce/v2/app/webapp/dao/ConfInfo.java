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
package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.util.MRJobConfUtil;
import org.apache.hadoop.mapreduce.v2.app.job.Job;

/**
 * MR应用Web服务的作业配置信息数据访问对象，封装作业完整配置信息，用于Web接口返回配置数据
 */
@XmlRootElement(name = "conf")
@XmlAccessorType(XmlAccessType.FIELD)
public class ConfInfo {

  protected String path;
  protected ArrayList<ConfEntryInfo> property;

  /**
   * 空构造函数，供JAXB反序列化使用
   */
  public ConfInfo() {
  }

  /**
   * 从指定作业构造配置信息对象，加载并脱敏作业配置
   * @param job 目标作业对象
   * @throws IOException 加载配置文件失败时抛出异常
   */
  public ConfInfo(Job job) throws IOException {

    this.property = new ArrayList<ConfEntryInfo>();
    // 加载作业配置文件
    Configuration jobConf = job.loadConfFile();
    // 保存配置文件路径
    this.path = job.getConfFile().toString();
    // 脱敏配置中敏感信息（如密码）
    MRJobConfUtil.redact(jobConf);
    // 遍历所有配置项，转换为ConfEntryInfo存入列表
    for (Map.Entry<String, String> entry : jobConf) {
      this.property.add(new ConfEntryInfo(entry.getKey(), entry.getValue(), 
          jobConf.getPropertySources(entry.getKey())));
    }

  }

  /**
   * 获取所有配置项列表
   * @return 包含所有配置项信息的列表
   */
  public ArrayList<ConfEntryInfo> getProperties() {
    return this.property;
  }

  /**
   * 获取配置文件路径
   * @return 配置文件路径字符串
   */
  public String getPath() {
    return this.path;
  }

}