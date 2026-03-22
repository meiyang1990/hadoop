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

import org.apache.hadoop.classification.*;

/**
 * MapReduce作业访问控制列表(ACL)枚举定义
 * 定义了两种核心的作业操作权限类型，用于控制不同用户对作业的访问和修改能力
 */
@InterfaceAudience.Private
public enum JobACL {

  /**
   * 作业查看权限，控制哪些用户可以查看作业相关信息
   */
  VIEW_JOB(MRJobConfig.JOB_ACL_VIEW_JOB),

  /**
   * 作业修改权限，控制哪些用户可以修改作业状态，例如终止作业、修改作业优先级等操作
   */
  MODIFY_JOB(MRJobConfig.JOB_ACL_MODIFY_JOB);

  String aclName;

  /**
   * 构造JobACL枚举实例，关联对应的配置项名称
   * @param name ACL对应的配置属性名称
   */
  JobACL(String name) {
    this.aclName = name;
  }

  /**
   * 获取该ACL对应的配置属性名称
   * @return ACL配置项名称
   */
  public String getAclName() {
    return aclName;
  }
}