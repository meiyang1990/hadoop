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

package org.apache.hadoop.yarn.server.resourcemanager.preprocessor;

import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;


/**
 * YARN应用提交上下文预处理插件，用于向应用提交上下文中添加自定义标签。
 */
class TagAddProcessor implements ContextProcessor {
  @Override
  public void process(String host, String value, ApplicationId applicationId,
      ApplicationSubmissionContext submissionContext) {
    // 获取应用已有的标签集合
    Set<String> applicationTags = submissionContext.getApplicationTags();
    // 如果原标签集合为空，新建空集合
    if (applicationTags == null) {
      applicationTags = new HashSet<>();
    } else {
      // 复制原有标签，避免修改原不可变集合
      applicationTags = new HashSet<>(applicationTags);
    }
    // 添加新标签
    applicationTags.add(value);
    // 更新应用提交上下文中的标签集合
    submissionContext.setApplicationTags(applicationTags);
  }
}