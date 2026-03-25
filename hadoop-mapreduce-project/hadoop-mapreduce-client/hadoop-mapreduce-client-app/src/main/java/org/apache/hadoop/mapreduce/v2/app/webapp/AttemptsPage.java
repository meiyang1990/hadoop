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

package org.apache.hadoop.mapreduce.v2.app.webapp;

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_TYPE;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.ATTEMPT_STATE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRApps.TaskAttemptStateUI;
import org.apache.hadoop.yarn.webapp.SubView;

import com.google.inject.Inject;

/**
 * MapReduce ApplicationMaster Web UI 任务尝试列表页面
 * 负责展示按任务类型和尝试状态过滤的任务尝试信息，是任务页面的子页面实现
 */
public class AttemptsPage extends TaskPage {
  /**
   * 过滤后的任务尝试区块，用于展示符合条件的批量任务尝试信息
   * 继承父类AttemptsBlock，重写尝试获取和渲染逻辑实现筛选功能
   */
  static class FewAttemptsBlock extends TaskPage.AttemptsBlock {
    /**
     * 构造方法，注入应用上下文和配置对象
     * @param ctx ApplicationMaster Web应用上下文
     * @param conf Hadoop配置对象
     */
    @Inject
    FewAttemptsBlock(App ctx, Configuration conf) {
      super(ctx, conf);
    }

    /**
     * 验证请求是否合法，该页面始终允许访问
     * @return 始终返回true，表示所有请求都有效
     */
    @Override
    protected boolean isValidRequest() {
      return true;
    }

    /**
     * 生成任务尝试ID的链接HTML，跳转回对应任务详情页
     * @param taskId 所属任务ID
     * @param ta 任务尝试信息对象
     * @return 带链接的任务尝试ID HTML字符串
     */
    @Override
    protected String getAttemptId(TaskId taskId, TaskAttemptInfo ta) {
      return "<a href='" + url("task", taskId.toString()) +
          "'>" + ta.getId() + "</a>";
    }

    /**
     * 根据请求参数过滤获取符合条件的任务尝试列表
     * @return 过滤后的任务尝试集合
     */
    @Override
    protected Collection<TaskAttempt> getTaskAttempts() {
      List<TaskAttempt> fewTaskAttemps = new ArrayList<TaskAttempt>();
      // 从请求参数获取任务类型
      String taskTypeStr = $(TASK_TYPE);
      TaskType taskType = MRApps.taskType(taskTypeStr);
      // 从请求参数获取尝试状态筛选条件
      String attemptStateStr = $(ATTEMPT_STATE);
      TaskAttemptStateUI neededState = MRApps
          .taskAttemptState(attemptStateStr);
      // 遍历对应类型的所有任务，筛选符合状态的尝试
      for (Task task : super.app.getJob().getTasks(taskType).values()) {
        Map<TaskAttemptId, TaskAttempt> attempts = task.getAttempts();
        for (TaskAttempt attempt : attempts.values()) {
          if (neededState.correspondsTo(attempt.getState())) {
            fewTaskAttemps.add(attempt);
          }
        }
      }
      return fewTaskAttemps;
    }
  }

  /**
   * 获取页面内容区块类型
   * @return 该页面使用的内容区块类
   */
  @Override
  protected Class<? extends SubView> content() {
    return FewAttemptsBlock.class;
  }
}