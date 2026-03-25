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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.ATTEMPT_STATE;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_TYPE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRApps.TaskAttemptStateUI;
import org.apache.hadoop.yarn.webapp.SubView;

import com.google.inject.Inject;

/**
 * 历史任务服务器中，按任务类型和状态过滤展示作业下所有任务尝试的页面
 * 属于MapReduce历史服务器Web UI模块，用于展示指定过滤条件下的任务尝试列表
 */
public class HsAttemptsPage extends HsTaskPage {
  
  /**
   * 按过滤条件筛选任务尝试的数据块，用于渲染任务尝试列表
   * 继承通用尝试块实现，针对整作业范围过滤场景做定制
   */
  static class FewAttemptsBlock extends HsTaskPage.AttemptsBlock {
    @Inject
    FewAttemptsBlock(App ctx, Configuration conf) {
      super(ctx, conf);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsTaskPage.AttemptsBlock#isValidRequest()
     * Verify that a job is given.
     */
    /**
     * 验证当前请求是否合法，检查是否存在有效的作业上下文
     * @return 存在作业返回true，否则返回false
     */
    @Override
    protected boolean isValidRequest() {
      return app.getJob() != null;
    }

    /**
     * 生成任务尝试ID的超链接，链接回对应任务详情页
     * @param taskId 任务ID
     * @param ta 任务尝试信息对象
     * @return 带超链接的HTML字符串
     */
    @Override
    protected String getAttemptId(TaskId taskId, TaskAttemptInfo ta) {
      return "<a href='" + url("task", taskId.toString()) +
          "'>" + ta.getId() + "</a>";
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsTaskPage.AttemptsBlock#getTaskAttempts()
     * @return the attempts that are for a given job and a specific type/state.
     */
    /**
     * 根据URL参数过滤出符合条件的任务尝试列表
     * 按任务类型和尝试状态两个维度过滤整个作业下的所有任务尝试
     * @return 过滤后的任务尝试集合
     */
    @Override
    protected Collection<TaskAttempt> getTaskAttempts() {
      List<TaskAttempt> fewTaskAttemps = new ArrayList<TaskAttempt>();
      // 从请求中获取任务类型参数
      String taskTypeStr = $(TASK_TYPE);
      TaskType taskType = MRApps.taskType(taskTypeStr);
      // 从请求中获取尝试状态参数
      String attemptStateStr = $(ATTEMPT_STATE);
      TaskAttemptStateUI neededState = MRApps
          .taskAttemptState(attemptStateStr);
      // 获取当前作业对象
      Job j = app.getJob();
      // 获取该作业下指定类型的所有任务
      Map<TaskId, Task> tasks = j.getTasks(taskType);
      // 遍历所有任务的所有尝试，筛选符合状态条件的尝试
      for (Task task : tasks.values()) {
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
   * 获取页面内容对应的子视图块类
   * @return 定制化的FewAttemptsBlock块类，用于渲染过滤后的任务尝试列表
   */
  @Override
  protected Class<? extends SubView> content() {
    return FewAttemptsBlock.class;
  }
}