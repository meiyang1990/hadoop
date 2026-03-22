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

import static org.apache.hadoop.yarn.webapp.YarnWebParams.ENTITY_STRING;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.mapreduce.v2.app.webapp.AppController;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.log.AggregatedLogsPage;

import com.google.inject.Inject;

/**
 * 文件级注释：历史服务器Web应用控制器，负责处理历史服务器所有Web页面请求，
 * 重写父类AppController的方法，提供MapReduce作业历史查询的页面路由和渲染控制
 * 
 * 类级注释：历史服务器Web控制器，管理历史服务器各个页面的渲染逻辑，
 * 提供作业、任务、尝试、日志、配置等不同页面的请求处理能力
 */
public class HsController extends AppController {

  /**
   * 构造方法，通过Guice注入依赖初始化历史服务器控制器
   * @param app 应用上下文对象
   * @param conf Hadoop配置对象
   * @param ctx 请求上下文对象
   */
  @Inject HsController(App app, Configuration conf, RequestContext ctx) {
    super(app, conf, ctx, "History");
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#index()
   */
  /**
   * 处理首页请求，设置页面标题
   */
  @Override
  public void index() {
    setTitle("JobHistory");
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#jobPage()
   */
  /**
   * 获取作业历史页面视图类
   * @return 历史服务器作业页面视图类
   */
  @Override
  protected Class<? extends View> jobPage() {
    return HsJobPage.class;
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#countersPage()
   */
  /**
   * 获取计数器页面视图类
   * @return 历史服务器计数器页面视图类
   */
  @Override
  public Class<? extends View> countersPage() {
    return HsCountersPage.class;
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#tasksPage()
   */
  /**
   * 获取任务列表页面视图类
   * @return 历史服务器任务列表页面视图类
   */
  @Override
  protected Class<? extends View> tasksPage() {
    return HsTasksPage.class;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#taskPage()
   */
  /**
   * 获取单个任务详情页面视图类
   * @return 历史服务器任务详情页面视图类
   */
  @Override
  protected Class<? extends View> taskPage() {
    return HsTaskPage.class;
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#attemptsPage()
   */
  /**
   * 获取尝试列表页面视图类
   * @return 历史服务器尝试列表页面视图类
   */
  @Override
  protected Class<? extends View> attemptsPage() {
    return HsAttemptsPage.class;
  }
  
  // Need all of these methods here also as Guice doesn't look into parent
  // classes.
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#job()
   */
  /**
   * 处理作业详情请求，转发给父类处理，Guice需要子类重写才能注入
   */
  @Override
  public void job() {
    super.job();
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#jobCounters()
   */
  /**
   * 处理作业计数器请求，转发给父类处理，Guice需要子类重写才能注入
   */
  @Override
  public void jobCounters() {
    super.jobCounters();
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#taskCounters()
   */
  /**
   * 处理任务计数器请求，转发给父类处理，Guice需要子类重写才能注入
   */
  @Override
  public void taskCounters() {
    super.taskCounters();
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#tasks()
   */
  /**
   * 处理任务列表请求，转发给父类处理，Guice需要子类重写才能注入
   */
  @Override
  public void tasks() {
    super.tasks();
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#task()
   */
  /**
   * 处理任务详情请求，转发给父类处理，Guice需要子类重写才能注入
   */
  @Override
  public void task() {
    super.task();
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#attempts()
   */
  /**
   * 处理尝试列表请求，转发给父类处理，Guice需要子类重写才能注入
   */
  @Override
  public void attempts() {
    super.attempts();
  }
  
  /**
   * 获取配置页面视图类
   * @return 历史服务器配置页面视图类
   */
  @Override
  protected Class<? extends View> confPage() {
    return HsConfPage.class;
  }

  /**
   * 获取关于页面视图类
   * @return 历史服务器关于页面视图类
   */
  protected Class<? extends View> aboutPage() {
    return HsAboutPage.class;
  }
  
  /**
   * 处理关于页面请求，渲染关于页面
   */
  public void about() {
    render(aboutPage());
  }
  
  /**
   * 处理日志页面请求，解析请求中的实体ID，设置上下文参数后渲染日志页面
   */
  public void logs() {
    // 获取请求中的日志实体ID字符串
    String logEntity = $(ENTITY_STRING);
    JobID jid = null;
    try {
      // 尝试解析为JobID
      jid = JobID.forName(logEntity);
      set(JOB_ID, logEntity);
      requireJob();
    } catch (Exception e) {
      // 解析失败，向下尝试解析为TaskAttemptID
    }

    if (jid == null) {
      try {
        // 尝试解析为TaskAttemptID
        TaskAttemptID taskAttemptId = TaskAttemptID.forName(logEntity);
        set(TASK_ID, taskAttemptId.getTaskID().toString());
        set(JOB_ID, taskAttemptId.getJobID().toString());
        requireTask();
        requireJob();
      } catch (Exception e) {
        // 解析失败，继续渲染页面，不做额外处理
      }
    }
    // 渲染历史服务器日志页面
    render(HsLogsPage.class);
  }

  /**
   * 处理NodeManager聚合日志页面请求，渲染聚合日志页面
   */
  public void nmlogs() {
    render(AggregatedLogsPage.class);
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#singleCounterPage()
   */
  /**
   * 获取单个计数器页面视图类
   * @return 历史服务器单个计数器页面视图类
   */
  @Override
  protected Class<? extends View> singleCounterPage() {
    return HsSingleCounterPage.class;
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#singleJobCounter()
   */
  /**
   * 处理单个作业计数器请求，转发给父类处理，Guice需要子类重写才能注入
   */
  @Override
  public void singleJobCounter() throws IOException{
    super.singleJobCounter();
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#singleTaskCounter()
   */
  /**
   * 处理单个任务计数器请求，转发给父类处理，Guice需要子类重写才能注入
   */
  @Override
  public void singleTaskCounter() throws IOException{
    super.singleTaskCounter();
  }
}