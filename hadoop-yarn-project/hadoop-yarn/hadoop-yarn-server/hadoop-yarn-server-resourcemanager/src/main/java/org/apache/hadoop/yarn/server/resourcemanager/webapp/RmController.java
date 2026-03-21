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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.QUEUE_NAME;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

import com.google.inject.Inject;

// Do NOT rename/refactor this to RMView as it will wreak havoc
// on macOS HFS as its case-insensitive!

/**
 * YARN ResourceManager Web UI 控制器，处理各个页面请求路由
 */
public class RmController extends Controller {

  @Inject
  RmController(RequestContext ctx) {
    super(ctx);
  }

  @Override public void index() {
    setTitle("Applications");
  }

  /**
   * 处理集群信息页面请求
   */
  public void about() {
    setTitle("About the Cluster");
    render(AboutPage.class);
  }

  /**
   * 处理应用详情页面请求
   */
  public void app() {
    render(AppPage.class);
  }

  /**
   * 处理应用尝试详情页面请求
   */
  public void appattempt() {
    render(AppAttemptPage.class);
  }

  /**
   * 处理容器详情页面请求
   */
  public void container() {
    render(ContainerPage.class);
  }

  /**
   * 处理重定向错误页面请求
   */
  public void failure() {
    render(RedirectionErrorPage.class);
  }

  /**
   * 处理节点列表页面请求
   */
  public void nodes() {
    render(NodesPage.class);
  }

  /**
   * 处理调度器页面请求，根据当前调度器类型渲染对应页面
   */
  public void scheduler() {
    // 仅展示处于调度相关状态的应用
    set(YarnWebParams.APP_STATE, StringHelper.cjoin(
        YarnApplicationState.NEW.toString(),
        YarnApplicationState.NEW_SAVING.toString(),
        YarnApplicationState.SUBMITTED.toString(),
        YarnApplicationState.ACCEPTED.toString(),
        YarnApplicationState.RUNNING.toString()));

    // 获取ResourceManager实例
    ResourceManager rm = getInstance(ResourceManager.class);
    // 获取当前资源调度器实例
    ResourceScheduler rs = rm.getResourceScheduler();
    // 容量调度器处理
    if (rs == null || rs instanceof CapacityScheduler) {
      setTitle("Capacity Scheduler");
      render(CapacitySchedulerPage.class);
      return;
    }
    
    // 公平调度器处理
    if (rs instanceof FairScheduler) {
      setTitle("Fair Scheduler");
      render(FairSchedulerPage.class);
      return;
    }

    // FIFO调度器处理
    if (rs instanceof FifoScheduler) {
      setTitle("FIFO Scheduler");
      render(DefaultSchedulerPage.class);
      return;
    }

    // 处理第三方插件调度器
    renderOtherPluginScheduler(rm);
  }

  /**
   * 渲染第三方插件调度器页面
   * @param rm ResourceManager实例
   */
  private void renderOtherPluginScheduler(ResourceManager rm) {
    ResourceScheduler rs = rm.getResourceScheduler();
    // 获取调度器类名作为页面标题
    String schedulerName = rs.getClass().getSimpleName();

    // 从配置中获取自定义调度器页面类
    Class<? extends View> cls = PluginSchedulerPageHelper
        .getPageClass(rm.getConfig());
    if (cls != null) {
      setTitle(schedulerName);
      render(cls);
    } else {
      // 配置不存在时渲染默认页面并打印警告
      LOG.warn(
          "Render default scheduler page as scheduler page configured doesn't exist");
      setTitle("Default Scheduler");
      render(DefaultSchedulerPage.class);
    }
  }

  /**
   * 插件调度器页面帮助类，加载配置中指定的自定义调度器页面类
   */
  static class PluginSchedulerPageHelper {
    private static boolean hasLoaded = false;
    private static Class<? extends View> pageClass = null;

    /**
     * 获取自定义调度器页面类，单例模式加载
     * @param conf 配置对象
     * @return 自定义页面类，不存在则返回null
     */
    public static Class<? extends View> getPageClass(Configuration conf) {
      if (!hasLoaded) {
        loadPluginSchedulerPageClass(conf);
        hasLoaded = true;
      }
      return pageClass;
    }

    /**
     * 从配置中加载自定义调度器页面类
     * @param conf 配置对象
     */
    private static void loadPluginSchedulerPageClass(Configuration conf) {
      Class<?> configuredClass = conf
          .getClass(YarnConfiguration.YARN_HTTP_WEBAPP_SCHEDULER_PAGE, null);
      // 校验配置类是否继承自View
      if (!View.class.isAssignableFrom(configuredClass)) {
        return;
      }
      pageClass = (Class<? extends View>) configuredClass;
    }
  }

  /**
   * 处理队列详情页面请求
   */
  public void queue() {
    setTitle(join("Queue ", get(QUEUE_NAME, "unknown")));
  }

  /**
   * 处理应用提交禁止页面请求
   */
  public void submit() {
    setTitle("Application Submission Not Allowed");
  }
  
  /**
   * 处理节点标签页面请求
   */
  public void nodelabels() {
    setTitle("Node Labels");
    render(NodeLabelsPage.class);
  }

  /**
   * 处理错误警告列表页面请求
   */
  public void errorsAndWarnings() {
    render(RMErrorsAndWarningsPage.class);
  }

  /**
   * 处理日志聚合状态页面请求
   */
  public void logaggregationstatus() {
    render(AppLogAggregationStatusPage.class);
  }
}