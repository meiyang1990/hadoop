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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.Serializable;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * 公平调度器饥饿应用跟踪器，用于维护待抢占资源处理的饥饿应用队列
 * 
 * 最初使用阻塞队列实现，未来可替换为其他数据结构，同时提供简化测试的辅助方法
 * 用于公平调度器的抢占机制，管理等待资源的饥饿应用
 */
class FSStarvedApps {

  // 等待抢占线程处理的饥饿应用优先级队列
  private PriorityBlockingQueue<FSAppAttempt> appsToProcess;

  // 当前正在处理的饥饿应用，单消费者模型假设
  private FSAppAttempt appBeingProcessed;

  /**
   * 构造饥饿应用队列，初始化优先级阻塞队列
   */
  FSStarvedApps() {
    appsToProcess = new PriorityBlockingQueue<>(10, new StarvationComparator());
  }

  /**
   * 添加饥饿应用到队列，如果应用未被添加且不在处理中则加入
   * @param app 待添加的饥饿应用尝试
   */
  void addStarvedApp(FSAppAttempt app) {
    if (!app.equals(appBeingProcessed) && !appsToProcess.contains(app)) {
      appsToProcess.add(app);
    }
  }

  /**
   * 阻塞获取下一个待处理的饥饿应用，返回的应用会被标记为处理中直到下一次调用
   * 该方法基于单消费者模型设计
   *
   * @return 待处理的饥饿应用
   * @throws InterruptedException 等待时被中断则抛出
   */
  FSAppAttempt take() throws InterruptedException {
    // 阻塞获取前清空当前处理中标记
    appBeingProcessed = null;

    // 阻塞获取下一个饥饿应用
    FSAppAttempt app = appsToProcess.take();
    appBeingProcessed = app;
    return app;
  }

  /**
   * 饥饿程度比较器，按饥饿程度降序排序，饥饿程度越高优先级越高
   */
  private static class StarvationComparator implements
      Comparator<FSAppAttempt>, Serializable {
    private static final long serialVersionUID = 1;

    @Override
    public int compare(FSAppAttempt app1, FSAppAttempt app2) {
      int ret = 1;
      // 如果app1的饥饿需求小于等于app2，说明app2更饥饿，返回1让app2排在前面
      if (Resources.fitsIn(app1.getStarvation(), app2.getStarvation())) {
        ret = -1;
      }
      return ret;
    }
  }
}