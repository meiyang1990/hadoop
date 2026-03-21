// 这个文件已经全部加上中文注释
/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this bottle except in compliance
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

import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

/**
 * YARN ResourceManager WebUI 关于页面块，渲染集群概览信息
 */
public class AboutBlock extends HtmlBlock {
  final ResourceManager rm;

  @Inject
  AboutBlock(ResourceManager rm, ViewContext ctx) {
    super(ctx);
    this.rm = rm;
  }

  @Override
  /**
   * 渲染关于页面的HTML内容
   * @param html HTML块构建器
   */
  protected void render(Block html) {
    // 输出指标概览表格
    html.__(MetricsOverviewTable.class);
    // 获取ResourceManager实例
    ResourceManager rm = getInstance(ResourceManager.class);
    // 构造集群信息对象
    ClusterInfo cinfo = new ClusterInfo(rm);
    // 开始构建集群概览信息块
    info("Cluster overview").
        // 添加集群ID信息
        __("Cluster ID:", cinfo.getClusterId()).
        // 添加ResourceManager运行状态
        __("ResourceManager state:", cinfo.getState()).
        // 添加ResourceManager HA状态
        __("ResourceManager HA state:", cinfo.getHAState()).
        // 添加HA Zookeeper连接状态
          cinfo.getHAZookeeperConnectionState()).
        // 添加RM状态存储信息
        __("ResourceManager RMStateStore:", cinfo.getRMStateStore()).
        // 添加RM启动时间
        __("ResourceManager started on:", Times.format(cinfo.getStartedOn())).
        // 添加RM版本信息
        __("ResourceManager version:", cinfo.getRMBuildVersion() +
          " on " + cinfo.getRMVersionBuiltOn()).
        // 添加Hadoop整体版本信息
        __("Hadoop version:", cinfo.getHadoopBuildVersion() +
          " on " + cinfo.getHadoopVersionBuiltOn());
    // 输出信息块
    html.__(InfoBlock.class);
  }

}