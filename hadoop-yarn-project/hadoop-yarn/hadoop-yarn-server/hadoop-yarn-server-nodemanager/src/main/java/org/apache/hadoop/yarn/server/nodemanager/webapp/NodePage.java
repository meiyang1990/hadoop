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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;

import java.util.Date;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.HTML;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

/**
 * NodeManager Web UI 节点信息页面，展示节点的资源、健康状态和版本等基础信息
 */
public class NodePage extends NMView {

  private static final long BYTES_IN_MB = 1024 * 1024;

  @Override
  protected void commonPreHead(HTML<__> html) {
    super.commonPreHead(html);
    // 设置页面标题
    setTitle("NodeManager information");
    // 初始化导航手风琴组件，默认激活第二个菜单项
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:1}");
  }

  @Override
  protected Class<? extends SubView> content() {
    // 返回节点信息内容块类
    return NodeBlock.class;
  }

  /**
   * 节点信息内容块，渲染NodeManager详细信息表格
   */
  public static class NodeBlock extends HtmlBlock {

    private final Context context;
    private final ResourceView resourceView;

    @Inject
    public NodeBlock(Context context, ResourceView resourceView) {
      this.context = context;
      this.resourceView = resourceView;
    }

    @Override
    protected void render(Block html) {
      // 构建节点信息数据对象
      NodeInfo info = new NodeInfo(this.context, this.resourceView);
      // 添加节点信息行
      info("NodeManager information")
          .__("Total Vmem allocated for Containers",
              StringUtils.byteDesc(info.getTotalVmemAllocated() * BYTES_IN_MB))
          .__("Vmem enforcement enabled",
              info.isVmemCheckEnabled())
          .__("Total Pmem allocated for Container",
              StringUtils.byteDesc(info.getTotalPmemAllocated() * BYTES_IN_MB))
          .__("Pmem enforcement enabled",
              info.isPmemCheckEnabled())
          .__("Total VCores allocated for Containers",
              String.valueOf(info.getTotalVCoresAllocated()))
          .__("Resource types",
              info.getResourceTypes())
          .__("NodeHealthyStatus",
              info.getHealthStatus())
          .__("LastNodeHealthTime", new Date(
              info.getLastNodeUpdateTime()))
          .__("NodeHealthReport",
              info.getHealthReport())
          .__("NodeManager started on", new Date(
              info.getNMStartupTime()))
          .__("NodeManager Version:", info.getNMBuildVersion() +
              " on " + info.getNMVersionBuiltOn())
          .__("Hadoop Version:", info.getHadoopBuildVersion() +
              " on " + info.getHadoopVersionBuiltOn());
      // 渲染信息块到页面
      html.__(InfoBlock.class);
    }
  }
}