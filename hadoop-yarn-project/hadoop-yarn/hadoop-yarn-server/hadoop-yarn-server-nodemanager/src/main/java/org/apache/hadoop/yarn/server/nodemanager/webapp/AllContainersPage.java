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

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import java.util.Map.Entry;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.BODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * NodeManager 容器列表页面，展示当前节点上运行的所有容器信息。
 * 为YARN WebUI提供本节点所有容器的可视化查询能力。
 */
public class AllContainersPage extends NMView {

  /**
   * 页面头部初始化，配置DataTables表格相关参数
   */
  @Override protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    // 设置页面标题
    setTitle("All containers running on this node");
    // 设置表格ID
    set(DATATABLES_ID, "containers");
    // 初始化容器列表表格
    set(initID(DATATABLES, "containers"), containersTableInit());
    // 设置表格样式
    setTableStyles(html, "containers");
  }

  /**
   * 生成DataTables容器列表表格的初始化配置JSON
   * @return 表格初始化配置字符串
   */
  private String containersTableInit() {
    return tableInit().
        // containerid, executiontype, containerid, log-url
        append(", aoColumns:[").append(getContainersIdColumnDefs())
        .append(", null, null, {bSearchable:false}]} ").toString();
  }

  /**
   * 获取容器ID列的渲染配置，启用自然排序和自定义ID解析
   * @return 列配置字符串
   */
  private String getContainersIdColumnDefs() {
    StringBuilder sb = new StringBuilder();
    return sb.append("{'sType':'natural', 'aTargets': [0]")
        .append(", 'mRender': parseHadoopID }").toString();
  }

  /**
   * 获取页面内容区块类
   * @return 内容区块类对象
   */
  @Override
  protected Class<? extends SubView> content() {
    return AllContainersBlock.class;
  }

  /**
   * 容器列表内容区块，负责渲染所有容器的详细表格数据
   */
  public static class AllContainersBlock extends HtmlBlock implements
      YarnWebParams {

    private final Context nmContext;

    @Inject
    /**
     * 构造函数，注入NodeManager上下文
     * @param nmContext NodeManager全局上下文
     */
    public AllContainersBlock(Context nmContext) {
      this.nmContext = nmContext;
    }

    @Override
    /**
     * 渲染容器列表HTML表格
     * @param html HTML块对象
     */
    protected void render(Block html) {
      // 构建表格表头
      TBODY<TABLE<BODY<Hamlet>>> tableBody = html.body()
        .table("#containers")
          .thead()
            .tr()
              .td().__("ContainerId").__()
              .td().__("ExecutionType").__()
              .td().__("ContainerState").__()
              .td().__("logs").__()
            .__()
          .__().tbody();
      // 遍历NodeManager中所有容器，生成表格行
      for (Entry<ContainerId, Container> entry : this.nmContext
          .getContainers().entrySet()) {
        // 构建容器展示信息对象
        ContainerInfo info = new ContainerInfo(this.nmContext, entry.getValue());
        // 添加表格行，包含容器ID、执行类型、状态和日志链接
        tableBody
          .tr()
            .td().a(url("container", info.getId()), info.getId())
            .__()
            .td().__(info.getExecutionType()).__()
            .td().__(info.getState()).__()
            .td()
                .a(url(info.getShortLogLink()), "logs").__()
          .__();
      }
      // 关闭表格标签
      tableBody.__().__().__();
    }

  }
}