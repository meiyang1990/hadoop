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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

/**
 * NodeManager Web UI 应用详情页面，展示指定应用的基本信息和关联容器列表
 */
public class ApplicationPage extends NMView implements YarnWebParams {

  @Override 
  protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    // 设置容器列表DataTable的ID
    set(DATATABLES_ID, "containers");
    // 初始化容器列表DataTable配置
    set(initID(DATATABLES, "containers"), containersTableInit());
    // 设置容器列表表格样式
    setTableStyles(html, "containers");
  }

  /**
   * 生成容器列表表格初始化配置
   * @return 表格初始化JSON配置字符串
   */
  private String containersTableInit() {
    return tableInit().append(",aoColumns:[null]}").toString();
  }

  @Override
  protected Class<? extends SubView> content() {
    // 返回应用详情内容块类
    return ApplicationBlock.class;
  }

  /**
   * 应用详情内容块，负责渲染应用基本信息和容器列表表格
   */
  public static class ApplicationBlock extends HtmlBlock implements
      YarnWebParams {

    private final Context nmContext;
    private final Configuration conf;
    private final RecordFactory recordFactory;

    @Inject
    public ApplicationBlock(Context nmContext, Configuration conf) {
      this.conf = conf;
      this.nmContext = nmContext;
      this.recordFactory = RecordFactoryProvider.getRecordFactory(this.conf);
    }

    @Override
    protected void render(Block html) {
      ApplicationId applicationID;
      try {
        // 从请求参数解析应用ID
        applicationID = ApplicationId.fromString($(APPLICATION_ID));
      } catch (IllegalArgumentException e) {
        // 应用ID格式非法，输出错误信息
        html.p().__("Invalid Application Id " + $(APPLICATION_ID)).__();
        return;
      }
      DIV<Hamlet> div = html.div("#content");
      // 从NodeManager上下文中获取应用实例
      Application app = this.nmContext.getApplications().get(applicationID);
      if (app == null) {
        // 应用不存在，输出提示信息（可能已完成）
        div.h1("Unknown application with id " + applicationID
            + ". Application might have been completed").__();
        return;
      }
      // 构建应用信息DTO
      AppInfo info = new AppInfo(app);
      // 输出应用基本信息到信息块
      info("Application's information")
            .__("ApplicationId", info.getId())
            .__("ApplicationState", info.getState())
            .__("User", info.getUser());
      // 初始化容器列表表格
      TABLE<Hamlet> containersListBody = html.__(InfoBlock.class)
          .table("#containers");
      // 遍历所有容器，输出容器链接到表格
      for (String containerIdStr : info.getContainers()) {
        containersListBody
               .tr().td()
                 .a(url("container", containerIdStr), containerIdStr)
                 .__().__();
      }
      // 结束表格渲染
      containersListBody.__();
    }
  }
}