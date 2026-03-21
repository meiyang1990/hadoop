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

import static org.apache.hadoop.yarn.webapp.view.JQueryUI._EVEN;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._ODD;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceRequestInfo;
import org.apache.hadoop.yarn.server.webapp.AppAttemptBlock;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

/**
 * ResourceManager端应用尝试信息页面块，继承通用AppAttemptBlock，
 * 负责渲染应用尝试的资源请求、容器位置统计、调度诊断信息等RM侧特有内容
 */
public class RMAppAttemptBlock extends AppAttemptBlock{

  private final ResourceManager rm;
  protected Configuration conf;
  // 前端获取并展示应用分配诊断信息的JavaScript脚本常量
  private final static String DIAGNOSTICS_SCRIPT_BODY = new StringBuilder()
      .append("var refresh = false;")
      .append("var haveGotAppDiagnostic = false;")
      .append("function getArray(objOrArray) {")
      .append("  if(Array.isArray(objOrArray)){")
      .append("    return objOrArray;")
      .append("  } else {")
      .append("    return [objOrArray];")
      .append("  }")
      .append("}")
      .append("function getTableDataFromObjectArray(objectArray, fields) {")
      .append("  if (objectArray == undefined) {")
      .append("    return [];")
      .append("  }")
      .append("  var data = [];")
      .append("  $.each(objectArray, function(i, object) {")
      .append("    var dataItem = [];")
      .append("    for (var i=0; i<fields.length; i++) {")
      .append("      dataItem.push(object[fields[i]] != undefined ? ")
      .append("object[fields[i]] : '');")
      .append("    }")
      .append("    data.push(dataItem);")
      .append("  });")
      .append("  return data;")
      .append("}")
      .append("function sendRequest(requestURL, doneCallBackFunc,")
      .append("                      failCallBackFunc) {")
      .append(" $.ajax({")
      .append("  type: 'GET',")
      .append("  url: requestURL,")
      .append("  contentType: 'text/plain'")
      .append(" }).done(doneCallBackFunc).fail(failCallBackFunc);")
      .append("}")
      .append("function parseQueryAppActResponse(responseObj) {")
      .append(" var requestDiagnostics = [], dateTime, appDiagnostic;")
      .append(" responseObj = responseObj['appActivities'];")
      .append(" if(responseObj.hasOwnProperty('allocations')){")
      .append("  var allocations = getArray(responseObj['allocations']);")
      .append("  var allocation = allocations[0];")
      .append("  dateTime = allocation['dateTime'];")
      .append("  if(allocation.hasOwnProperty('diagnostic')){")
      .append("   appDiagnostic = allocation['diagnostic'];")
      .append("  }")
      .append("  if(allocation.hasOwnProperty('children')){")
      .append("   var requestAllocations = ")
      .append("     getArray(allocation['children']);")
      .append("   $.each(requestAllocations, function (i, requestAllocation) {")
      .append("    if(requestAllocation.hasOwnProperty('children')){")
      .append("     var allocationAttempts = ")
      .append("       getArray(requestAllocation['children']);")
      .append("     var diagnosticSummary = requestAllocation.hasOwnProperty(")
      .append("       'diagnostic')? requestAllocation['diagnostic']+'. ':'';")
      .append("     $.each(allocationAttempts, function(i,allocationAttempt) {")
      .append("      if (i > 0) {")
      .append("       diagnosticSummary += ', ';")
      .append("      }")
      .append("      diagnosticSummary += allocationAttempt['count'] + ' ' +")
      .append("        (allocationAttempt['count']=='1' ? 'node ' : 'nodes ')")
      .append("        + allocationAttempt['allocationState'];")
      .append("      if (allocationAttempt.hasOwnProperty('diagnostic')) {")
      .append("       diagnosticSummary += ' with diagnostic:' + ")
      .append("                   allocationAttempt['diagnostic'];")
      .append("      }")
      .append("     });")
      .append("     delete requestAllocation['children'];")
      .append("     requestAllocation['diagnostic']=diagnosticSummary;")
      .append("    }")
      .append("    requestDiagnostics.push(requestAllocation);")
      .append("   });")
      .append("  }")
      .append(" }")
      .append(" return [requestDiagnostics, dateTime, appDiagnostic];")
      .append("}")
      .append("function handleQueryAppActDone(data){")
      .append("  console.log('App activities:', data);")
      .append("  var results = parseQueryAppActResponse(data);")
      .append("  console.log('parsed results:', results);")
      .append("  var requestDiagnostics = results[0];")
      .append("  var dateTime = results[1];")
      .append("  var appDiagnostic = results[2];")
      .append("  haveGotAppDiagnostic = false;")
      .append("  if (appDiagnostic != undefined) {")
      .append("    haveGotAppDiagnostic = true;")
      .append("    $('#appDiagnostic').html('App diagnostic: '")
      .append("        + appDiagnostic);")
      .append("  }")
      .append("  $('#diagnosticsTableDiv').empty();")
      .append("  if (requestDiagnostics.length > 0) {")
      .append("    haveGotAppDiagnostic = true;")
      .append("    tableData = getTableDataFromObjectArray(requestDiagnostics,")
      .append(" ['requestPriority', 'allocationRequestId', 'allocationState',")
      .append(" 'diagnostic']);")
      .append("    $('#diagnosticsTableDiv').append('<table ")
      .append("      id=\"diagnosticsTable\"></table>');")
      .append("    $('#diagnosticsTable').dataTable( {")
      .append("      'aaData': tableData,")
      .append("      'aoColumns': [")
      .append("        { 'sTitle': 'Priority' },")
      .append("        { 'sTitle': 'AllocationRequestId' },")
      .append("        { 'sTitle': 'AllocationState' },")
      .append("        { 'sTitle': 'DiagnosticSummary' }")
      .append("      ],")
      .append("      bJQueryUI:true, sPaginationType: 'full_numbers',")
      .append("      iDisplayLength:20, aLengthMenu:[20, 40, 60, 80, 100]")
      .append("    });")
      .append("  }")
      .append("  if (haveGotAppDiagnostic) {")
      .append("    $('#refreshDiagnosticsBtn').attr('disabled',false);")
      .append("    $('#diagnosticsUpdateTime').html('Updated at ' + dateTime);")
      .append("  } else {")
      .append("    $('#diagnosticsUpdateTime').html('');")
      .append("    if (refresh) {")
      .append("      refreshSchedulerDiagnostics();")
      .append("    }")
      .append("  }")
      .append("}")
      .append("function handleRequestFailure(data){")
      .append(" alert('Request app activities REST API failed.');")
      .append(" console.log(data);")
      .append("}")
      .append("function handleRefreshAppActDone(data){")
      .append(" refresh = true;")
      .append(" setTimeout(function(){")
      .append("  queryAppDiagnostics();")
      .append(" }, 2000);")
      .append("}")
      .append("function refreshAppDiagnostics() {")
      .append(" $('#refreshDiagnosticsBtn').attr('disabled',true);")
      .append(" sendRequest(refreshAppActivitiesURL,handleRefreshAppActDone,")
      .append("             handleRequestFailure);")
      .append("}")
      .append("function queryAppDiagnostics() {")
      .append(" sendRequest(getAppActivitiesURL,handleQueryAppActDone,")
      .append("             handleRequestFailure);")
      .append("}")
      .append("function parseHierarchicalQueue(node, nodeInfos,")
      .append("    parentNodePath, partition) {")
      .append("  var nodePath = parentNodePath + '/' + node['name'];")
      .append("  if (node.hasOwnProperty('name')){")
      .append("    if (node['diagnostic'] == undefined || node['diagnostic']")
      .append("      .indexOf(ignoreActivityContent) == -1){")
      .append("     var nodeInfo = {};")
      .append("     nodeInfo['partition'] = partition;")
      .append("     nodeInfo['path'] = nodePath;")
      .append("     nodeInfo['allocationState'] = node['allocationState'];")
      .append("     nodeInfo['diagnostic'] = ")
      .append("      node['diagnostic'] == undefined ? '':node['diagnostic'];")
      .append("     nodeInfos.push(nodeInfo);")
      .append("    }")
      .append("  }")
      .append("  if (node.hasOwnProperty('children')){")
      .append("    var children = getArray(node['children']);")
      .append("    $.each(children, function (i, child) {")
      .append("      parseHierarchicalQueue(child, nodeInfos, nodePath,")
      .append("        partition);")
      .append("    });")
      .append("  }")
      .append("}")
      .append("function parseQueueDiagnostics(data) {")
      .append("  var nodeInfos = [], dateTime;")
      .append("  if(data.hasOwnProperty('dateTime')){")
      .append("      dateTime = data['dateTime'];")
      .append("  }")
      .append("  if(data.hasOwnProperty('allocations')){")
      .append("    var allocations = getArray(data['allocations']);")
      .append("    $.each(allocations, function (i, allocation) {")
      .append("      if (allocation.hasOwnProperty('root')) {")
      .append("        var root = allocation['root'];")
      .append("        var partition = allocation['partition'];")
      .append("        parseHierarchicalQueue(root, nodeInfos, '', partition);")
      .append("      }")
      .append("    });")
      .append("  }")
      .append("  return [nodeInfos, dateTime];")
      .append("}")
      .append("function handleRefreshSchedulerActDone(data){")
      .append(" console.log('handleRefreshSchedulerActDone', data);")
      .append(" setTimeout(function(){")
      .append("  querySchedulerDiagnostics();")
      .append("  $('#refreshDiagnosticsBtn').attr('disabled',false);")
      .append(" }, 100);")
      .append("}")
      .append("function handleQuerySchedulerActDone(data){")
      .append("  console.log('Scheduler activities:', data);")
      .append("  data = data['activities'];")
      .append("  var results = parseQueueDiagnostics(data);")
      .append("  var nodeInfos = results[0];")
      .append("  var dateTime = results[1];")
      .append("  $('#diagnosticsTableDiv').empty();")
      .append("  if(dateTime != undefined){")
      .append("   $('#diagnosticsUpdateTime').html('App diagnostics not found!")
      .append(" Got useful scheduler activities updated at '+dateTime);")
      .append("    tableData = getTableDataFromObjectArray(nodeInfos, ")
      .append(" ['partition', 'path', 'allocationState', 'diagnostic']);")
      .append("    $('#diagnosticsTableDiv').append('<table ")
      .append("      id=\"diagnosticsTable\"></table>');")
      .append("    $('#diagnosticsTable').dataTable({")
      .append("      'aaData': tableData,")
      .append("      'aoColumns': [")
      .append("        { 'sTitle': 'Partition' },")
      .append("        { 'sTitle': 'SchedulingNode' },")
      .append("        { 'sTitle': 'AllocationState' },")
      .append("        { 'sTitle': 'Diagnostic' }")
      .append("      ],")
      .append("      bJQueryUI:true, sPaginationType: 'full_numbers',")
      .append("      iDisplayLength:20, aLengthMenu:[20, 40, 60, 80, 100]")
      .append("    });")
      .append("  } else if(data.hasOwnProperty('diagnostic')){")
      .append("    $('#diagnosticsUpdateTime').html(data['diagnostic']);")
      .append("  }")
      .append("  $('#refreshDiagnosticsBtn').attr('disabled',false);")
      .append("}")
      .append("function refreshSchedulerDiagnostics() {")
      .append(" $('#refreshDiagnosticsBtn').attr('disabled',true);")
      .append(" sendRequest(schedulerActivitiesURL,")
      .append("   handleRefreshSchedulerActDone,handleRequestFailure);")
      .append("}")
      .append("function querySchedulerDiagnostics() {")
      .append(" sendRequest(schedulerActivitiesURL,")
      .append("   handleQuerySchedulerActDone,handleRequestFailure);")
      .append("}")
      .append("queryAppDiagnostics();").toString();

  /**
   * 构造函数，通过Guice注入依赖
   * @param ctx 页面视图上下文
   * @param rm ResourceManager实例
   * @param conf Hadoop配置
   */
  @Inject
  RMAppAttemptBlock(ViewContext ctx, ResourceManager rm, Configuration conf) {
    super(null, ctx);
    this.rm = rm;
    this.conf = conf;
  }

  /**
   * 创建未完成