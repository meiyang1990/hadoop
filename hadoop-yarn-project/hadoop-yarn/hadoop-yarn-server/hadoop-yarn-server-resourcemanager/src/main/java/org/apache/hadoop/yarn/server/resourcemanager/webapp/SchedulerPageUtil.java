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

import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

/**
 * YARN ResourceManager 调度器页面工具类，提供调度队列树展开状态持久化相关的前端JS生成能力
 */
public class SchedulerPageUtil {

  /**
   * 调度队列页面HTML块工具类，生成队列展开状态恢复和存储所需的前端JavaScript
   */
  static class QueueBlockUtil extends HtmlBlock {

    /**
     * 生成重新打开队列树的JavaScript代码，从URL参数恢复队列展开状态
     * @param html HTML块构建器
     */
    private void reopenQueue(Block html) {
      html.
          script().$type("text/javascript").
          __("function reopenQueryNodes() {",
            "  var currentParam = decodeURIComponent(window.location.href)"
                + ".split('?');",
            "  var tmpCurrentParam = currentParam;",
            "  var queryQueuesString = '';",
            "  if (tmpCurrentParam.length > 1) {",
            "    // 解析URL参数中已展开队列的部分",
            "    tmpCurrentParam = tmpCurrentParam[1];",
            "    if (tmpCurrentParam.indexOf('openQueues=') != -1 ) {",
            "      tmpCurrentParam = tmpCurrentParam.split('openQueues=')[1].split('&')[0];",
            "      queryQueuesString = tmpCurrentParam;",
            "    }",
            "  }",
            "  if (queryQueuesString != '') {",
            // 分割队列名称数组，#分隔不同队列
            "    queueArray = queryQueuesString.split('#');",
            // 遍历所有队列节点，匹配到已展开队列则修改jstree状态
            "    $('#cs .q').each(function() {",
            "      var name = $(this).html();",
            "      if (name != 'root' && $.inArray(name, queueArray) != -1) {",
            "        $(this).closest('li').removeClass('jstree-closed').addClass('jstree-open'); ",
            "      }",
            "    });",
            "  }",
            // 绑定节点展开/关闭事件，触发状态更新
            "  $('#cs').bind( {",
            "                  'open_node.jstree' :function(e, data) { storeExpandedQueue(e, data); },",
            "                  'close_node.jstree':function(e, data) { storeExpandedQueue(e, data); }",
            "  });",
            "}").__();
    }

    /**
     * 生成存储队列展开状态到URL参数的JavaScript代码，用户展开/关闭队列时更新URL
     * @param html HTML块构建器
     */
    private void storeExpandedQueue (Block html) {
      html.
          script().$type("text/javascript").
          __("function storeExpandedQueue(e, data) {",
            "  var OPEN_QUEUES = 'openQueues';",
            "  var ACTION_OPEN = 'open';",
            "  var ACTION_CLOSED = 'closed';",
            "  var $li = $(data.args[0]);",
            "  var action = ACTION_CLOSED;  //closed or open",
            "  var queueName = ''",
            "  if ($li.hasClass('jstree-open')) {",
            "      action=ACTION_OPEN;",
            "  }",
            "  queueName = $li.find('.q').html();",
            "  // 分割URL获取查询参数部分",
            "  var currentParam = window.location.href.split('?');",
            "  var tmpCurrentParam = currentParam;",
            "  var queryString = '';",
            "  if (tmpCurrentParam.length > 1) {",
            "    // 获取查询字符串部分",
            "    tmpCurrentParam = tmpCurrentParam[1];",
            "    currentParam = tmpCurrentParam;",
            "    tmpCurrentParam = tmpCurrentParam.split('&');",
            "    var len = tmpCurrentParam.length;",
            "    var paramExist = false;",
            "    if (len > 1) {    // 处理多个查询参数的场景",
            "      queryString = '';",
            "      for (var i = 0 ; i < len ; i++) {  // 遍历查找openQueues参数",
            "        if (tmpCurrentParam[i].substr(0,11) == OPEN_QUEUES + '=') {",
            "          if (action == ACTION_OPEN) {",
            "            tmpCurrentParam[i] = addQueueName(tmpCurrentParam[i],queueName);",
            "          }",
            "          else if (action == ACTION_CLOSED) {",
            "            tmpCurrentParam[i] = removeQueueName(tmpCurrentParam[i] , queueName);",
            "          }",
            "          paramExist = true;",
            "        }",
            "        if (i > 0) {",
            "          queryString += '&';",
            "        }",
            "        queryString += tmpCurrentParam[i];",
            "      }",
            "      // 参数不存在，且是展开操作，新增openQueues参数",
            "      if (action == ACTION_OPEN && !paramExist) {",
            "        queryString = currentParam + '&' + OPEN_QUEUES + '=' + queueName;",
            "      }",
            "    } ",
            "    // 只有一个查询参数的场景",
            "    else {",
            "      tmpCurrentParam=tmpCurrentParam[0];",
            "      // 检查唯一参数是否是openQueues",
            "      if (tmpCurrentParam.substr(0,11) == OPEN_QUEUES + '=') {",
            "        if (action == ACTION_OPEN) {",
            "          queryString = addQueueName(tmpCurrentParam,queueName);",
            "        }",
            "        else if (action == ACTION_CLOSED) {",
            "          queryString = removeQueueName(tmpCurrentParam , queueName);",
            "        }",
            "      }",
            "      else {",
            "        if (action == ACTION_OPEN) {",
            "          queryString = tmpCurrentParam + '&' + OPEN_QUEUES + '=' + queueName;",
            "        }",
            "      }",
            "    }",
            "  } else {",
            "    // URL原本没有任何查询参数，展开操作直接生成参数",
            "    if (action == ACTION_OPEN) {",
            "      tmpCurrentParam = '';",
            "      currentParam = tmpCurrentParam;",
            "      queryString = OPEN_QUEUES+'='+queueName;",
            "    }",
            "  }",
            "  if (queryString != '') {",
            "    queryString = '?' + queryString;",
            "  }",
            "  // 构建新URL，使用pushState更新浏览器地址不刷新页面",
            "  var url = window.location.protocol + '//' + window.location.host + window.location.pathname + queryString;",
            "  window.history.pushState( { path : url }, '', url);",
            "};",
            "",
            /**
             * 从openQueues参数中移除指定队列名称
             */
            "function removeQueueName(queryString, queueName) {",
            "  queryString = decodeURIComponent(queryString);",
            "  var index = queryString.indexOf(queueName);",
            "  // 队列存在才执行移除",
            "  if (index != -1) {",
            "    // 分割出所有队列名称",
            "    var tmp = queryString.substr(11, queryString.length);",
            "    tmp = tmp.split('#');",
            "    var len = tmp.length;",
            "    var newQueryString = '';",
            "    // 遍历保留不等于当前队列的名称",
            "    for (var i = 0 ; i < len ; i++) {",
            "      if (tmp[i] != queueName) {",
            "        if (newQueryString != '') {",
            "          newQueryString += '#';",
            "        }",
            "        newQueryString += tmp[i];",
            "      }",
            "    }",
            "    queryString = newQueryString;",
            "    if (newQueryString != '') {",
            "      queryString = 'openQueues=' + newQueryString;",
            "    }",
            "  }",
            "  return queryString;",
            "}",
            "",
            /**
             * 向openQueues参数添加指定队列名称
             */
            "function addQueueName(queryString, queueName) {",
            "  queueArray = queryString.split('#');",
            "  if ($.inArray(queueArray, queueName) == -1) {",
            "    queryString = queryString + '#' + queueName;",
            "  }",
            "  return queryString;",
            "}").__();
    }

    @Override
    /**
     * 渲染HTML块，生成恢复和存储队列展开状态所需JS
     */
    protected void render(Block html) {
      reopenQueue(html);
      storeExpandedQueue(html);
    }
  }
}