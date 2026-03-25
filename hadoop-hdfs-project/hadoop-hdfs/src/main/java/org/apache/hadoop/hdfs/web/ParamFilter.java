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
package org.apache.hadoop.hdfs.web;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.Filter;
import javax.servlet.FilterConfig;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

/**
 * HDFS Web服务请求参数过滤器，将所有请求参数名转为小写，实现参数名大小写不敏感解析
 * 解决Web请求中参数名大小写不统一的问题，保证接口兼容性
 */
public class ParamFilter implements Filter {

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain) throws IOException, ServletException {
    // 仅处理HTTP请求，包装请求对象统一转换参数名
    if (request instanceof HttpServletRequest) {
      HttpServletRequest httpServletRequest = (HttpServletRequest) request;
      chain.doFilter(new CustomHttpServletRequestWrapper(httpServletRequest), response);
    } else {
      // 非HTTP请求直接放行
      chain.doFilter(request, response);
    }
  }

  @Override
  public void destroy() {
  }

  /**
   * 自定义HTTP请求包装类，统一将所有参数名转换为小写存储
   */
  private static final class CustomHttpServletRequestWrapper
      extends HttpServletRequestWrapper {

    // 存储转换为小写键名的参数映射表
    private Map<String, String[]> lowerCaseParams = new HashMap<>();

    /**
     * 构造方法，将原始请求参数全部转换为小写键名存储
     * @param request 原始HTTP请求对象
     */
    private CustomHttpServletRequestWrapper(HttpServletRequest request) {
      super(request);
      // 获取原始请求参数映射
      Map<String, String[]> originalParams = request.getParameterMap();
      // 遍历所有参数，将键名转为小写后保存
      for (Map.Entry<String, String[]> entry : originalParams.entrySet()) {
        lowerCaseParams.put(entry.getKey().toLowerCase(), entry.getValue());
      }
    }

    /**
     * 根据参数名获取第一个参数值，自动将输入参数名转为小写匹配
     * @param name 原始参数名
     * @return 匹配到的第一个参数值，无匹配返回null
     */
    public String getParameter(String name) {
      String[] values = getParameterValues(name);
      if (values != null && values.length > 0) {
        return values[0];
      } else {
        return null;
      }
    }

    @Override
    public Map<String, String[]> getParameterMap() {
      // 返回不可修改的小写参数映射，防止外部修改内部状态
      return Collections.unmodifiableMap(lowerCaseParams);
    }

    @Override
    public Enumeration<String> getParameterNames() {
      // 返回所有小写参数名的枚举
      return Collections.enumeration(lowerCaseParams.keySet());
    }

    @Override
    public String[] getParameterValues(String name) {
      // 将输入参数名转为小写后查询
      return lowerCaseParams.get(name.toLowerCase());
    }
  }
}