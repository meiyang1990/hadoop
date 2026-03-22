// 这个文件已经全部加上中文注释
/*
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
package org.apache.hadoop.hdfs.server.common;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 文件级注释：WebHDFS HTTP请求基于主机/用户/路径的访问控制过滤器，用于限制敏感操作的来源访问。
 * 核心职责：对包含OPEN、获取代理令牌等敏感操作的WebHDFS请求，根据配置的访问规则进行主机地址和用户授权检查。
 */
/**
 * An HTTP filter that can filter requests based on Hosts.
 */
public class HostRestrictingAuthorizationFilter implements Filter {
  public static final String HDFS_CONFIG_PREFIX = "dfs.web.authentication.";
  public static final String RESTRICTION_CONFIG = "host.allow.rules";
  // 定义需要进行访问控制限制的操作类型，匹配查询字符串参数
  public static final Predicate<String> RESTRICTED_OPERATIONS =
      qStr -> (qStr.trim().equalsIgnoreCase("op=OPEN") ||
      qStr.trim().equalsIgnoreCase("op=GETDELEGATIONTOKEN"));
  // 按用户名分组存储访问规则，支持并发更新
  private final Map<String, CopyOnWriteArrayList<Rule>> rulemap =
      new ConcurrentHashMap<>();
  private static final Logger LOG =
      LoggerFactory.getLogger(HostRestrictingAuthorizationFilter.class);

  /**
   * 从Hadoop配置中提取过滤器初始化参数，提取指定前缀的所有配置项。
   * @param conf Hadoop配置对象
   * @param confPrefix 配置前缀
   * @return 提取后的过滤器参数映射表
   */
  public static Map<String, String> getFilterParams(Configuration conf,
      String confPrefix) {
    return conf.getPropsWithPrefix(confPrefix);
  }

  /**
   * 根据用户、客户端IP和请求路径，匹配是否存在允许访问的规则。
   * @param user 请求用户名
   * @param remoteIp 客户端IP地址
   * @param path 请求的HDFS文件路径
   * @return true表示匹配到允许规则，false表示无匹配规则或参数错误
   */
  private boolean matchRule(String user, String remoteIp, String path) {
    // 空值处理，空用户名/路径转换为空字符串匹配通配规则
    user = (user != null ? user : "");
    path = (path != null ? path : "");

    LOG.trace("Got user: {}, remoteIp: {}, path: {}", user, remoteIp, path);

    // IP地址为空直接拒绝，必须有IP才能进行子网匹配
    if (remoteIp == null) {
      LOG.trace("Returned false due to null rempteIp");
      return false;
    }

    // 获取用户专属规则和通配符(*)规则，合并后逐一匹配
    List<Rule> userRules = ((userRules = rulemap.get(user)) != null) ?
        userRules : new ArrayList<Rule>();
    List<Rule> anyRules = ((anyRules = rulemap.get("*")) != null) ?
        anyRules : new ArrayList<Rule>();

    List<Rule> rules = Stream.of(userRules, anyRules)
        .flatMap(l -> l.stream()).collect(Collectors.toList());

    // 遍历所有规则检查匹配
    for (Rule rule : rules) {
      SubnetUtils.SubnetInfo subnet = rule.getSubnet();
      String rulePath = rule.getPath();
      LOG.trace("Evaluating rule, subnet: {}, path: {}",
          subnet != null ? subnet.getCidrSignature() : "*", rulePath);
      // 子网匹配通配(*)或IP在子网范围内，且请求路径在规则路径下，则匹配成功
      if ((subnet == null || subnet.isInRange(remoteIp))
          && FilenameUtils.directoryContains(rulePath, path)) {
        LOG.debug("Found matching rule, subnet: {}, path: {}; returned true",
            rule.getSubnet() != null ? subnet.getCidrSignature() : null,
            rulePath);
        return true;
      }
    }

    LOG.trace("Found no rules for user");
    return false;
  }

  @Override
  public void destroy() {
  }

  /**
   * 过滤器初始化方法，从FilterConfig加载访问规则配置。
   * @param config Servlet过滤器配置
   * @throws ServletException 规则解析错误时抛出异常
   */
  @Override
  public void init(FilterConfig config) throws ServletException {
    // 获取规则配置字符串
    String dropboxRules = config.getInitParameter(RESTRICTION_CONFIG);
    loadRuleMap(dropboxRules);
  }

  /**
   * 解析规则字符串并构建内存规则映射表。
   * 规则格式：每条规则换行或|分隔，单条规则格式为"用户名,子网CIDR,路径通配符"，*表示通配所有。
   * @param ruleString 原始规则字符串，换行或|分隔多条规则
   * @throws IllegalArgumentException 规则格式错误时抛出异常
   */
  private void loadRuleMap(String ruleString) throws IllegalArgumentException {
    if (ruleString == null || ruleString.equals("")) {
      LOG.debug("Got no rules - will disallow anyone access");
    } else {
      // 定义分隔符正则：逗号分割单条规则字段，|或换行分割不同规则
      Pattern comma_split = Pattern.compile(",");
      Pattern rule_split = Pattern.compile("\\||\n");
      // 按字段长度分组，校验规则格式
      Map<Integer, List<String[]>> splits = rule_split.splitAsStream(ruleString)
          .map(x -> comma_split.split(x, 3))
          .collect(Collectors.groupingBy(x -> x.length));
      // 检查所有规则是否都包含三个字段，否则抛出异常并返回错误规则行
      if (!splits.keySet().equals(Collections.singleton(3))) {
        String bad_lines = rule_split.splitAsStream(ruleString)
            .filter(x -> comma_split.split(x, 3).length != 3)
            .collect(Collectors.joining("\n"));
        throw new IllegalArgumentException("Bad rule definition: " + bad_lines);
      }
      // 定义字段索引常量
      int user = 0;
      int cidr = 1;
      int path = 2;
      // 合并规则列表的函数，用于rulemap.merge操作
      BiFunction<CopyOnWriteArrayList<Rule>, CopyOnWriteArrayList<Rule>,
          CopyOnWriteArrayList<Rule>> arrayListMerge = (v1, v2) -> {
        v1.addAll(v2);
        return v1;
      };
      // 遍历所有合法规则，构建Rule对象并存入规则映射表
      for (String[] split : splits.get(3)) {
        LOG.debug("Loaded rule: user: {}, network/bits: {} path: {}",
            split[user], split[cidr], split[path]);
        // CIDR为*表示允许所有子网，否则解析CIDR创建子网信息对象
        Rule rule = (split[cidr].trim().equals("*") ? new Rule(null,
            split[path]) : new Rule(new SubnetUtils(split[cidr]).getInfo(),
            split[path]));
        // 创建包含当前规则的列表，合并对应用户名的现有规则
        CopyOnWriteArrayList<Rule> arrayListRule =
            new CopyOnWriteArrayList<Rule>() {
          {
            add(rule);
          }
        };
        rulemap.merge(split[user], arrayListRule, arrayListMerge);
      }
    }
  }

  /**
   * Servlet过滤器入口方法，封装请求为HttpInteraction对象后调用处理逻辑。
   */
  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain filterChain)
      throws IOException, ServletException {
    final HttpServletRequest httpRequest = (HttpServletRequest) request;
    HttpServletResponse httpResponse = (HttpServletResponse) response;

    handleInteraction(new ServletFilterHttpInteraction(httpRequest,
        httpResponse, filterChain));
  }

  /**
   * 核心过滤处理逻辑，对请求进行授权检查，决定允许或拒绝访问。
   * @param interaction HTTP请求交互抽象对象
   * @throws IOException IO异常
   * @throws ServletException Servlet异常
   */
  public void handleInteraction(HttpInteraction interaction)
      throws IOException, ServletException {
    final String address = interaction.getRemoteAddr();
    final String query = interaction.getQueryString();
    final String uri = interaction.getRequestURI();
    // 非WebHDFS API请求直接放行，不做限制
    if (!uri.startsWith(WebHdfsFileSystem.PATH_PREFIX)) {
      LOG.trace("Proceeding with interaction since the request doesn't access WebHDFS API");
      interaction.proceed();
      return;
    }
    // 从URI中提取请求的HDFS路径
    final String path = uri.substring(WebHdfsFileSystem.PATH_PREFIX.length());
    String user = interaction.getRemoteUser();

    LOG.trace("Got request user: {}, remoteIp: {}, query: {}, path: {}",
        user, address, query, path);
    // 检查请求是否包含需要限制的敏感操作
    boolean authenticatedQuery =
        Arrays.stream(Optional.ofNullable(query).orElse("")
            .trim()
            .split("&"))
            .anyMatch(RESTRICTED_OPERATIONS);
    // 响应未提交且是需要限制的操作，进行授权检查
    if (!interaction.isCommitted() && authenticatedQuery) {
      String[] queryParts = query.split("&");

      // 如果用户名为空，尝试从查询参数中的代理令牌提取用户信息
      if (user == null) {
        LOG.trace("Looking for delegation token to identify user");
        for (String part : queryParts) {
          if (part.trim().startsWith("delegation=")) {
            Token t = new Token();
            // 从URL字符串解码代理令牌
            t.decodeFromUrlString(part.split("=", 2)[1]);
            ByteArrayInputStream buf =
                new ByteArrayInputStream(t.getIdentifier());
            DelegationTokenIdentifier identifier =
                new DelegationTokenIdentifier();
            identifier.readFields(new DataInputStream(buf));
            // 从令牌标识符中获取请求用户名
            user = identifier.getUser().getUserName();
            LOG.trace("Updated request user: {}, remoteIp: {}, query: {}, " +
                "path: {}", user, address, query, path);
          }
        }
      }

      // 检查通配规则或用户规则是否匹配，都不匹配则拒绝请求返回403
      if (authenticatedQuery && !(matchRule("*", address,
          path) || matchRule(user, address, path))) {
        LOG.trace("Rejecting interaction; no rule found");
        interaction.sendError(HttpServletResponse.SC_FORBIDDEN,
            "WebHDFS is configured write-only for " + user + "@" + address +
                " for file: " + path);
        return;
      }
    }

    // 授权通过，继续执行后续过滤器链
    LOG.trace("Proceeding with interaction");
    interaction.proceed();
  }

  /**
   * HTTP请求交互抽象接口，解耦过滤器逻辑与Servlet容器，支持非Servlet环境调用。
   */
  public interface HttpInteraction {

    /**
     * 获取响应是否已提交。
     * @return true表示响应已提交，false表示未提交
     */
    boolean isCommitted();

    /**
     * 获取客户端远程IP地址。
     * @return 客户端IP地址字符串
     */
    String getRemoteAddr();

    /**
     * 获取请求用户名。
     * @return 用户名字符串
     */
    String getRemoteUser();

    /**
     * 获取请求URI路径。
     * @return 请求URI字符串
     */
    String getRequestURI();

    /**
     * 获取请求查询字符串。
     * @return 查询字符串
     */
    String getQueryString();

    /**
     * 获取请求HTTP方法。
     * @return HTTP方法名称
     */
    String getMethod();

    /**
     * 授权通过，继续处理请求。
     * @throws IOException IO异常
     * @throws ServletException Servlet异常
     */
    void proceed() throws IOException, ServletException;

    /**
     * 拒绝请求，发送错误响应。
     * @param code HTTP错误状态码
     * @param message 错误描述信息
     * @throws IOException IO异常
     */
    void sendError(int code, String message) throws IOException;
  }

  /**
   * 单条访问规则存储类，保存子网信息和允许访问的路径。
   */
  private static class Rule {
    private final SubnetUtils.SubnetInfo subnet;
    private final String path;

    /**
     * 构造访问规则对象。
     * @param subnet 允许的子网信息，null表示允许所有IP
     * @param path 允许访问的HDFS路径前缀
     */
    Rule(SubnetUtils.SubnetInfo subnet, String path) {
      this.subnet = subnet;
      this.path = path;
    }

    public SubnetUtils.SubnetInfo getSubnet() {
      return (subnet);
    }

    public String getPath() {
      return (path);
    }
  }

  /**
   * 用于Servlet环境的HttpInteraction实现，封装ServletRequest、ServletResponse和过滤器链。
   */
  private static final class ServletFilterHttpInteraction
      implements HttpInteraction {

    private final FilterChain chain;
    private final HttpServletRequest httpRequest;
    private final HttpServletResponse httpResponse;

    /**
     * 构造Servlet环境交互对象。
     * @param httpRequest HTTP请求对象
     * @param httpResponse HTTP响应对象
     * @param chain 过滤器链
     */
    public ServletFilterHttpInteraction(HttpServletRequest httpRequest,
        HttpServletResponse httpResponse, FilterChain chain) {
      this.httpRequest = httpRequest;
      this.httpResponse = httpResponse;
      this.chain = chain;
    }

    @Override
    public boolean isCommitted() {
      return (httpResponse.isCommitted());
    }

    @Override
    public String getRemoteAddr() {
      return (httpRequest.getRemoteAddr());
    }

    @Override
    public String getRemoteUser() {
      return (httpRequest.getRemoteUser());
    }

    @Override
    public String getRequestURI() {
      return (httpRequest.getRequestURI());
    }

    @Override
    public String getQueryString() {
      return (httpRequest.getQueryString());
    }

    @Override
    public String getMethod() {
      return httpRequest.getMethod();
    }

    @Override
    public void proceed() throws IOException, ServletException {
      chain.doFilter(httpRequest, httpResponse);
    }

    @Override
    public void sendError(int code, String message) throws IOException {
      httpResponse.sendError(code, message);
    }

  }
}