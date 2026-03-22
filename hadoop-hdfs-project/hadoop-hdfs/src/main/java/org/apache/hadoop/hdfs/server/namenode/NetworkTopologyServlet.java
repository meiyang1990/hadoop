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
package org.apache.hadoop.hdfs.server.namenode;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.net.HttpHeaders;
import org.apache.hadoop.util.StringUtils;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * 文件级注释：HDFS NameNode 网络拓扑展示Servlet，用于通过HTTP接口对外输出集群DataNode的网络拓扑分布
 * 提供文本和JSON两种输出格式，支持运维监控查看集群节点机架分布情况
 *
 * A servlet to print out the network topology.
 */
@InterfaceAudience.Private
public class NetworkTopologyServlet extends DfsServlet {

  public static final String SERVLET_NAME = "topology";
  public static final String PATH_SPEC = "/topology";

  protected static final String FORMAT_JSON = "json";
  protected static final String FORMAT_TEXT = "text";

  /**
   * 处理HTTP GET请求，获取并输出集群网络拓扑信息
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    final ServletContext context = getServletContext();

    // 从请求Accept头解析输出格式
    String format = parseAcceptHeader(request);
    // 设置对应响应Content-Type
    if (FORMAT_TEXT.equals(format)) {
      response.setContentType("text/plain; charset=UTF-8");
    } else if (FORMAT_JSON.equals(format)) {
      response.setContentType("application/json; charset=UTF-8");
    }

    // 从Servlet上下文获取NameNode实例
    NameNode nn = NameNodeHttpServer.getNameNodeFromContext(context);
    // 获取块管理器，用于获取网络拓扑信息
    BlockManager bm = nn.getNamesystem().getBlockManager();
    // 获取所有DataNode叶节点列表
    List<Node> leaves = bm.getDatanodeManager().getNetworkTopology()
        .getLeaves(NodeBase.ROOT);

    // 输出拓扑信息到响应流
    try (PrintStream out = new PrintStream(
            response.getOutputStream(), false, "UTF-8")) {
      printTopology(out, leaves, format);
    } catch (Throwable t) {
      // 异常处理，返回错误响应
      String errMsg = "Print network topology failed. "
              + StringUtils.stringifyException(t);
      response.sendError(HttpServletResponse.SC_GONE, errMsg);
      throw new IOException(errMsg);
    } finally {
      // 关闭输出流
      response.getOutputStream().close();
    }
  }

  /**
   * 按指定格式打印网络拓扑信息，将节点按机架分组排序后输出
   * 所有机架和节点按字母顺序排序
   *
   * @param stream 输出流
   * @param leaves 根节点下的所有叶节点（DataNode）
   * @param format 输出格式（json/text）
   * @throws BadFormatException 格式不支持时抛出
   * @throws IOException IO异常时抛出
   */
  protected void printTopology(PrintStream stream, List<Node> leaves,
      String format) throws BadFormatException, IOException {
    if (leaves.isEmpty()) {
      stream.print("No DataNodes");
      return;
    }

    // 构建机架到节点列表的映射
    Map<String, TreeSet<String>> tree = new HashMap<>();
    for(Node dni : leaves) {
      String location = dni.getNetworkLocation();
      String name = dni.getName();

      tree.putIfAbsent(location, new TreeSet<>());
      tree.get(location).add(name);
    }

    // 对机架名称按字母排序
    ArrayList<String> racks = new ArrayList<>(tree.keySet());
    Collections.sort(racks);

    // 按指定格式输出
    if (FORMAT_JSON.equals(format)) {
      printJsonFormat(stream, tree, racks);
    } else if (FORMAT_TEXT.equals(format)) {
      printTextFormat(stream, tree, racks);
    } else {
      throw new BadFormatException("Bad format: " + format);
    }
  }

  /**
   * 以JSON格式输出网络拓扑信息
   * @param stream 输出流
   * @param tree 机架->节点集合映射
   * @param racks 排序后的机架列表
   * @throws IOException IO异常时抛出
   */
  protected void printJsonFormat(PrintStream stream, Map<String,
      TreeSet<String>> tree, ArrayList<String> racks) throws IOException {
    JsonFactory dumpFactory = new JsonFactory();
    JsonGenerator dumpGenerator = dumpFactory.createGenerator(stream);
    // 开始输出根数组
    dumpGenerator.writeStartArray();

    // 遍历每个机架
    for(String r : racks) {
      dumpGenerator.writeStartObject();
      dumpGenerator.writeFieldName(r);
      TreeSet<String> nodes = tree.get(r);
      // 开始输出机架下的节点数组
      dumpGenerator.writeStartArray();

      // 遍历每个节点
      for(String n : nodes) {
        dumpGenerator.writeStartObject();
        dumpGenerator.writeStringField("ip", n);
        // 尝试解析IP对应的主机名
        String hostname = NetUtils.getHostNameOfIP(n);
        if(hostname != null) {
          dumpGenerator.writeStringField("hostname", hostname);
        }
        dumpGenerator.writeEndObject();
      }
      dumpGenerator.writeEndArray();
      dumpGenerator.writeEndObject();
    }
    dumpGenerator.writeEndArray();
    dumpGenerator.flush();

    if (!dumpGenerator.isClosed()) {
      dumpGenerator.close();
    }
  }

  /**
   * 以文本格式输出网络拓扑信息
   * @param stream 输出流
   * @param tree 机架->节点集合映射
   * @param racks 排序后的机架列表
   */
  protected void printTextFormat(PrintStream stream, Map<String,
      TreeSet<String>> tree, ArrayList<String> racks) {
    for(String r : racks) {
      stream.println("Rack: " + r);
      TreeSet<String> nodes = tree.get(r);

      for(String n : nodes) {
        stream.print("   " + n);
        // 尝试解析IP对应的主机名，有则附加输出
        String hostname = NetUtils.getHostNameOfIP(n);
        if(hostname != null) {
          stream.print(" (" + hostname + ")");
        }
        stream.println();
      }
      stream.println();
    }
  }

  /**
   * 从请求Accept头解析期望的输出格式
   * @param request HTTP请求
   * @return 解析得到的输出格式（json/text）
   */
  @VisibleForTesting
  protected static String parseAcceptHeader(HttpServletRequest request) {
    String format = request.getHeader(HttpHeaders.ACCEPT);
    return format != null && format.contains(FORMAT_JSON) ?
            FORMAT_JSON : FORMAT_TEXT;
  }

  /**
   * 不支持的输出格式异常类
   */
  public static class BadFormatException extends Exception {
    private static final long serialVersionUID = 1L;

    public BadFormatException(String msg) {
      super(msg);
    }
  }
}