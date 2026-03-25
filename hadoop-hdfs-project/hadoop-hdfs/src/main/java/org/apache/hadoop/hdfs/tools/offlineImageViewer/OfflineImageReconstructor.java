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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import org.apache.hadoop.util.Preconditions;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.ACL_ENTRY_NAME_MASK;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.ACL_ENTRY_NAME_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.ACL_ENTRY_SCOPE_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.ACL_ENTRY_TYPE_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAMESPACE_MASK;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAME_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAMESPACE_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAMESPACE_EXT_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAMESPACE_EXT_MASK;
import static org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter.*;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.thirdparty.com.google.common.io.CountingOutputStream;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ECSchemaProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ErasureCodingPolicyProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoExpirationProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SectionName;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.CacheManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.ErasureCodingSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.AclFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.NameSystemSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SecretManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SnapshotDiffSection.DiffEntry;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.hdfs.util.XMLUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.StringUtils;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

/**
 * 将XML格式的fsimage重新构建为Protobuf二进制格式fsimage的工具类
 * 属于HDFS离线图像查看器工具，用于将PBImageXmlWriter生成的XML转换回标准fsimage
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class OfflineImageReconstructor {
  public static final Logger LOG =
      LoggerFactory.getLogger(OfflineImageReconstructor.class);

  /**
   * 输出流，用于统计当前输出字节数
   */
  private final CountingOutputStream out;

  /**
   * 基于输入XML文件的XML事件读取器
   */
  private final XMLEventReader events;

  /**
   * 段名到段处理器的映射表
   */
  private final HashMap<String, SectionProcessor> sections;

  /**
   * 当前段的起始偏移量
   */
  private long sectionStartOffset;

  /**
   * 文件摘要构建器，收集每个写入段的信息
   */
  private final FileSummary.Builder fileSummaryBld =
      FileSummary.newBuilder();

  /**
   * 字符串表，将字符串映射为ID用于压缩存储
   */
  private final HashMap<String, Integer> stringTable = new HashMap<>();

  /**
   * 用于解析fsimage XML中日期的格式化器
   */
  private final SimpleDateFormat isoDateFormat;

  /**
   * 当前已分配的最大字符串ID
   */
  private int latestStringId = 0;

  private static final String EMPTY_STRING = "";

  /**
   * 构造离线图像重建器
   * @param out 计数输出流
   * @param reader 输入XML的字符流读取器
   * @throws XMLStreamException 初始化XML读取器出错
   */
  private OfflineImageReconstructor(CountingOutputStream out,
      InputStreamReader reader) throws XMLStreamException {
    this.out = out;
    XMLInputFactory factory = XMLInputFactory.newInstance();
    // 禁用DTD支持防止XML实体注入
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    // 禁用外部实体支持防止XML实体注入
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    this.events = factory.createXMLEventReader(reader);
    this.sections = new HashMap<>();
    // 注册各个段的处理器
    this.sections.put(NameSectionProcessor.NAME, new NameSectionProcessor());
    this.sections.put(ErasureCodingSectionProcessor.NAME,
        new ErasureCodingSectionProcessor());
    this.sections.put(INodeSectionProcessor.NAME, new INodeSectionProcessor());
    this.sections.put(SecretManagerSectionProcessor.NAME,
        new SecretManagerSectionProcessor());
    this.sections.put(CacheManagerSectionProcessor.NAME,
        new CacheManagerSectionProcessor());
    this.sections.put(SnapshotDiffSectionProcessor.NAME,
        new SnapshotDiffSectionProcessor());
    this.sections.put(INodeReferenceSectionProcessor.NAME,
        new INodeReferenceSectionProcessor());
    this.sections.put(INodeDirectorySectionProcessor.NAME,
        new INodeDirectorySectionProcessor());
    this.sections.put(FilesUnderConstructionSectionProcessor.NAME,
        new FilesUnderConstructionSectionProcessor());
    this.sections.put(SnapshotSectionProcessor.NAME,
        new SnapshotSectionProcessor());
    this.isoDateFormat = PBImageXmlWriter.createSimpleDateFormat();
  }

  /**
   * 读取并验证下一个标签开始或结束事件
   * @param expected 期望的标签名称，用[]包裹表示不验证名称
   * @param allowEnd 是否允许结束事件
   * @return 匹配到的XML事件
   * @throws IOException 读取XML或验证失败
   */
  private XMLEvent expectTag(String expected, boolean allowEnd)
      throws IOException {
    XMLEvent ev = null;
    while (true) {
      try {
        ev = events.nextEvent();
      } catch (XMLStreamException e) {
        throw new IOException("Expecting " + expected +
            ", but got XMLStreamException", e);
      }
      switch (ev.getEventType()) {
      case XMLEvent.ATTRIBUTE:
        throw new IOException("Got unexpected attribute: " + ev);
      case XMLEvent.CHARACTERS:
        // 跳过空白字符，非空白字符报错
        if (!ev.asCharacters().isWhiteSpace()) {
          throw new IOException("Got unxpected characters while " +
              "looking for " + expected + ": " +
              ev.asCharacters().getData());
        }
        break;
      case XMLEvent.END_ELEMENT:
        if (!allowEnd) {
          throw new IOException("Got unexpected end event " +
              "while looking for " + expected);
        }
        return ev;
      case XMLEvent.START_ELEMENT:
        // 如果不是[]包裹，则验证标签名称
        if (!expected.startsWith("[")) {
          if (!ev.asStartElement().getName().getLocalPart().
                equals(expected)) {
            throw new IOException("Failed to find <" + expected + ">; " +
                "got " + ev.asStartElement().getName().getLocalPart() +
                " instead.");
          }
        }
        return ev;
      default:
        // 跳过注释等其他事件类型
        if (LOG.isTraceEnabled()) {
          LOG.trace("Skipping XMLEvent of type " +
              ev.getEventType() + "(" +  ev + ")");
        }
        break;
      }
    }
  }

  /**
   * 期望并验证标签结束事件
   * @param expected 期望的标签名称
   * @throws IOException 验证失败
   */
  private void expectTagEnd(String expected) throws IOException {
    XMLEvent ev = expectTag(expected, true);
    if (ev.getEventType() != XMLStreamConstants.END_ELEMENT) {
      throw new IOException("Expected tag end event for " + expected +
            ", but got: " + ev);
    }
    if (!expected.startsWith("[")) {
      String tag = ev.asEndElement().getName().getLocalPart();
      if (!tag.equals(expected)) {
        throw new IOException("Expected tag end event for " + expected +
        ", but got tag end event for " + tag);
      }
    }
  }

  /**
   * 辅助XML解析的节点树结构，存储XML子树
   */
  private static class Node {
    private static final String EMPTY = "";
    HashMap<String, LinkedList<Node>> children;
    String val = EMPTY;

    /**
     * 添加子节点
     * @param key 标签名称
     * @param node 子节点
     */
    void addChild(String key, Node node) {
      if (children == null) {
        children = new HashMap<>();
      }
      LinkedList<Node> cur = children.get(key);
      if (cur == null) {
        cur = new LinkedList<>();
        children.put(key, cur);
      }
      cur.add(node);
    }

    /**
     * 弹出并移除第一个子节点
     * @param key 标签名称
     * @return 弹出的节点，不存在返回null
     */
    Node removeChild(String key) {
      if (children == null) {
        return null;
      }
      LinkedList<Node> cur = children.get(key);
      if (cur == null) {
        return null;
      }
      Node node = cur.remove();
      if ((node == null) || cur.isEmpty()) {
        children.remove(key);
      }
      return node;
    }

    /**
     * 弹出子节点并返回其文本值
     * @param key 标签名称
     * @return 文本值，不存在返回null
     */
    String removeChildStr(String key) {
      Node child = removeChild(key);
      if (child == null) {
        return null;
      }
      if ((child.children != null) && (!child.children.isEmpty())) {
        throw new RuntimeException("Node " + key + " contains children " +
            "of its own.");
      }
      return child.getVal();
    }

    /**
     * 弹出子节点并返回整数值
     * @param key 标签名称
     * @return 整数值，不存在返回null
     * @throws IOException 解析出错
     */
    Integer removeChildInt(String key) throws IOException {
      String str = removeChildStr(key);
      if (str == null) {
        return null;
      }
      return Integer.valueOf(str);
    }

    /**
     * 弹出子节点并返回长整数值
     * @param key 标签名称
     * @return 长整数值，不存在返回null
     * @throws IOException 解析出错
     */
    Long removeChildLong(String key) throws IOException {
      String str = removeChildStr(key);
      if (str == null) {
        return null;
      }
      return Long.valueOf(str);
    }

    /**
     * 检查是否存在子节点，存在返回true
     * @param key 标签名称
     * @return 是否存在
     * @throws IOException 解析出错
     */
    boolean removeChildBool(String key) throws IOException {
      String str = removeChildStr(key);
      if (str == null) {
        return false;
      }
      return true;
    }

    /**
     * 获取剩余未处理子节点的键名拼接字符串
     * @return 拼接后的键名字符串
     */
    String getRemainingKeyNames() {
      if (children == null) {
        return "";
      }
      return StringUtils.join(", ", children.keySet());
    }

    /**
     * 验证没有剩余未处理的子节点
     * @param sectionName 当前处理的段名称
     * @throws IOException 存在未处理子节点时抛出
     */
    void verifyNoRemainingKeys(String sectionName) throws IOException {
      String remainingKeyNames = getRemainingKeyNames();
      if (!remainingKeyNames.isEmpty()) {
        throw new IOException("Found unknown XML keys in " +
            sectionName + ": " + remainingKeyNames);
      }
    }

    void setVal(String val) {
      this.val = val;
    }

    String getVal() {
      return val;
    }

    String dump() {
      StringBuilder bld = new StringBuilder();
      if ((children != null) && (!children.isEmpty())) {
        bld.append("{");
      }
      if (val != null) {
        bld.append("[").append(val).append("]");
      }
      if ((children != null) && (!children.isEmpty())) {
        String prefix = "";
        for (Map.Entry<String, LinkedList<Node>> entry : children.entrySet()) {
          for (Node n : entry.getValue()) {
            bld.append(prefix);
            bld.append(entry.getKey()).append(": ");
            bld.append(n.dump());
            prefix = ", ";
          }
        }
        bld.append("}");
      }
      return bld.toString();
    }
  }

  private void loadNodeChildrenHelper(Node parent, String expected,
                                String terminators[]) throws IOException {
    XMLEvent ev = null;
    while (true) {
      try {
        ev = events.peek();
        switch (ev.getEventType()) {
        case XMLEvent.END_ELEMENT:
          if (terminators.length != 0) {
            return;
          }
          events.nextEvent();
          return;
        case XMLEvent.START_ELEMENT:
          String key = ev.asStartElement().getName().getLocalPart();
          // 遇到终止标签则停止遍历
          for (String terminator : terminators) {
            if (terminator.equals(key)) {
              return;
            }
          }
          events.nextEvent();
          Node node = new Node();
          parent.addChild(key, node);
          // 递归加载子节点
          loadNodeChildrenHelper(node, expected, new String[0]);
          break;
        case XMLEvent.CHARACTERS:
          // 累积文本内容，反转义XML特殊字符
          String val = XMLUtils.
              unmangleXmlString(ev.asCharacters().getData(), false);
          parent.setVal(parent.getVal() + val);
          events.nextEvent();
          break;
        case XMLEvent.ATTRIBUTE:
          throw new IOException("Unexpected XML event " + ev