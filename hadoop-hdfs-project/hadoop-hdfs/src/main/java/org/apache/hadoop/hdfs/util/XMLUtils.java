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

package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * HDFS XML处理工具类，提供XML字符串转义/反转义、SAX事件生成、XML节点树存储等功能。
 * 主要用于处理HDFS配置文件和元数据持久化中的XML相关操作。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class XMLUtils {
  /**
   * XML文档非法异常，用于表示解析的XML文档不符合格式要求。
   */
  static public class InvalidXmlException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public InvalidXmlException(String s) {
      super(s);
    }
  }
  
  /**
   * XML反转义错误异常，用于表示无法将转义后的字符串还原为原始字符串。
   */
  public static class UnmanglingError extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    public UnmanglingError(String str, Exception e) {
      super(str, e);
    }
    
    public UnmanglingError(String str) {
      super(str);
    }
  }
  

  /**
   * 判断给定Unicode码点是否需要在XML文档中转义。
   * 根据XML规范，非法字符和反斜杠需要转义处理。
   * @param cp 待检查的Unicode码点
   * @return true表示该码点需要转义，false表示可以直接使用
   */
  private static boolean codePointMustBeMangled(int cp) {
    if (cp < 0x20) {
      return ((cp != 0x9) && (cp != 0xa) && (cp != 0xd));
    } else if ((0xd7ff < cp) && (cp < 0xe000)) {
      return true;
    } else if ((cp == 0xfffe) || (cp == 0xffff)) {
      return true;
    } else if (cp == 0x5c) {
      // 对反斜杠转义，简化解码过程，确保转义序列总是以反斜杠开头
      return true;
    }
    return false;
  }

  private static final int NUM_SLASH_POSITIONS = 4;

  /**
   * 将需要转义的码点格式化为转义字符串。
   * 格式为 \ABCD;，ABCD是码点4位十六进制值。
   * @param cp 需要转义的Unicode码点
   * @return 格式化后的转义字符串
   */
  private static String mangleCodePoint(int cp) {
    return String.format("\\%0" + NUM_SLASH_POSITIONS + "x;", cp);
  }

  /**
   * 将特殊XML字符转换为对应的实体引用。
   * @param cp Unicode码点
   * @return 如果是需要转义的特殊字符，返回对应的实体引用；否则返回null
   */
  private static String codePointToEntityRef(int cp) {
    switch (cp) {
      case '&':
        return "&amp;";
      case '\"':
        return "&quot;";
      case '\'':
        return "&apos;";
      case '<':
        return "&lt;";
      case '>':
        return "&gt;";
      default:
        return null;
    }
  }

  /**
   * 转义字符串使其可以安全放入XML文档中。
   * 对XML非法字符进行自定义转义，对XML特殊字符可选择生成标准实体引用。
   * @param str 原始输入字符串
   * @param createEntityRefs 是否对特殊XML字符生成标准实体引用
   * @return 转义完成后的字符串
   */
  public static String mangleXmlString(String str, boolean createEntityRefs) {
    final StringBuilder bld = new StringBuilder();
    final int length = str.length();
    for (int offset = 0; offset < length; ) {
       final int cp = str.codePointAt(offset);
       final int len = Character.charCount(cp);
       if (codePointMustBeMangled(cp)) {
         bld.append(mangleCodePoint(cp));
       } else {
         String entityRef = null;
         if (createEntityRefs) {
           entityRef = codePointToEntityRef(cp);
         }
         if (entityRef != null) {
           bld.append(entityRef);
         } else {
           for (int i = 0; i < len; i++) {
             bld.append(str.charAt(offset + i));
           }
         }
       }
       offset += len;
    }
    return bld.toString();
  }

  /**
   * 对XML中的转义字符串进行反转义，还原为原始字符串。
   * @param str 待反转义的字符串
   * @param decodeEntityRefs 是否反转义标准XML实体引用
   * @return 还原后的原始字符串
   * @throws UnmanglingError 当输入格式错误时抛出异常
   */
  public static String unmangleXmlString(String str, boolean decodeEntityRefs)
        throws UnmanglingError {
    int slashPosition = -1;
    String escapedCp = "";
    StringBuilder bld = new StringBuilder();
    StringBuilder entityRef = null;
    for (int i = 0; i < str.length(); i++) {
      char ch = str.charAt(i);
      if (entityRef != null) {
        // 正在解析实体引用，累积字符直到遇到分号
        entityRef.append(ch);
        if (ch == ';') {
          String e = entityRef.toString();
          if (e.equals("&quot;")) {
            bld.append("\"");
          } else if (e.equals("&apos;")) {
            bld.append("\'");
          } else if (e.equals("&amp;")) {
            bld.append("&");
          } else if (e.equals("&lt;")) {
            bld.append("<");
          } else if (e.equals("&gt;")) {
            bld.append(">");
          } else {
            throw new UnmanglingError("Unknown entity ref " + e);
          }
          entityRef = null;
        }
      } else  if ((slashPosition >= 0) && (slashPosition < NUM_SLASH_POSITIONS)) {
        // 正在累积转义码点的十六进制字符
        escapedCp += ch;
        ++slashPosition;
      } else if (slashPosition == NUM_SLASH_POSITIONS) {
        // 十六进制字符收集完成，检查并解析码点
        if (ch != ';') {
          throw new UnmanglingError("unterminated code point escape: " +
              "expected semicolon at end.");
        }
        try {
          bld.appendCodePoint(Integer.parseInt(escapedCp, 16));
        } catch (NumberFormatException e) {
          throw new UnmanglingError("error parsing unmangling escape code", e);
        }
        escapedCp = "";
        slashPosition = -1;
      } else if (ch == '\\') {
        // 遇到反斜杠，开始新的转义序列
        slashPosition = 0;
      } else {
        boolean startingEntityRef = false;
        if (decodeEntityRefs) {
          startingEntityRef = (ch == '&');
        }
        if (startingEntityRef) {
          // 开始解析实体引用
          entityRef = new StringBuilder();
          entityRef.append("&");
        } else {
          // 普通字符直接添加
          bld.append(ch);
        }
      }
    }
    // 检查输入是否正常结束，没有未完成的转义序列
    if (entityRef != null) {
      throw new UnmanglingError("unterminated entity ref starting with " +
          entityRef.toString());
    } else if (slashPosition != -1) {
      throw new UnmanglingError("unterminated code point escape: string " +
          "broke off in the middle");
    }
    return bld.toString();
  }
  
  /**
   * 向SAX内容处理器添加一个包含字符串内容的XML标签。
   * 自动对字符串内容进行转义处理，生成对应的SAX开始/内容/结束事件。
   * @param contentHandler SAX内容处理器
   * @param tag XML标签名称
   * @param val 标签包含的字符串内容
   * @throws SAXException 由SAX处理器抛出异常
   */
  public static void addSaxString(ContentHandler contentHandler,
      String tag, String val) throws SAXException {
    contentHandler.startElement("", "", tag, new AttributesImpl());
    char c[] = mangleXmlString(val, false).toCharArray();
    contentHandler.characters(c, 0, c.length);
    contentHandler.endElement("", "", tag);
  }

  /**
   * 表示XML解析过程中的一个节点 stanza，存储节点值和子节点集合，用于构建XML解析树。
   * 支持按名称查询子节点和节点值，是HDFS XML配置解析的基础数据结构。
   */
  static public class Stanza {
    private final TreeMap<String, LinkedList <Stanza > > subtrees;

    /** 当前节点自身的文本值 */
    private String value;
    
    public Stanza() {
      subtrees = new TreeMap<String, LinkedList <Stanza > >();
      value = "";
    }
    
    public void setValue(String value) {
      this.value = value;
    }
    
    public String getValue() {
      return this.value;
    }
    
    /**
     * 检查当前节点是否包含指定名称的子节点。
     * @param name 子节点名称
     * @return true表示存在至少一个对应名称的子节点
     */
    public boolean hasChildren(String name) {
      return subtrees.containsKey(name);
    }
    
    /**
     * 获取当前节点下指定名称的所有子节点列表。
     * @param name 子节点名称
     * @return 指定名称的子节点列表
     * @throws InvalidXmlException 如果不存在该名称的子节点，抛出异常
     */
    public List<Stanza> getChildren(String name) throws InvalidXmlException {
      LinkedList <Stanza> children = subtrees.get(name);
      if (children == null) {
        throw new InvalidXmlException("no entry found for " + name);
      }
      return children;
    }
    
    /**
     * 获取当前节点下指定名称唯一子节点的文本值。
     * @param name 子节点名称
     * @return 指定子节点的文本值
     * @throws InvalidXmlException 如果不存在该节点或存在多个该节点，抛出异常
     */
    public String getValue(String name) throws InvalidXmlException {
      String ret = getValueOrNull(name);
      if (ret == null) {
        throw new InvalidXmlException("no entry found for " + name);
      }
      return ret;
    }

    /**
     * 获取当前节点下指定名称唯一子节点的文本值，不存在则返回null。
     * @param name 子节点名称
     * @return 指定子节点的文本值，不存在则返回null
     * @throws InvalidXmlException 如果存在多个该名称的子节点，抛出异常
     */
    public String getValueOrNull(String name) throws InvalidXmlException {
      if (!subtrees.containsKey(name)) {
        return null;
      }
      LinkedList <Stanza> l = subtrees.get(name);
      if (l.size() != 1) {
        throw new InvalidXmlException("More than one value found for " + name);
      }
      return l.get(0).getValue();
    }
    
    /**
     * 向当前节点添加一个子节点。
     * @param name 子节点名称
     * @param child 子节点Stanza对象
     */
    public void addChild(String name, Stanza child) {
      LinkedList<Stanza> l;
      if (subtrees.containsKey(name)) {
        l = subtrees.get(name);
      } else {
        l = new LinkedList<Stanza>();
        subtrees.put(name, l);
      }
      l.add(child);
    }
    
    /**
     * 将当前Stanza转换为人类可读的字符串表示，用于调试和日志输出。
     * @return 格式化后的字符串表示
     */
    @Override
    public String toString() {
      StringBuilder bld = new StringBuilder();
      bld.append("{");
      if (!value.equals("")) {
        bld.append("\"").append(value).append("\"");
      }
      String prefix = "";
      for (Map.Entry<String, LinkedList <Stanza > > entry :
          subtrees.entrySet()) {
        String key = entry.getKey();
        LinkedList <Stanza > ll = entry.getValue();
        for (Stanza child : ll) {
          bld.append(prefix);
          bld.append("<").append(key).append(">");
          bld.append(child.toString());
          prefix = ", ";
        }
      }
      bld.append("}");
      return bld.toString();
    }
  }
}