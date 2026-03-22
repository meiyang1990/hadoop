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
package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Stack;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.util.XMLUtils;
import org.apache.hadoop.hdfs.util.XMLUtils.InvalidXmlException;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.OpInstanceCache;
import org.apache.hadoop.hdfs.util.XMLUtils.Stanza;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;


/**
 * 文件级注释：HDFS离线编辑日志XML格式加载器，基于SAX解析XML格式的编辑日志文件，将解析出的操作交给访问者处理
 * 
 * OfflineEditsXmlLoader walks an EditsVisitor over an OEV XML file
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class OfflineEditsXmlLoader 
    extends DefaultHandler implements OfflineEditsLoader {
  // 是否自动修正事务ID
  private final boolean fixTxIds;
  // 访问者对象，用于处理解析出的编辑日志操作
  private final OfflineEditsVisitor visitor;
  // XML文件输入流阅读器
  private final InputStreamReader fileReader;
  // 当前解析状态
  private ParseState state;
  // 当前解析的XML节对象
  private Stanza stanza;
  // XML节栈，用于处理嵌套节点结构
  private Stack<Stanza> stanzaStack;
  // 当前操作码
  private FSEditLogOpCodes opCode;
  // 存放字符数据的缓冲区
  private StringBuilder cbuf;
  // 下一个要分配的事务ID，用于修正事务ID时计数
  private long nextTxId;
  // 编辑日志操作实例缓存，复用操作对象
  private final OpInstanceCache opCache = new OpInstanceCache();
  
  /**
   * XML解析状态枚举，定义SAX解析过程中的各个阶段
   */
  enum ParseState {
    // 等待根EDITS标签
    EXPECT_EDITS_TAG,
    // 等待EDITS_VERSION标签
    EXPECT_VERSION,
    // 等待RECORD记录标签
    EXPECT_RECORD,
    // 等待OPCODE操作码标签
    EXPECT_OPCODE,
    // 等待DATA数据标签
    EXPECT_DATA,
    // 处理DATA数据内容
    HANDLE_DATA,
    // 解析完成，等待结束
    EXPECT_END,
  }
  
  /**
   * 构造OfflineEditsXmlLoader对象，初始化访问者、输入文件和配置标志
   * @param visitor 编辑日志访问者，处理解析出的操作
   * @param inputFile 输入XML格式编辑日志文件
   * @param flags 离线编辑日志查看器配置标志
   * @throws FileNotFoundException 输入文件不存在时抛出
   */
  public OfflineEditsXmlLoader(OfflineEditsVisitor visitor,
        File inputFile, OfflineEditsViewer.Flags flags) throws FileNotFoundException {
    this.visitor = visitor;
    this.fileReader =
        new InputStreamReader(new FileInputStream(inputFile), StandardCharsets.UTF_8);
    this.fixTxIds = flags.getFixTxIds();
  }

  /**
   * 加载XML格式编辑日志文件，使用SAX解析并通过访问者处理所有编辑操作
   * @throws IOException 解析或IO错误时抛出
   */
  @Override
  public void loadEdits() throws IOException {
    try {
      // 创建SAX XML阅读器
      XMLReader xr = XMLReaderFactory.createXMLReader();
      // 禁用DOCTYPE声明，防止XXE攻击
      xr.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
      // 禁用加载外部DTD
      xr.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
      // 禁用外部通用实体
      xr.setFeature("http://xml.org/sax/features/external-general-entities", false);
      // 禁用外部参数实体
      xr.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
      // 设置内容处理器为当前对象
      xr.setContentHandler(this);
      // 设置错误处理器为当前对象
      xr.setErrorHandler(this);
      // 清空DTD处理器
      xr.setDTDHandler(null);
      // 开始解析XML输入
      xr.parse(new InputSource(fileReader));
      // 解析完成，关闭访问者
      visitor.close(null);
    } catch (SAXParseException e) {
      // 打印解析错误位置信息
      System.out.println("XML parsing error: " + "\n" +
          "Line:    " + e.getLineNumber() + "\n" +
          "URI:     " + e.getSystemId() + "\n" +
          "Message: " + e.getMessage());        
      visitor.close(e);
      throw new IOException(e.toString());
    } catch (SAXException e) {
      visitor.close(e);
      throw new IOException(e.toString());
    } catch (RuntimeException e) {
      visitor.close(e);
      throw e;
    } finally {
      // 无论解析成功失败，都关闭文件输入流
      fileReader.close();
    }
  }
  
  /**
   * SAX解析开始文档回调，初始化解析状态
   */
  @Override
  public void startDocument() {
    state = ParseState.EXPECT_EDITS_TAG;
    stanza = null;
    stanzaStack = new Stack<Stanza>();
    opCode = null;
    cbuf = new StringBuilder();
    nextTxId = -1;
  }
  
  /**
   * SAX解析结束文档回调，校验文档结构是否正确
   */
  @Override
  public void endDocument() {
    if (state != ParseState.EXPECT_END) {
      throw new InvalidXmlException("expecting </EDITS>");
    }
  }
  
  /**
   * SAX解析开始元素回调，根据当前解析状态处理不同标签
   */
  @Override
  public void startElement (String uri, String name,
      String qName, Attributes atts) {
    switch (state) {
    case EXPECT_EDITS_TAG:
      if (!name.equals("EDITS")) {
        throw new InvalidXmlException("you must put " +
            "<EDITS> at the top of the XML file! " +
            "Got tag " + name + " instead");
      }
      state = ParseState.EXPECT_VERSION;
      break;
    case EXPECT_VERSION:
      if (!name.equals("EDITS_VERSION")) {
        throw new InvalidXmlException("you must put " +
            "<EDITS_VERSION> at the top of the XML file! " +
            "Got tag " + name + " instead");
      }
      break;
    case EXPECT_RECORD:
      if (!name.equals("RECORD")) {
        throw new InvalidXmlException("expected a <RECORD> tag");
      }
      state = ParseState.EXPECT_OPCODE;
      break;
    case EXPECT_OPCODE:
      if (!name.equals("OPCODE")) {
        throw new InvalidXmlException("expected an <OPCODE> tag");
      }
      break;
    case EXPECT_DATA:
      if (!name.equals("DATA")) {
        throw new InvalidXmlException("expected a <DATA> tag");
      }
      stanza = new Stanza();
      state = ParseState.HANDLE_DATA;
      break;
    case HANDLE_DATA:
      Stanza parent = stanza;
      Stanza child = new Stanza();
      stanzaStack.push(parent);
      stanza = child;
      parent.addChild(name, child);
      break;
    case EXPECT_END:
      throw new InvalidXmlException("not expecting anything after </EDITS>");
    }
  }
  
  /**
   * SAX解析结束元素回调，处理标签内容完成后的逻辑，构建编辑操作并交给访问者
   */
  @Override
  public void endElement (String uri, String name, String qName) {
    // 去除转义并修剪空白字符得到文本内容
    String str = XMLUtils.unmangleXmlString(cbuf.toString(), false).trim();
    // 重置字符缓冲区
    cbuf = new StringBuilder();
    switch (state) {
    case EXPECT_EDITS_TAG:
      throw new InvalidXmlException("expected <EDITS/>");
    case EXPECT_VERSION:
      if (!name.equals("EDITS_VERSION")) {
        throw new InvalidXmlException("expected </EDITS_VERSION>");
      }
      try {
        // 解析编辑日志版本号，调用访问者开始处理
        int version = Integer.parseInt(str);
        visitor.start(version);
      } catch (IOException e) {
        // SAX方法不允许抛出IOException，包装为运行时异常
        throw new RuntimeException(e);
      }
      state = ParseState.EXPECT_RECORD;
      break;
    case EXPECT_RECORD:
      if (name.equals("EDITS")) {
        state = ParseState.EXPECT_END;
      } else if (!name.equals("RECORD")) {
        throw new InvalidXmlException("expected </EDITS> or </RECORD>");
      }
      break;
    case EXPECT_OPCODE:
      if (!name.equals("OPCODE")) {
        throw new InvalidXmlException("expected </OPCODE>");
      }
      // 解析操作码
      opCode = FSEditLogOpCodes.valueOf(str);
      state = ParseState.EXPECT_DATA;
      break;
    case EXPECT_DATA:
      throw new InvalidXmlException("expected <DATA/>");
    case HANDLE_DATA:
      // 设置当前节点的文本值
      stanza.setValue(str);
      if (stanzaStack.empty()) {
        // 所有数据解析完成，处理整条操作记录
        if (!name.equals("DATA")) {
          throw new InvalidXmlException("expected </DATA>");
        }
        state = ParseState.EXPECT_RECORD;
        // 从缓存获取操作实例
        FSEditLogOp op = opCache.get(opCode);
        opCode = null;
        try {
          // 从XML节点解析操作数据
          op.decodeXml(stanza);
          stanza = null;
        } finally {
          if (stanza != null) {
            System.err.println("fromXml error decoding opcode " + opCode +
                "\n" + stanza.toString());
            stanza = null;
          }
        }
        // 如果开启事务ID修正，重新分配连续的事务ID
        if (fixTxIds) {
          if (nextTxId <= 0) {
            // 初始化起始事务ID
            nextTxId = op.getTransactionId();
            if (nextTxId <= 0) {
              nextTxId = 1;
            }
          }
          op.setTransactionId(nextTxId);
          nextTxId++;
        }
        try {
          // 交给访问者处理该操作
          visitor.visitOp(op);
        } catch (IOException e) {
          // SAX方法不允许抛出IOException，包装为运行时异常
          throw new RuntimeException(e);
        }
        state = ParseState.EXPECT_RECORD;
      } else {
        // 弹出父节点，继续处理上层节点
        stanza = stanzaStack.pop();
      }
      break;
    case EXPECT_END:
      throw new InvalidXmlException("not expecting anything after </EDITS>");
    }
  }
  
  /**
   * SAX解析字符内容回调，将字符追加到缓冲区
   */
  @Override
  public void characters (char ch[], int start, int length) {
    cbuf.append(ch, start, length);
  }
}