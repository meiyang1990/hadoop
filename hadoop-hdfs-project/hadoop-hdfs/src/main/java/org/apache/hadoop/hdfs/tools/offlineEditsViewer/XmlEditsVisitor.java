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

import java.io.IOException;
import java.io.OutputStream;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import org.apache.hadoop.hdfs.util.XMLUtils;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * XML格式Edits日志访问者，用于遍历编辑日志结构，将Edits日志内容输出为等价的XML文档，供离线分析使用。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class XmlEditsVisitor implements OfflineEditsVisitor {
  private final OutputStream out;
  private ContentHandler contentHandler;
  private final SAXTransformerFactory factory;
  private final static String XML_INDENTATION_PROP ="{http://xml.apache.org/" +
          "xslt}indent-amount";
  private final static String XML_INDENTATION_NUM ="2";

  /**
   * 构造XML格式Edits日志访问者，初始化SAX转换工厂与处理器，准备写入输出流
   *
   * @param out 输出流，用于写入生成的XML文档
   * @throws IOException 初始化过程中发生IO或SAX错误时抛出
   */
  public XmlEditsVisitor(OutputStream out)
      throws IOException {
    this.out = out;
    try {
      // 创建安全的SAX转换工厂实例
      factory = org.apache.hadoop.util.XMLUtils.newSecureSAXTransformerFactory();
      // 创建转换器处理器
      TransformerHandler handler = factory.newTransformerHandler();
      // 设置XML输出方法
      handler.getTransformer().setOutputProperty(OutputKeys.METHOD, "xml");
      // 设置输出编码为UTF-8
      handler.getTransformer().setOutputProperty(OutputKeys.ENCODING, "UTF-8");
      // 开启XML缩进格式化
      handler.getTransformer().setOutputProperty(OutputKeys.INDENT, "yes");
      // 设置缩进空格数为2
      handler.getTransformer().setOutputProperty(XML_INDENTATION_PROP,
              XML_INDENTATION_NUM);
      // 设置XML为独立文档
      handler.getTransformer().setOutputProperty(OutputKeys.STANDALONE, "yes");
      // 设置输出结果指向传入的输出流
      handler.setResult(new StreamResult(out));
      contentHandler = handler;
      
      // 开始XML文档，写入根元素EDITS
      contentHandler.startDocument();
      contentHandler.startElement("", "", "EDITS", new AttributesImpl());
    } catch (TransformerConfigurationException e) {
      throw new IOException("SAXTransformer error: " + e.getMessage());
    } catch (SAXException e) {
      throw new IOException("SAX error: " + e.getMessage());
    }
  }

  /**
   * 访问器初始化，写入Edits日志版本号到XML
   */
  @Override
  public void start(int version) throws IOException {
    try {
      // 开始EDITS_VERSION元素
      contentHandler.startElement("", "", "EDITS_VERSION", new AttributesImpl());
      StringBuilder bld = new StringBuilder();
      bld.append(version);
      // 写入版本号字符串
      addString(bld.toString());
      // 结束EDITS_VERSION元素
      contentHandler.endElement("", "", "EDITS_VERSION");
    }
    catch (SAXException e) {
      throw new IOException("SAX error: " + e.getMessage());
    }
  }

  /**
   * 将字符串转换为字符数组并通过SAX内容处理器输出
   * @param str 要输出的字符串
   * @throws SAXException SAX处理错误时抛出
   */
  public void addString(String str) throws SAXException {
    int slen = str.length();
    char arr[] = new char[slen];
    str.getChars(0, slen, arr, 0);
    contentHandler.characters(arr, 0, slen);
  }
  
  /**
   * 关闭访问器，完成XML文档写入，关闭输出流
   */
  @Override
  public void close(Throwable error) throws IOException {
    try {
      // 结束根元素EDITS
      contentHandler.endElement("", "", "EDITS");
      // 如果处理过程存在错误，写入错误信息到XML
      if (error != null) {
        String msg = error.getMessage();
        XMLUtils.addSaxString(contentHandler, "ERROR",
            (msg == null) ? "null" : msg);
      }
      // 结束XML文档
      contentHandler.endDocument();
    }
    catch (SAXException e) {
      throw new IOException("SAX error: " + e.getMessage());
    }
    // 关闭输出流
    out.close();
  }

  /**
   * 将单个编辑日志操作输出为XML格式
   */
  @Override
  public void visitOp(FSEditLogOp op) throws IOException {
    try {
      // 调用操作自身的XML输出方法写入XML节点
      op.outputToXml(contentHandler);
    }
    catch (SAXException e) {
      throw new IOException("SAX error: " + e.getMessage());
    }
  }
}