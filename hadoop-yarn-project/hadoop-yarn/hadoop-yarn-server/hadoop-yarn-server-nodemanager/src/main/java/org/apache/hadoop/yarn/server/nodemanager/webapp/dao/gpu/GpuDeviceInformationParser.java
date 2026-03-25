// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu;

import java.io.StringReader;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.sax.SAXSource;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.exceptions.YarnException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import static org.apache.hadoop.util.XMLUtils.EXTERNAL_GENERAL_ENTITIES;
import static org.apache.hadoop.util.XMLUtils.EXTERNAL_PARAMETER_ENTITIES;
import static org.apache.hadoop.util.XMLUtils.LOAD_EXTERNAL_DECL;
import static org.apache.hadoop.util.XMLUtils.VALIDATION;

/**
 * GPU设备信息XML解析器，解析nvidia-smi命令输出的XML格式GPU信息
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GpuDeviceInformationParser {
  private static final Logger LOG = LoggerFactory.getLogger(
      GpuDeviceInformationParser.class);
  public static final String GPU_SCRIPT_REFERENCE = "GPU device detection " +
      "script";

  private final Unmarshaller unmarshaller;
  private final XMLReader xmlReader;

  /**
   * 构造GPU设备信息解析器，初始化XML解析相关组件
   * @throws YarnException 初始化失败时抛出异常
   */
  public GpuDeviceInformationParser() throws YarnException {
    try {
      // 初始化安全配置的SAX解析工厂
      final SAXParserFactory parserFactory = initSaxParserFactory();
      // 创建JAXB上下文，绑定GPU信息对象类型
      final JAXBContext jaxbContext = JAXBContext.newInstance(
          GpuDeviceInformation.class);
      this.xmlReader = parserFactory.newSAXParser().getXMLReader();
      this.unmarshaller = jaxbContext.createUnmarshaller();
    } catch (Exception e) {
      String msg = "Exception while initializing parser for " +
          GPU_SCRIPT_REFERENCE;
      LOG.error(msg, e);
      throw new YarnException(msg, e);
    }
  }

  /**
   * 初始化安全配置的SAX解析工厂，禁用外部实体加载避免XXE攻击
   * 由于nvidia-smi输出默认包含DOCTYPE引用外部DTD，需要禁用相关加载
   * @return 配置完成的SAX解析工厂
   * @throws Exception 初始化过程异常
   */
  private SAXParserFactory initSaxParserFactory() throws Exception {
    SAXParserFactory spf = SAXParserFactory.newInstance();
    // 开启安全处理模式
    spf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    // 禁用外部实体和DTD加载，防止XML注入攻击
    spf.setFeature(LOAD_EXTERNAL_DECL, false);
    spf.setFeature(EXTERNAL_GENERAL_ENTITIES, false);
    spf.setFeature(EXTERNAL_PARAMETER_ENTITIES, false);
    spf.setFeature(VALIDATION, false);
    return spf;
  }

  /**
   * 解析nvidia-smi输出的XML内容，提取GPU设备信息
   * @param xmlContent nvidia-smi命令输出的XML格式字符串
   * @return 解析完成的GPU设备信息对象
   * @throws YarnException 解析失败时抛出异常
   */
  public synchronized GpuDeviceInformation parseXml(String xmlContent)
      throws YarnException {
    InputSource inputSource = new InputSource(new StringReader(xmlContent));
    SAXSource source = new SAXSource(xmlReader, inputSource);
    try {
      return (GpuDeviceInformation) unmarshaller.unmarshal(source);
    } catch (JAXBException e) {
      String msg = "Failed to parse XML output of " +
          GPU_SCRIPT_REFERENCE + "!";
      LOG.error(msg, e);
      throw new YarnException(msg, e);
    }
  }
}