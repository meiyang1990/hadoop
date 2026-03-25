// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.QueueState;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.XMLUtils;

import static org.apache.hadoop.mapred.QueueManager.toFullPropertyName;

import org.xml.sax.SAXException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.DOMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;

/**
 * MapReduce 队列配置解析器，负责解析 mapred-queues.xml 配置文件，构建层次化队列结构
 * <p/>
 * 支持队列嵌套（层次化队列）特性，XML根元素必须为 <queues>，解析后生成完整的队列层级树
 */
class QueueConfigurationParser {
  private static final Logger LOG =
      LoggerFactory.getLogger(QueueConfigurationParser.class);
  
  private boolean aclsEnabled = false;

  //默认根队列
  protected Queue root = null;

  //mapred-queues.xml 配置标签常量定义
  static final String NAME_SEPARATOR = ":";
  static final String QUEUE_TAG = "queue";
  static final String ACL_SUBMIT_JOB_TAG = "acl-submit-job";
  static final String ACL_ADMINISTER_JOB_TAG = "acl-administer-jobs";

  // 该标签已废弃，配置文件中的该值不再使用
  // 开启队列ACL需在 mapred-site.xml 中设置 mapreduce.cluster.acls.enabled
  @Deprecated
  static final String ACLS_ENABLED_TAG = "aclsEnabled";

  static final String PROPERTIES_TAG = "properties";
  static final String STATE_TAG = "state";
  static final String QUEUE_NAME_TAG = "name";
  static final String QUEUES_TAG = "queues";
  static final String PROPERTY_TAG = "property";
  static final String KEY_TAG = "key";
  static final String VALUE_TAG = "value";

  /**
   * 默认构造函数
   */
  QueueConfigurationParser() {
    
  }

  /**
   * 根据文件路径构造队列配置解析器，解析配置文件构建队列树
   * @param confFile 队列配置文件路径
   * @param areAclsEnabled 是否开启ACL权限控制
   */
  QueueConfigurationParser(String confFile, boolean areAclsEnabled) {
    aclsEnabled = areAclsEnabled;
    File file = new File(confFile).getAbsoluteFile();
    if (!file.exists()) {
      throw new RuntimeException("Configuration file not found at " +
                                 confFile);
    }
    InputStream in = null;
    try {
      in = new BufferedInputStream(new FileInputStream(file));
      loadFrom(in);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } finally {
      IOUtils.closeStream(in);
    }
  }

  /**
   * 根据输入流构造队列配置解析器，解析XML输入构建队列树
   * @param xmlInput XML配置输入流
   * @param areAclsEnabled 是否开启ACL权限控制
   */
  QueueConfigurationParser(InputStream xmlInput, boolean areAclsEnabled) {
    aclsEnabled = areAclsEnabled;
    loadFrom(xmlInput);
  }

  private void loadFrom(InputStream xmlInput) {
    try {
      this.root = loadResource(xmlInput);
    } catch (ParserConfigurationException e) {
      throw new RuntimeException(e);
    } catch (SAXException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  void setAclsEnabled(boolean aclsEnabled) {
    this.aclsEnabled = aclsEnabled;
  }

  boolean isAclsEnabled() {
    return aclsEnabled;
  }

  Queue getRoot() {
    return root;
  }

  void setRoot(Queue root) {
    this.root = root;
  }

  /**
   * 从输入流加载并解析XML配置，生成根队列节点
   * @param resourceInput XML配置输入流
   * @return 解析后的根队列节点
   * @throws ParserConfigurationException 解析器配置错误
   * @throws SAXException XML解析错误
   * @throws IOException IO读取错误
   */
  protected Queue loadResource(InputStream resourceInput)
    throws ParserConfigurationException, SAXException, IOException {
    DocumentBuilderFactory docBuilderFactory =
        XMLUtils.newSecureDocumentBuilderFactory();

    // 忽略XML文件中的注释
    docBuilderFactory.setIgnoringComments(true);

    // 开启命名空间支持，允许XML包含
    docBuilderFactory.setNamespaceAware(true);
    try {
      docBuilderFactory.setXIncludeAware(true);
    } catch (UnsupportedOperationException e) {
      LOG.info(
        "Failed to set setXIncludeAware(true) for parser "
          + docBuilderFactory
          + NAME_SEPARATOR + e);
    }
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc = null;
    Element queuesNode = null;

    doc = builder.parse(resourceInput);
    queuesNode = doc.getDocumentElement();
    return this.parseResource(queuesNode);
  }

  /**
   * 解析XML根queues节点，构建顶层队列结构
   * @param queuesNode XML根queues元素节点
   * @return 解析完成的虚拟根队列节点
   */
  private Queue parseResource(Element queuesNode) {
    Queue rootNode = null;
    try {
      // 校验根节点必须是<queues>标签
      if (!QUEUES_TAG.equals(queuesNode.getTagName())) {
        LOG.info("Bad conf file: top-level element not <queues>");
        throw new RuntimeException("No queues defined ");
      }
      NamedNodeMap nmp = queuesNode.getAttributes();
      Node acls = nmp.getNamedItem(ACLS_ENABLED_TAG);

      // 废弃标签提示，告知用户需在mapred-site.xml配置ACL开关
      if (acls != null) {
        LOG.warn("Configuring " + ACLS_ENABLED_TAG + " flag in " +
            QueueManager.QUEUE_CONF_FILE_NAME + " is not valid. " +
            "This tag is ignored. Configure " +
            MRConfig.MR_ACLS_ENABLED + " in mapred-site.xml. See the " +
            " documentation of " + MRConfig.MR_ACLS_ENABLED +
            ", which is used for enabling job level authorization and " +
            " queue level authorization.");
      }

      NodeList props = queuesNode.getChildNodes();
      if (props == null || props.getLength() <= 0) {
        LOG.info(" Bad configuration no queues defined ");
        throw new RuntimeException(" No queues defined ");
      }

      // 遍历所有顶层队列节点
      for (int i = 0; i < props.getLength(); i++) {
        Node propNode = props.item(i);
        if (!(propNode instanceof Element)) {
          continue;
        }

        // 根节点下只允许存在<queue>标签
        if (!propNode.getNodeName().equals(QUEUE_TAG)) {
          LOG.info("At root level only \" queue \" tags are allowed ");
          throw
            new RuntimeException("Malformed xml document no queue defined ");
        }

        Element prop = (Element) propNode;
        // 递归构建队列层次结构
        Queue q = createHierarchy("", prop);
        if(rootNode == null) {
          rootNode = new Queue();
          rootNode.setName("");
        }
        rootNode.addChild(q);
      }
      return rootNode;
    } catch (DOMException e) {
      LOG.info("Error parsing conf file: " + e);
      throw new RuntimeException(e);
    }
  }

  /**
   * 递归构建队列层次结构，从XML节点生成Queue对象树
   * @param parent 父队列完整名称
   * @param queueNode 当前队列XML元素节点
   * @return 构建完成的当前队列对象
   */
  private Queue createHierarchy(String parent, Element queueNode) {

    if (queueNode == null) {
      return null;
    }
    String name = "";
    Queue newQueue = new Queue();
    Map<String, AccessControlList> acls =
      new HashMap<String, AccessControlList>();

    NodeList fields = queueNode.getChildNodes();
    validate(queueNode);
    List<Element> subQueues = new ArrayList<Element>();

    String submitKey = "";
    String adminKey = "";
    
    for (int j = 0; j < fields.getLength(); j++) {
      Node fieldNode = fields.item(j);
      if (!(fieldNode instanceof Element)) {
        continue;
      }
      Element field = (Element) fieldNode;
      // 解析队列名称，生成完整限定名（父队列名:当前队列名）
      if (QUEUE_NAME_TAG.equals(field.getTagName())) {
        String nameValue = field.getTextContent();
        // 校验队列名称合法性：不允许为空，不允许包含分隔符冒号
        if (field.getTextContent() == null ||
          field.getTextContent().trim().equals("") ||
          field.getTextContent().contains(NAME_SEPARATOR)) {
          throw new RuntimeException("Improper queue name : " + nameValue);
        }

        if (!parent.equals("")) {
          name += parent + NAME_SEPARATOR;
        }
        // 生成完整限定名 格式：父队列名:当前队列名
        name += nameValue;
        newQueue.setName(name);
        // 生成ACL属性完整名称
        submitKey = toFullPropertyName(name,
            QueueACL.SUBMIT_JOB.getAclName());
        adminKey = toFullPropertyName(name,
            QueueACL.ADMINISTER_JOBS.getAclName());
      }

      // 收集子队列节点，后续递归处理
      if (QUEUE_TAG.equals(field.getTagName()) && field.hasChildNodes()) {
        subQueues.add(field);
      }
      // ACL开启时解析提交任务和管理任务的权限列表
      if(isAclsEnabled()) {
        if (ACL_SUBMIT_JOB_TAG.equals(field.getTagName())) {
          acls.put(submitKey, new AccessControlList(field.getTextContent()));
        }

        if (ACL_ADMINISTER_JOB_TAG.equals(field.getTagName())) {
          acls.put(adminKey, new AccessControlList(field.getTextContent()));
        }
      }

      // 解析队列自定义属性
      if (PROPERTIES_TAG.equals(field.getTagName())) {
        Properties properties = populateProperties(field);
        newQueue.setProperties(properties);
      }

      // 解析队列状态（运行/停止）
      if (STATE_TAG.equals(field.getTagName())) {
        String state = field.getTextContent();
        newQueue.setState(QueueState.getState(state));
      }
    }
    
    // 默认ACL：如果未配置，设置为空权限列表
    if (!acls.containsKey(submitKey)) {
      acls.put(submitKey, new AccessControlList(" "));
    }
    
    if (!acls.containsKey(adminKey)) {
      acls.put(adminKey, new AccessControlList(" "));
    }
    
    // 设置ACL权限
    newQueue.setAcls(acls);

    // 递归处理所有子队列，添加到当前队列
    for(Element field:subQueues) {
      newQueue.addChild(createHierarchy(newQueue.getName(), field));
    }
    return newQueue;
  }

  /**
   * 解析properties标签，填充队列自定义属性
   * @param field propertiesXML元素节点
   * @return 解析完成的属性集合
   */
  private Properties populateProperties(Element field) {
    Properties props = new Properties();

    NodeList propfields = field.getChildNodes();

    for (int i = 0; i < propfields.getLength(); i++) {
      Node prop = propfields.item(i);

      if (!(prop instanceof Element)) {
        continue;
      }

      if (PROPERTY_TAG.equals(prop.getNodeName())) {
        if (prop.hasAttributes()) {
          NamedNodeMap nmp = prop.getAttributes();
          // 同时存在key和value属性时才添加属性
          if (nmp.getNamedItem(KEY_TAG) != null && nmp.getNamedItem(
            VALUE_TAG) != null) {
            props.setProperty(
              nmp.getNamedItem(KEY_TAG).getTextContent(), nmp.getNamedItem(
                VALUE_TAG).getTextContent());
          }
        }
      }
    }
    return props;
  }

  /**
   * 校验队列XML节点格式合法性：
   * 1. 必须包含name标签
   * 2. 包含子queue的父队列不能同时配置ACL和状态标签（ACL和状态仅叶子节点配置）
   * @param node 当前队列XML节点
   */
  private void validate(Node node) {

    NodeList fields = node.getChildNodes();

    Set<String> siblings = new HashSet<String>();
    for (int i = 0; i < fields.getLength(); i++) {
      if (!(fields.item(i) instanceof Element)) {
        continue;
      }
      siblings.add((fields.item(i)).getNodeName());
    }

    // 必须配置队列名称
    if(! siblings.contains(QUEUE_NAME_TAG)) {
      throw new RuntimeException(
        " Malformed xml formation queue name not specified ");
    }

    // 如果当前队列包含子队列，则不允许同时配置ACL和状态标签
    if (siblings.contains(QUEUE_TAG) && (
      siblings.contains(ACL_ADMINISTER_JOB_TAG) ||
        siblings.contains(ACL_SUBMIT_JOB_TAG) ||
        siblings.contains(STATE_TAG)
    )) {
      throw new RuntimeException(
        " Malformed xml formation queue tag and acls " +
          "tags or state tags are siblings ");
    }
  }


  /**
   * 从完整队列名称中提取当前队列的短名称（去掉父队列前缀）
   * @param fullQName 完整队列名称（父:子）
   * @return 当前队列短名称
   */
  private static String getSimpleQueueName(String fullQName) {
    int index = fullQName.lastIndexOf(NAME_SEPARATOR);
    if (index < 0) {
      return fullQName;
    }
    return fullQName.substring(index + 1, fullQName.length());
  }

  /**
   * 根据JobQueueInfo递归生成队列XML元素，包含名称、属性、状态和子队列
   * @param document XML文档对象，用于创建元素
   * @param jqi 队列信息对象
   * @return 生成的队列XML元素
   */
  static Element getQueueElement(Document document, JobQueueInfo jqi) {

    // 创建队列标签
    Element q = document.createElement(QUEUE_TAG);

    // 创建队列名称标签
    Element qName = document.createElement(QUEUE_NAME_TAG);
    qName.setTextContent(getSimpleQueueName(jqi.getQueueName()));
    q.appendChild(qName);

    // 创建队列自定义属性标签
    Properties props = jqi.getProperties();
    Element propsElement = document.createElement(PROPERTIES_TAG);
    if (props != null) {
      Set<String> propList = props.stringPropertyNames();
      for (String prop : propList) {
        Element propertyElement = document.createElement(PROPERTY_TAG);
        propertyElement.setAttribute(KEY_TAG, prop);
        propertyElement.setAttribute(VALUE_TAG, (String) props.get(prop));
        propsElement.appendChild(propertyElement);
      }
    }
    q.appendChild(propsElement);

    // 创建队列状态标签（非undefined状态才生成）
    String queueState = jqi.getState().getStateName();
    if (queueState != null
        && !queueState.equals(QueueState.UNDEFINED.getStateName())) {
      Element qStateElement = document.createElement(STATE_TAG);
      qStateElement.setTextContent(queueState);
      q.appendChild(qStateElement);
    }

    // 递归创建子队列标签
    List<JobQueueInfo> children = jqi.getChildren();
    if (children != null) {
      for (JobQueueInfo child : children) {
        q.appendChild(getQueueElement(document,