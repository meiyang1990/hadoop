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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN ResourceManager Web UI 队列ACL权限信息集合数据访问对象
 * 用于封装所有队列的ACL权限信息，支持XML/JSON序列化返回给前端
 */
@XmlRootElement(name = "queueAcls")
@XmlAccessorType(XmlAccessType.FIELD)
public class QueueAclsInfo {

    // 存储单个队列ACL信息列表
    protected ArrayList<QueueAclInfo> queueAcl = new ArrayList<QueueAclInfo>();

    /**
     * JAXB反序列化需要的无参构造函数
     */
    public QueueAclsInfo() {
    } // JAXB needs this

    /**
     * 添加单个队列ACL信息到集合
     * @param queueAclInfo 单个队列ACL信息对象
     */
    public void add(QueueAclInfo queueAclInfo) {
        queueAcl.add(queueAclInfo);
    }

    /**
     * 获取所有队列ACL信息列表
     * @return 所有队列ACL信息组成的列表
     */
    public ArrayList<QueueAclInfo> getQueueAcls() {
        return queueAcl;
    }

    /**
     * 批量添加多个队列ACL信息到集合
     * @param queueAclsInfo 待添加的队列ACL信息列表
     */
    public void addAll(ArrayList<QueueAclInfo> queueAclsInfo) {
        queueAcl.addAll(queueAclsInfo);
    }

}