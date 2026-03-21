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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 队列ACL权限信息数据对象，用于RM WebUI展示队列权限配置信息
 * 封装了队列访问类型与对应的权限控制列表信息
 */
@XmlRootElement(name = "queueAcl")
@XmlAccessorType(XmlAccessType.FIELD)
public class QueueAclInfo {

    protected String accessType;
    protected String accessControlList;

    /**
     * JAXB反序列化需要的无参构造函数
     */
    public QueueAclInfo() {
        // JAXB needs this
    }

    /**
     * 构造队列ACL权限信息对象
     * @param accessType 访问类型
     * @param accessControlList 权限控制列表字符串
     */
    public QueueAclInfo(String accessType, String accessControlList) {
      this.accessType = accessType;
      this.accessControlList = accessControlList;
    }

    /**
     * 获取访问类型
     * @return 访问类型字符串
     */
    public String getAccessType() {
      return accessType;
    }

    /**
     * 获取权限控制列表
     * @return 权限控制列表字符串
     */
    public String getAccessControlList() {
      return accessControlList;
    }
}