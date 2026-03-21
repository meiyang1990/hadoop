// 这个文件已经全部加上中文注释
/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.VisibleForTesting;
import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

/**
 * 从JSON配置文件加载用户/用户组到网络流量标记ID的映射关系管理器实现
 * 用于YARN NodeManager网络流量隔离功能，为容器分配对应的net_cls cgroup类ID
 */
public class NetworkTagMappingJsonManager implements NetworkTagMappingManager {

  /** net_cls cgroup类ID的格式要求，必须为0xAAAABBBB形式 */
  private static final String FORMAT_NET_CLS_CLASS_ID = "0x[0-9]{8}";

  private NetworkTagMapping networkTagMapping = null;

  @Override
  public void initialize(Configuration conf) {
    // 从配置获取JSON映射文件路径，使用默认值兜底
    String mappingJsonFile = conf.get(
        YarnConfiguration.NM_NETWORK_TAG_MAPPING_FILE_PATH,
        YarnConfiguration.DEFAULT_NM_NETWORK_RESOURCE_TAG_MAPPING_FILE_PATH);
    // 路径为空时抛出异常，要求必须配置
    if (mappingJsonFile == null || mappingJsonFile.isEmpty()) {
      throw new YarnRuntimeException("To use NetworkTagMappingJsonManager,"
          + " we have to set the configuration:" +
          YarnConfiguration.NM_NETWORK_TAG_MAPPING_FILE_PATH);
    }
    // 使用Jackson解析JSON配置文件
    ObjectMapper mapper = new ObjectMapper();
    try {
      networkTagMapping = mapper.readValue(new File(mappingJsonFile),
          NetworkTagMapping.class);
    } catch (Exception e) {
      throw new YarnRuntimeException(e);
    }

    // JSON解析结果为空抛出异常
    if (networkTagMapping == null) {
      throw new YarnRuntimeException("Fail to load the specific JSON file: "
          + mappingJsonFile);
    }

    // 依次验证用户、用户组和默认配置的合法性
    networkTagMapping.validateUsers();
    networkTagMapping.validateGroups();
    networkTagMapping.validateDefaultClass();
  }

  @Override
  public String getNetworkTagHexID(Container container) {
    // 优先查找用户级别配置的标记ID
    String userNetworkTagID = this.networkTagMapping.getUserNetworkTagID(
        container.getUser());
    if (userNetworkTagID != null) {
      return userNetworkTagID;
    }

    // 用户未配置，查找用户所属用户组的配置
    UserGroupInformation userUGI = UserGroupInformation.createRemoteUser(
        container.getUser());
    List<Group> groups = this.networkTagMapping.getGroups();
    for(Group group : groups) {
      if (userUGI.getGroupsSet().contains(group.getGroupName())) {
        return group.getNetworkTagID();
      }
    }

    // 用户和用户组都未配置，返回默认标记ID
    return this.networkTagMapping.getDefaultNetworkTagID();
  }

  /**
   * 封装从JSON加载的网络标记映射关系，包含用户、用户组映射和默认值，并提供验证功能
   *
   */
  @VisibleForTesting
  @Private
  public static class NetworkTagMapping {
    @JsonProperty("users")
    private List<User> users = new LinkedList<>();
    @JsonProperty("groups")
    private List<Group> groups = new LinkedList<>();
    @JsonProperty("default-network-tag-id")
    private String defaultNetworkTagID;
    @JsonIgnore
    private final Pattern pattern = Pattern.compile(FORMAT_NET_CLS_CLASS_ID);

    public NetworkTagMapping() {}

    public List<User> getUsers() {
      return this.users;
    }

    public void setUsers(List<User> users) {
      this.users = users;
    }

    public void addUser(User user) {
      this.users.add(user);
    }

    public String getUserNetworkTagID(String userName) {
      for (User user : users) {
        if (userName.equals(user.getUserName())) {
          return user.getNetworkTagID();
        }
      }
      return null;
    }

    public List<Group> getGroups() {
      return this.groups;
    }

    public void setGroups(List<Group> groups) {
      this.groups = groups;
    }

    public void addGroup(Group group) {
      this.groups.add(group);
    }

    public String getDefaultNetworkTagID() {
      return this.defaultNetworkTagID;
    }

    public void setDefaultNetworkTagID(String defaultNetworkTagID) {
      this.defaultNetworkTagID = defaultNetworkTagID;
    }

    private boolean containsUser(String user, List<User> userList) {
      for (User existing : userList) {
        if (user.equals(existing.getUserName())) {
          return true;
        }
      }
      return false;
    }

    private boolean containsGroup(String group, List<Group> groupList) {
      for (Group existing : groupList) {
        if (group.equals(existing.getGroupName())) {
          return true;
        }
      }
      return false;
    }

    /**
     * 验证用户配置合法性：检查标记ID格式，拆分多用户配置，去重保留第一个配置
     */
    public void validateUsers() {
      List<User> validateUsers = new LinkedList<>();
      for(User user : this.users) {
        // 验证标记ID格式是否符合要求
        Matcher m = pattern.matcher(user.getNetworkTagID());
        if (!m.matches()) {
          throw new YarnRuntimeException(
              "User-network-tag-id mapping configuraton error. "
              + "The user:" + user.getUserName()
              + " 's configured network-tag-id:" + user.getNetworkTagID()
              + " does not match the '0xAAAABBBB' format.");
        }
        // 支持逗号分隔多个用户名共用同一个标记ID
        String[] userSplits = user.getUserName().split(",");
        if (userSplits.length > 1) {
          String networkTagID = user.getNetworkTagID();
          for(String split : userSplits) {
            // 去重，只保留第一个配置的用户
            if (!containsUser(split.trim(), validateUsers)) {
              User addUsers = new User(split.trim(), networkTagID);
              validateUsers.add(addUsers);
            }
          }
        } else {
          // 单个用户名，去重后添加
          if (!containsUser(user.getUserName(), validateUsers)) {
            validateUsers.add(user);
          }
        }
      }
      this.users = validateUsers;
    }

    /**
     * 验证用户组配置合法性：检查标记ID格式，去重保留第一个配置
     */
    public void validateGroups() {
      List<Group> validateGroups = new LinkedList<>();
      for(Group group : this.groups) {
        // 去重，只保留第一个配置的用户组
        if (!containsGroup(group.getGroupName(), validateGroups)) {
          // 验证标记ID格式
          Matcher m = pattern.matcher(group.getNetworkTagID());
          if (!m.matches()) {
            throw new YarnRuntimeException(
                "Group-network-tag-id mapping configuraton error. "
                + "The group:" + group.getGroupName()
                + " 's configured network-tag-id:" + group.getNetworkTagID()
                + " does not match the '0xAAAABBBB' format.");
          }
          validateGroups.add(group);
        }
      }
      this.groups = validateGroups;
    }

    /**
     * 验证默认标记ID配置合法性：检查非空和格式要求
     */
    public void validateDefaultClass() {
      // 检查默认ID非空要求
      if (getDefaultNetworkTagID() == null ||
          getDefaultNetworkTagID().isEmpty()) {
        throw new YarnRuntimeException("Missing value for defaultNetworkTagID."
            + " We have to set non-empty value for defaultNetworkTagID");
      }
      // 验证格式
      Matcher m = pattern.matcher(getDefaultNetworkTagID());
      if (!m.matches()) {
        throw new YarnRuntimeException("Configuration error on "
            + "default-network-tag-id. The configured default-network-tag-id:"
            + getDefaultNetworkTagID()
            + " does not match the '0xAAAABBBB' format.");
      }
    }
  }

  /**
   * 封装单个用户到网络标记ID的映射
   *
   */
  @VisibleForTesting
  @Private
  public static class User {
    @JsonProperty("name")
    private String userName;
    @JsonProperty("network-tag-id")
    private String networkTagID;

    public User() {}

    public User(String userName, String networkTagID) {
      this.setUserName(userName);
      this.setNetworkTagID(networkTagID);
    }

    public String getUserName() {
      return userName;
    }
    public void setUserName(String userName) {
      this.userName = userName;
    }
    public String getNetworkTagID() {
      return networkTagID;
    }
    public void setNetworkTagID(String networkTagID) {
      this.networkTagID = networkTagID;
    }
  }

  /**
   * 封装单个用户组到网络标记ID的映射
   *
   */
  @VisibleForTesting
  @Private
  public static class Group {
    @JsonProperty("name")
    private String groupName;
    @JsonProperty("network-tag-id")
    private String networkTagID;

    public Group() {}

    public String getGroupName() {
      return groupName;
    }
    public void setGroupName(String groupName) {
      this.groupName = groupName;
    }

    public String getNetworkTagID() {
      return networkTagID;
    }
    public void setNetworkTagID(String networkTagID) {
      this.networkTagID = networkTagID;
    }
  }

  @Private
  @VisibleForTesting
  public NetworkTagMapping getNetworkTagMapping() {
    return this.networkTagMapping;
  }
}