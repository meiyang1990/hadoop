// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task;

/**
 * NodeManager删除任务类型枚举，定义了DeletionTask支持的删除任务类型。
 * 用于区分不同类型的删除操作，让删除服务可以根据类型路由到对应处理器处理。
 */
public enum DeletionTaskType {
  /** 删除本地文件任务 */
  FILE,
  /** 删除Docker容器任务 */
  DOCKER_CONTAINER
}