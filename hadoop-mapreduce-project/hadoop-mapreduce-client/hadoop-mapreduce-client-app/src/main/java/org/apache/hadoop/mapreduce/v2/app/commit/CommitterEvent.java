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

package org.apache.hadoop.mapreduce.v2.app.commit;

import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * MapReduce作业提交器事件基类，封装输出提交流程中的各类事件
 * 继承YARN AbstractEvent，使用CommitterEventType区分事件类型
 */
public class CommitterEvent extends AbstractEvent<CommitterEventType> {

  /**
   * 构造提交器事件，指定事件类型
   * @param type 提交器事件类型
   */
  public CommitterEvent(CommitterEventType type) {
    super(type);
  }
}