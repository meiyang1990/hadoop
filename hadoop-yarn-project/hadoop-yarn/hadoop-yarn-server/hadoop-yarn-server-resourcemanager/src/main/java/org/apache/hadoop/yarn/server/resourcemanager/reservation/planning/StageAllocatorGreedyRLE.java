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

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation.RLEOperator;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 基于贪心算法实现的预约阶段分配器，利用游程编码优化性能。
 * 贪心规则会从最左/最右侧的可能区间开始重复分配容器，相比基线实现性能更高。
 */

public class StageAllocatorGreedyRLE implements StageAllocator {

  // 标记是否从左开始分配，false则从右开始分配
  private final boolean allocateLeft;

  /**
   * 构造贪心分配器，指定分配方向
   * @param allocateLeft true从左分配，false从右分配
   */
  public StageAllocatorGreedyRLE(boolean allocateLeft) {
    this.allocateLeft = allocateLeft;
  }

  @Override
  public Map<ReservationInterval, Resource> computeStageAllocation(Plan plan,
      RLESparseResourceAllocation planLoads,
      RLESparseResourceAllocation planModifications, ReservationRequest rr,
      long stageEarliestStart, long stageDeadline, long period, String user,
      ReservationId oldId) throws PlanningException {

    // 总时长不足，提前终止分配
    if (stageEarliestStart + rr.getDuration() > stageDeadline) {
      return null;
    }

    Map<ReservationInterval, Resource> allocationRequests =
        new HashMap<ReservationInterval, Resource>();

    Resource totalCapacity = plan.getTotalCapacity();

    // 计算一组（gang）任务的总资源量
    Resource sizeOfGang =
        Resources.multiply(rr.getCapability(), rr.getConcurrency());
    long dur = rr.getDuration();
    long step = plan.getStep();

    // 将时长向上取整为计划步长的整数倍
    if (dur % step != 0) {
      dur += (step - (dur % step));
    }

    // 计算需要分配的任务组数
    int gangsToPlace = rr.getNumContainers() / rr.getConcurrency();

    // 从计划中获取指定时间范围内的可用资源
    RLESparseResourceAllocation netRLERes =
        plan.getAvailableResourceOverTime(user, oldId, stageEarliestStart,
            stageDeadline, period);

    // 扣除已修改占用的资源，得到净可用资源
    netRLERes =
        RLESparseResourceAllocation.merge(plan.getResourceCalculator(),
            totalCapacity, netRLERes, planModifications, RLEOperator.subtract,
            stageEarliestStart, stageDeadline);

    // 循环分配任务组，直到分配完成或区间不可用
    while (gangsToPlace > 0 && stageEarliestStart + dur <= stageDeadline) {

      // 初始化当前轮次最大可分配组数和约束点位置
      int maxGang = gangsToPlace;
      long minPoint = -1;

      // 获取当前时间范围内的资源分段映射
      NavigableMap<Long, Resource> partialMap =
          netRLERes.getRangeOverlapping(stageEarliestStart, stageDeadline)
              .getCumulative();

      // 如果是从右分配，反转映射遍历顺序
      if (!allocateLeft) {
        partialMap = partialMap.descendingMap();
      }

      Iterator<Entry<Long, Resource>> netIt = partialMap.entrySet().iterator();

      long oldT = stageDeadline;

      // 遍历资源分段，计算当前时间范围内最大可分配组数
      while (maxGang > 0 && netIt.hasNext()) {

        long t;
        Resource curAvailRes;

        Entry<Long, Resource> e = netIt.next();
        if (allocateLeft) {
          // 从左分配：计算当前分段起始时间
          t = Math.max(e.getKey(), stageEarliestStart);
          curAvailRes = e.getValue();
        } else {
          // 从右分配：计算当前分段起始时间
          t = oldT;
          oldT = e.getKey();
          // 反转映射后，higherEntry对应当前区间的可用资源
          curAvailRes = partialMap.higherEntry(t).getValue();
        }

        // 跳过资源为空的分段
        if (curAvailRes == null) {
          continue;
        }
        // 满足退出条件，终止当前轮次遍历
        if (exitCondition(t, stageEarliestStart, stageDeadline, dur)) {
          break;
        }

        // 计算当前分段可容纳的最大任务组数
        int curMaxGang =
            (int) Math.floor(Resources.divide(plan.getResourceCalculator(),
                totalCapacity, curAvailRes, sizeOfGang));
        curMaxGang = Math.min(gangsToPlace, curMaxGang);

        // 更新最大可分配组数，记录资源约束点位置
        if (curMaxGang <= maxGang) {
          maxGang = curMaxGang;
          minPoint = t;
        }
      }

      // 更新分配进度，记录已分配资源
      gangsToPlace =
          trackProgress(planModifications, rr, stageEarliestStart,
              stageDeadline, allocationRequests, dur, gangsToPlace, maxGang);

      // 更新下一轮次的搜索起始/结束时间
      if (allocateLeft) {
        // 从左分配：更新下一轮起始时间
        if(partialMap.higherKey(minPoint) == null){
          stageEarliestStart = stageEarliestStart + dur;
        } else {
          stageEarliestStart =
             Math.min(partialMap.higherKey(minPoint), stageEarliestStart + dur);
        }
      } else {
        // 从右分配：更新下一轮结束时间
        if(partialMap.higherKey(minPoint) == null){
          stageDeadline = stageDeadline - dur;
        } else {
          stageDeadline =
              Math.max(partialMap.higherKey(minPoint), stageDeadline - dur);
        }
      }
    }

    // 所有任务组分配完成，返回分配结果
    if (gangsToPlace == 0) {
      return allocationRequests;
    } else {
      // 分配失败，回滚已分配的资源修改
      for (Map.Entry<ReservationInterval, Resource> tempAllocation :
          allocationRequests.entrySet()) {
        planModifications.removeInterval(tempAllocation.getKey(),
            tempAllocation.getValue());
      }
      // 返回null表示分配失败
      return null;
    }

  }

  /**
   * 记录已分配的任务组，更新剩余待分配数量
   */
  private int trackProgress(RLESparseResourceAllocation planModifications,
      ReservationRequest rr, long stageEarliestStart, long stageDeadline,
      Map<ReservationInterval, Resource> allocationRequests, long dur,
      int gangsToPlace, int maxGang) {
    // 如果分配了至少一组任务
    if (maxGang > 0) {
      // 减少待分配组数
      gangsToPlace -= maxGang;

      // 计算本次分配的时间区间
      ReservationInterval reservationInt =
          computeReservationInterval(stageEarliestStart, stageDeadline, dur);
      // 计算本次分配的总资源量
      Resource reservationRes =
          Resources.multiply(rr.getCapability(), rr.getConcurrency() * maxGang);
      // 更新已修改资源占用，避免重复分配
      planModifications.addInterval(reservationInt, reservationRes);
      // 将本次分配加入结果
      allocationRequests.put(reservationInt, reservationRes);

    }
    return gangsToPlace;
  }

  /**
   * 根据分配方向计算预约时间区间
   */
  private ReservationInterval computeReservationInterval(
      long stageEarliestStart, long stageDeadline, long dur) {
    ReservationInterval reservationInt;
    if (allocateLeft) {
      // 从左分配：从起始时间开始分配
      reservationInt =
          new ReservationInterval(stageEarliestStart, stageEarliestStart + dur);
    } else {
      // 从右分配：从结束时间向前分配
      reservationInt =
          new ReservationInterval(stageDeadline - dur, stageDeadline);
    }
    return reservationInt;
  }


  /**
   * 判断是否满足退出遍历的条件
   */
  private boolean exitCondition(long t, long stageEarliestStart,
      long stageDeadline, long dur) {
    if (allocateLeft) {
      // 从左分配：当前时间超过分配结束位置则退出
      return t >= stageEarliestStart + dur;
    } else {
      // 从右分配：当前时间低于分配起始位置则退出
      return t < stageDeadline - dur;
    }
  }
}