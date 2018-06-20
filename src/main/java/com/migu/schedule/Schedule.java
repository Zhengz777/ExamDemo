package com.migu.schedule;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/*
 *类名和方法不能修改
 */
public class Schedule {

	// 节点列表
	private List<Integer> nodeList;
	
	// 节点运行任务key:nodeId,value:taskId
	private Map<Integer, Integer> nodeTaskMap;
	
	// 任务
	private Map<Integer, Integer> taskMap;
	
	// 任务挂起队列
	private Map<Integer, Integer> taskWaitMap;
	

	public int init() {

		nodeList = new ArrayList<Integer>();
		nodeTaskMap = new HashMap<Integer, Integer>();
		taskMap = new HashMap<Integer, Integer>();
		taskWaitMap = new HashMap<Integer, Integer>();
		
		// 初始化成功
		return ReturnCodeKeys.E001;
	}

	public int registerNode(int nodeId) {
		if (null == nodeList) {
			nodeList = new ArrayList<Integer>();
		}
		// 服务节点编号非法
		if (0 >= nodeId) {
			return ReturnCodeKeys.E004;
		}
		// 服务节点已注册
		if (nodeList.contains(nodeId)) {
			return ReturnCodeKeys.E005;
		}

		nodeList.add(nodeId);
		// 注册成功
		return ReturnCodeKeys.E003;
	}

	public int unregisterNode(int nodeId) {
		if (null != nodeTaskMap && nodeTaskMap.containsKey(nodeId))
		{
			int taskId = nodeTaskMap.get(nodeId);			
			nodeTaskMap.remove(nodeId);
			
			if (taskMap.containsKey(taskId))
			{
				int consumption = taskMap.get(taskId);
				taskWaitMap.put(taskId, consumption);
			}
		}
		
		// TODO 服务列为空
		if (null == nodeList) {
			return ReturnCodeKeys.E000;
		}
		// 服务节点编号非法
		if (0 >= nodeId) {
			return ReturnCodeKeys.E004;
		}
		// 服务节点不存在
		if (!nodeList.contains(nodeId)) {
			return ReturnCodeKeys.E007;
		}
		
		
		// 服务节点注销成功
		return ReturnCodeKeys.E006;
	}

	public int addTask(int taskId, int consumption) {
		if (null == taskWaitMap) {
			taskWaitMap = new HashMap<Integer, Integer>();
		}
		// 任务编号非法
		if (0 >= taskId) {
			return ReturnCodeKeys.E009;
		}
		// 任务已添加
		if (taskWaitMap.containsKey(taskId)) {
			return ReturnCodeKeys.E010;
		}

		taskWaitMap.put(taskId, consumption);
		// 任务添加成功
		return ReturnCodeKeys.E008;
	}

	public int deleteTask(int taskId) {
		// TODO 将在挂起队列中的任务 或 运行在服务节点上的任务删除

		// TODO 任务列为空
		if (null == taskWaitMap) {
			return ReturnCodeKeys.E000;
		}
		// 任务编号非法
		if (0 >= taskId) {
			return ReturnCodeKeys.E009;
		}
		// 任务不存在
		if (!taskWaitMap.containsKey(taskId)) {
			return ReturnCodeKeys.E012;
		}

		// 任务删除成功
		return ReturnCodeKeys.E011;
	}

	public int scheduleTask(int threshold) {
		// 调度阈值非法
		if (0 > threshold) {
			return ReturnCodeKeys.E002;
		}

		// TODO 方法未实现
		return ReturnCodeKeys.E000;
	}

	public int queryTaskStatus(List<TaskInfo> tasks) {
		// TODO 方法未实现
		return ReturnCodeKeys.E000;
	}
	
	// 实现消耗值最优分组
	public Map<Integer, Integer> getBestGroup()
	{
		Map<Integer, Integer> mapResult = new HashMap<Integer, Integer>();
		
		// 节点数量
		int nodeNum = nodeList.size();
		
		int count = taskWaitMap.size();		
		Integer[] consumptionArray = new Integer[count];
		
		int sum = 0;
		
		for (int i = 0; i < count; i++)
		{
			sum += consumptionArray[i];
		}
		
		Arrays.sort(consumptionArray);
		float avg = (float)sum / count;
		
		List<Integer> sortConsumption = new LinkedList<Integer>();
		for (int i = count - 1; i >= 0; i--)
		{
			sortConsumption.add(consumptionArray[i]);
		}
		
		List<Integer> list1 = new ArrayList<Integer>();
		List<Integer> list2 = new ArrayList<Integer>();
		List<Integer> list3 = new ArrayList<Integer>();
		float temp = avg;
		for (int consumptionTemp : sortConsumption)
		{
			if (temp < 0)
			{
				continue;
			}
			temp = temp - consumptionTemp;
			list1.add(consumptionTemp);
		}
		for (int consumptionTemp : list1)
		{
			sortConsumption.remove(consumptionTemp);
		}
		temp = avg;
		for (int consumptionTemp : sortConsumption)
		{
			if (temp < 0)
			{
				continue;
			}
			temp = temp - consumptionTemp;
			list2.add(consumptionTemp);
		}
		for (int consumptionTemp : list2)
		{
			sortConsumption.remove(consumptionTemp);
		}
		for (int consumptionTemp : sortConsumption)
		{
			list3.add(consumptionTemp);
		}
		
		return mapResult;
	}

}
