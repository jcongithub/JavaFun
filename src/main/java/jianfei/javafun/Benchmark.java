package jianfei.javafun;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class Benchmark {
	public static Benchmark BMK = new Benchmark();
	private HashMap<String, Long> taskStartTime = new HashMap<>();
	private HashMap<String, LinkedList<Long>> taskElapsedTime = new HashMap<>();

	private Benchmark() {
	}

	public void star(String taskName) {
		if (taskStartTime.containsKey(taskName)) {
			System.out.println("Task:" + taskName + " already started. Ignore this start");
		} else {
			taskStartTime.put(taskName, System.currentTimeMillis());
			if (!taskElapsedTime.containsKey(taskName)) {
				taskElapsedTime.put(taskName, new LinkedList<Long>());
			}
		}
	}

	public void end(String taskName) {
		if(taskStartTime.containsKey(taskName)) {
			long elapsed = System.currentTimeMillis() - taskStartTime.get(taskName);
			taskElapsedTime.get(taskName).add(elapsed);
			
			taskStartTime.remove(taskName);
		} else {
			System.out.println("Task:" + taskName + " not started yet.");
		}
	}
	
	public void report(String taskName) {
		if(!taskElapsedTime.containsKey(taskName)) {
			System.out.println("Task:" + taskName + " never run. No performance benchmark data to report");
			return;
		}
		
		List<Long> elapsedList = taskElapsedTime.get(taskName);
		int count = elapsedList.size();
		long sum = elapsedList.stream().mapToLong(i->i.longValue()).sum();
		System.out.println("Task:" + taskName + " Run Count:" + count + " Average Time: " + formatTime(sum/count));
	}
	
	public String formatTime(long millis) {
		long seconds = millis / 1000;
		millis = millis - seconds * 1000;
		
		long minutes = seconds / 60;
		seconds = seconds - minutes * 60;
		
		return minutes + ":" + seconds + ":" + millis;
	}
	
}
