package jianfei.javafun;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class WordCount {	
	public static long lineCount(String fileName){		
		long lineCount = 0;
        RandomAccessFile memoryMappedFile = null;
        long totalRead = 0;
		
        try {
			memoryMappedFile = new RandomAccessFile(fileName, "r");
	        FileChannel fc = memoryMappedFile.getChannel();
	        long fileSize = fc.size();
			int maxThreads =  Runtime.getRuntime().availableProcessors();
	        int numberWorkers = (int)(fileSize / Integer.MAX_VALUE) + 1;
	        //WorkSplitter ws = new WorkSplitter(Math.min(numberWorkers, maxThreads));
	        WorkSplitter ws = new WorkSplitter(numberWorkers);
	        
	        while (totalRead < fileSize) {
	        	long buffSize = Math.min((fileSize - totalRead), Integer.MAX_VALUE);
	        	MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_ONLY, totalRead, buffSize);
	        	ws.addWork(mbb);
	        	totalRead = totalRead + mbb.capacity();
	        	//lineCount += count(mbb);
	        }
	        
	        lineCount = ws.get();
	        
	        log("File size:{0} Total read:{1} Line Count:{2}",  fileSize , totalRead, lineCount);

        } catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			if (memoryMappedFile != null) {
				try {
					memoryMappedFile.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
        
        return totalRead;
	
	}
	
	public static void main(String[] args) {
		int threads =  Runtime.getRuntime().availableProcessors();
		log("Total processors:{0}", threads);
		Benchmark.BMK.star("CountLine");
		lineCount(args[0]);
		Benchmark.BMK.end("CountLine");
		Benchmark.BMK.report("CountLine");
	}
	
	public static class WorkSplitter {
		List<Future<Long>> counterResultes = new ArrayList<>();
		ExecutorService es;
		
		private WorkSplitter(int numberWorkers) {
			es = Executors.newFixedThreadPool(numberWorkers);
			System.out.println("Create a WorkSplitter with " + numberWorkers + " workers");
		}
		public static WorkSplitter build(int numberWorkers) {
			return new WorkSplitter(numberWorkers);
		}
		
		public WorkSplitter addWork(MappedByteBuffer mbb) {
			
			Future<Long> result = es.submit(()->{
				long count = count(mbb);
				System.out.println("Thread:" + Thread.currentThread().getName() + " finished");
				return count;
			});
			
			counterResultes.add(result);
			return this;
		}
		
		public long get() throws InterruptedException, ExecutionException {
			es.shutdown();
			long count = 0;
			for (Future<Long> f : counterResultes) {
				count = count + f.get();
			}
			return count;
		}
	}
	
	private static long count(MappedByteBuffer mbb) {
		long count = 0;
		while(mbb.hasRemaining()){
			if(mbb.get() == '\n')
				count++;
		}
		return count;
	}
	
	public static void log(String formatter, Object ... args){
		String enchancedFormatter = "[{" + args.length +"}]" + formatter;
		Object[] newArgs = new Object[args.length + 1];		
		System.arraycopy(args, 0, newArgs, 0, args.length);
		newArgs[newArgs.length - 1] = Thread.currentThread().getName();
		
		System.out.println(MessageFormat.format(enchancedFormatter, newArgs));
	}
}
