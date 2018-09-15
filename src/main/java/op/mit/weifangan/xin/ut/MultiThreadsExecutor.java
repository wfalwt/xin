package op.mit.weifangan.xin.ut;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiThreadsExecutor {

    private ThreadPoolExecutor threadPool = null;

    /**
     * init thread pool
     */
    public void init() {
        threadPool = new ThreadPoolExecutor(
                10,
                30,
                100,
                TimeUnit.MINUTES,
                new ArrayBlockingQueue<Runnable>(5),
                new MultiThreadFactory(),
                new MultiRejectedExecutionHandler()
        );
    }


    public void destory() {
        if(threadPool != null) {
            threadPool.shutdownNow();
        }
    }


    public ExecutorService getCustomThreadPoolExecutor() {
        return this.threadPool;
    }

    private class MultiThreadFactory implements ThreadFactory {

        private AtomicInteger count = new AtomicInteger(0);

        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            String threadName = MultiThreadsExecutor.class.getSimpleName() + count.addAndGet(1);
            t.setName(threadName);
            return t;
        }
    }


    private class MultiRejectedExecutionHandler implements RejectedExecutionHandler {

        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            try {
                executor.getQueue().put(r);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}