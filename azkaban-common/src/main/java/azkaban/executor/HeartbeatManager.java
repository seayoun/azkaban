package azkaban.executor;

import org.apache.log4j.Logger;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yuhaiyang <yuhaiyang@kuaishou.com>
 * Created on 2019-01-22
 */
public class HeartbeatManager {

    private static final Logger logger = Logger.getLogger(HeartbeatManager.class);

    private final ActiveExecutors activeExecutors;
    private final Map<Executor, Long> heartbeats;
    private final ScheduledExecutorService heartbeatCheckService;
    private final ExecutorLoader executorLoader;

    private final long heartbeatMaxTime;
    private final long heartbeatCheckInterval;

    private final ReentrantLock lock = new ReentrantLock(false);

    public HeartbeatManager(ActiveExecutors activeExecutors, ExecutorLoader executorLoader, long heartbeatMaxTime, long heartbeatCheckInterval) {
        this.activeExecutors = activeExecutors;
        this.executorLoader = executorLoader;
        this.heartbeatMaxTime = heartbeatMaxTime;
        this.heartbeatCheckInterval = heartbeatCheckInterval;
        this.heartbeats = new HashMap<>();
        heartbeatCheckService = setupHeartbeatService();
    }

    /**
     * init all executor heartbeant time
     * @param currentTimeMs
     */
    public void setup(long currentTimeMs) {
        lock.lock();
        try {
            for (Executor executor: activeExecutors.getAll()) {
                heartbeats.clear();
                heartbeats.put(executor, currentTimeMs);
            }
        } finally {
            lock.unlock();
        }
        logger.info(String.format("init all executor heartbeat time %d",  currentTimeMs));
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("heartbeats=%s", heartbeats.toString()));
        }
    }

    /**
     * init check heartbeat thread
     * @return
     */
    private ScheduledExecutorService setupHeartbeatService() {
        return Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("heartbeat-check-thread");
                return t;
            }
        });
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    public void start() {
        this.heartbeatCheckService.scheduleWithFixedDelay(new HeartbeatCheckThread(),
                heartbeatCheckInterval, heartbeatCheckInterval, TimeUnit.MILLISECONDS);
        logger.info("Start HeartbeatManager success.");
    }

    /**
     * refresh heartbeat time every executor
     * @param executor
     * @param heartbeat
     */
    public void updateExecutorHeartbeat(Executor executor, long heartbeat) {
        if (!executor.isActive()) {
            return;
        }
        lock.lock();
        try {
            if (heartbeats.get(executor) == null) {
                logger.warn(String.format("Please reload manual, find not loaded executor heartbeat, executor info : %s", executor.toString()));
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("Executor heartbeat, executor %s", executor.toString()));
                }
                heartbeats.put(executor, heartbeat);
            }
        } finally {
            lock.unlock();
        }
    }

    public void removeExecutor(Executor executor) {
        lock.lock();
        try {
            this.heartbeats.remove(executor);
        } finally {
            lock.unlock();
        }
    }

    public void shutdown() {
        this.heartbeatCheckService.shutdown();
    }

    /**
     * executor check thread
     */
    private class HeartbeatCheckThread implements Runnable {
        @Override
        public void run() {
            try {
                List<Executor> deadExecutors = new ArrayList<>();
                Collection<Executor> executorCollection = activeExecutors.getAll();
                if (executorCollection.size() > 0) {
                    for (Executor executor : executorCollection) {
                        Long lastHeartbeat = heartbeats.get(executor);
                        if (null == lastHeartbeat) {
                            if (executor.isActive()) {
                                long currentTimeMs = System.currentTimeMillis();
                                heartbeats.put(executor, currentTimeMs);
                                logger.info(String.format("Find new active executor, time %d, executor %s", currentTimeMs, executor.toString()));
                            } else {
                                // no situation
                                logger.error(String.format("Find unActive executor when heartbeatCheck: %s", executor.toString()));
                            }
                        } else {
                            long elapsed = System.currentTimeMillis() - lastHeartbeat;
                            if (elapsed > heartbeatMaxTime) {
                                logger.warn(String.format("Heartbeat timeout, heartbeatMaxTime=%d , notHeartbeatTime=%d, executor=%s",
                                        heartbeatMaxTime, elapsed, executor.toString()));
                                deadExecutors.add(executor);
                            } else {
                                if (logger.isDebugEnabled()) {
                                    logger.debug(String.format("Executor heartbeat normally, executor: %s , lastHeartbeat time : %d", executor.toString(), lastHeartbeat));
                                }
                            }
                        }
                    }
                    if (!deadExecutors.isEmpty()) {
                        try {
                            for (Executor executor : deadExecutors) {
                                executor.setActive(false);
                                executorLoader.updateExecutor(executor);
                                removeExecutor(executor);
                            }
                        } catch (ExecutorManagerException e) {
                            logger.warn(e);
                        }
                        try {
                            activeExecutors.setupExecutors();
                            //TODO  yuhaiyang: redispatch the flow on the executor
                        } catch (ExecutorManagerException e) {
                            logger.error(e);
                        }
                    }
                } else {
                    logger.warn("No active executors found.");
                }
            } catch (Exception e) {
                logger.error(e);
            }
        }
    }
}