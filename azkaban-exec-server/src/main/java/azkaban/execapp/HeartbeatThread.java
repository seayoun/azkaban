package azkaban.execapp;

import azkaban.executor.*;
import azkaban.utils.Props;
import org.apache.log4j.Logger;

import javax.inject.Inject;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

/**
 * @author yuhaiyang <yuhaiyang@kuaishou.com>
 * Created on 2019-01-22
 */
public class HeartbeatThread implements Runnable {

    private static final Logger logger = Logger.getLogger(HeartbeatThread.class);
    private static final ExecutorApiGateway apiGateway = new ExecutorApiGateway(new ExecutorApiClient());

    private final String host;
    private final int port;

    private final ExecutorLoader executorLoader;

    public HeartbeatThread(Props props, ExecutorLoader executorLoader) throws MalformedURLException {
        URL url = new URL(props.getString(JobRunner.AZKABAN_WEBSERVER_URL));
        this.host = url.getHost();
        this.port = url.getPort();
        this.executorLoader = executorLoader;
    }

    @Override
    public void run() {
        try {
            Executor executor = executorLoader.fetchExecutor(AzkabanExecutorServer.getApp().getHost(), AzkabanExecutorServer.getApp().getPort());
            if (executor.isActive()) {
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("Heartbeat now %d", System.currentTimeMillis()));
                }
                Heartbeat heartbeat = new Heartbeat(executor, apiGateway);
                String result = heartbeat.heartbeat(this.host, this.port);
                if (result.contains("error")) {
                    logger.error(String.format("Heartbeat return error, error: %s", result.toString()));
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("Heartbeat result %s", result));
                    }
                }
            } else {
                logger.warn("Heartbeat failed for executor is unActive");
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }
}