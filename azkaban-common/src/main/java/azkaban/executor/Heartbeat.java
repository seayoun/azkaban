package azkaban.executor;


import azkaban.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author yuhaiyang <yuhaiyang@kuaishou.com>
 * Created on 2019-01-22
 */
public class Heartbeat {

    private final String HEARTBEAT_PATH = "heartbeat";

    private final int id;
    private final String host;
    private final int port;
    private final boolean isActive;

    private ExecutorApiGateway apiGateway;

    public Heartbeat(int id, String host, int port, boolean isActive, ExecutorApiGateway apiGateway) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.isActive = isActive;
        this.apiGateway = apiGateway;
    }

    public Heartbeat(Executor executor, ExecutorApiGateway apiGateway) {
        this.id = executor.getId();
        this.host = executor.getHost();
        this.port = executor.getPort();
        this.isActive = executor.isActive();
        this.apiGateway = apiGateway;
    }

    public String heartbeat(String targetHost, int targetPort) throws IOException {
        return this.apiGateway.callForJsonString(targetHost, targetPort, HEARTBEAT_PATH, this.collect());
    }

    private List<Pair<String, String>>  collect() {
        Pair<String, String> idPair = new Pair<>("id", String.valueOf(this.id));
        Pair<String, String> hostPair = new Pair<>("host", this.host);
        Pair<String, String> portPair = new Pair<>("port", String.valueOf(this.port));
        Pair<String, String> isActivePair = new Pair<>("isActive", String.valueOf(this.isActive));
        List<Pair<String, String>> params = new ArrayList<>();
        params.add(idPair);
        params.add(hostPair);
        params.add(portPair);
        params.add(isActivePair);
        return params;
    }
}