package azkaban.webapp.servlet;

import azkaban.executor.Executor;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.webapp.AzkabanWebServer;
import org.apache.log4j.Logger;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;

/**
 * @author yuhaiyang <yuhaiyang@kuaishou.com>
 * Created on 2019-01-23
 */
public class HeartbeatServlet extends AbstractAzkabanServlet {

    private final static Logger logger = Logger.getLogger(HeartbeatServlet.class);

    private ExecutorManagerAdapter executorManagerAdapter;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        final AzkabanWebServer server = (AzkabanWebServer) getApplication();
        this.executorManagerAdapter = server.getExecutorManager();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        final HashMap<String, Object> ret = new HashMap<>();
        final String id = getParam(req, "id");
        final String host = getParam(req, "host");
        final String port = getParam(req, "port");
        final String active = getParam(req, "isActive");
        Executor executor = new Executor(Integer.parseInt(id), host, Integer.parseInt(port), Boolean.parseBoolean(active));
        ajaxExecutorHeartbeat(executor, ret);
        if (ret != null) {
            this.writeJSON(resp, ret);
        }
    }

    private void ajaxExecutorHeartbeat(Executor executor, final HashMap<String, Object> ret) {
        try {
            executorManagerAdapter.updateExecutorHeartbeat(executor);
            ret.put("status", "ok");
        } catch (ExecutorManagerException e) {
            ret.put("error", e.getMessage());
        }
    }
}