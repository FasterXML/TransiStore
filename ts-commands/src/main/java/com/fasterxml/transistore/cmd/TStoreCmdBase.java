package com.fasterxml.transistore.cmd;

import static io.airlift.command.OptionType.GLOBAL;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSKeyConverter;
import com.fasterxml.transistore.client.BasicTSClient;
import com.fasterxml.transistore.client.BasicTSClientBootstrapper;
import com.fasterxml.transistore.client.BasicTSClientConfig;
import com.fasterxml.transistore.client.BasicTSClientConfigBuilder;
import com.fasterxml.transistore.client.ahc.AHCBasedClientBootstrapper;

import io.airlift.command.Option;

public abstract class TStoreCmdBase implements Runnable
{
    protected final static ObjectMapper mapper = new ObjectMapper();
    static {
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    protected final static BasicTSKeyConverter KEY_CONVERTER = BasicTSKeyConverter.defaultInstance();
    
    @Option(type = GLOBAL, name = { "-v", "--verbose"}, description = "Verbose mode")
    public boolean verbose = false;

    @Option(type = GLOBAL, name = { "-c", "--config-file" }, description = "Config file to use",
            arity=1 )
    public String configFile;

    @Option(type = GLOBAL, name = { "-t", "--text"}, description = "Textual output mode (vs JSON)")
    public boolean isTextual = true;

    @Option(type = GLOBAL, name = { "-j", "--json"}, description = "JSON output mode (vs textual)")
    public boolean isJSON = false;

    protected SkeletalServiceConfig getServiceConfig()
    {
        if (configFile == null || configFile.isEmpty()) {
            throw new IllegalArgumentException("Missing configuration file setting");
        }
        File f = new File(configFile);
        if (!f.exists() || !f.canRead()) {
            throw new IllegalArgumentException("Can not read config file '"+f.getAbsolutePath()+"'");
        }
        SkeletalServiceConfig config;        
        try {
            config = mapper.readValue(f, SkeletalServiceConfig.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Fail to read config file '"+f.getAbsolutePath()+"': "+e.getMessage());
        }
        if (config.ts.cluster.clusterNodes == null || config.ts.cluster.clusterNodes.isEmpty()) {
            throw new IllegalArgumentException("Missing cluster nodes definition");
        }
        return config;
    }

    protected BasicTSClientConfig getClientConfig() {
        return new BasicTSClientConfigBuilder()
            .setMinimalOksToSucceed(1)
            .setOptimalOks(2)
            .setMaxOks(2)
            .build();
    }

    protected BasicTSClient bootstrapClient(BasicTSClientConfig clientConfig, SkeletalServiceConfig serviceConfig)
    {
        BasicTSClientBootstrapper bs = new AHCBasedClientBootstrapper(clientConfig);
        for (SkeletalServiceConfig.Node node : serviceConfig.ts.cluster.clusterNodes) {
            bs = bs.addNode(node.ipAndPort);
        }
        if (verbose) {
            System.out.printf("Bootstrapping the client (%d nodes listed), wait up to 5 seconds\n",
                    serviceConfig.ts.cluster.clusterNodes.size());
        }
        final long nanoStart = System.nanoTime();
        try {
            BasicTSClient client = bs.buildAndInitCompletely(5);
            if (verbose) {
                long nanos = System.nanoTime() - nanoStart;
                double msecs = (double) (nanos >> 20);
                System.out.printf(" bootstrap complete in %.1f msecs\n", msecs);
            }
            return client;
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    protected BasicTSKey contentKey(String partition, String path)
    {
        return null;
    }

    protected ObjectWriter jsonWriter(Class<?> cls) {
        return mapper.writerWithType(cls);
    }
}
