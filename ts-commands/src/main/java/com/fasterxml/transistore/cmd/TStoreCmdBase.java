package com.fasterxml.transistore.cmd;

import io.airlift.command.Option;
import static io.airlift.command.OptionType.GLOBAL;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectWriter;

import com.fasterxml.clustermate.json.ClusterMateObjectMapper;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.transistore.basic.BasicTSKey;
import com.fasterxml.transistore.basic.BasicTSKeyConverter;
import com.fasterxml.transistore.client.BasicTSClient;
import com.fasterxml.transistore.client.BasicTSClientBootstrapper;
import com.fasterxml.transistore.client.BasicTSClientConfig;
import com.fasterxml.transistore.client.BasicTSClientConfigBuilder;

import com.fasterxml.transistore.client.ahc.AHCBasedClientBootstrapper;
//import com.fasterxml.transistore.client.jdk.JDKBasedClientBootstrapper;

public abstract class TStoreCmdBase implements Runnable
{
    protected final static ClusterMateObjectMapper mapper = new ClusterMateObjectMapper();
    static {
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    protected final static Pattern SLASH_PATTERN = Pattern.compile("/");
    
    protected final static BasicTSKeyConverter KEY_CONVERTER = BasicTSKeyConverter.defaultInstance();

    protected final boolean _canPrintVerbose;

    @Option(type = GLOBAL, name = { "-v", "--verbose"}, description = "Verbose mode")
    public boolean verbose = false;

    @Option(type = GLOBAL, name = { "-c", "--config-file" }, description = "Config file to use")
    public String configFile;

    @Option(type = GLOBAL, name = { "-s", "--server" },
            description = "Server node(s) (comma-separated) to use for boostrap (overrides config file settings)")
    public String server;
    
    @Option(type = GLOBAL, name = { "-t", "--text"}, description = "Textual output mode (vs JSON)")
    public boolean isTextual = true;

    @Option(type = GLOBAL, name = { "-j", "--json"}, description = "JSON output mode (vs textual)")
    public boolean isJSON = false;

    /**
     * @param canPrintVerbose Whether this command is allowed to print things to stdout
     *   in verbose mode
     */
    protected TStoreCmdBase(boolean canPrintVerbose) {
        _canPrintVerbose = canPrintVerbose;
    }
    
    protected SkeletalServiceConfig getServiceConfig()
    {
        if (configFile == null || configFile.isEmpty()) {
            throw new IllegalArgumentException("Missing configuration file setting");
        }
        // Use the one specified last:
        File f = new File(configFile);
        if (!f.exists() || !f.canRead()) {
            throw new IllegalArgumentException("Can not read config file '"+f.getAbsolutePath()+"'");
        }
        if (_canPrintVerbose && verbose) {
            System.out.printf("INFO: using config file '%s'", f.getAbsolutePath());
        }

        SkeletalServiceConfig config;

        // If we have server definition(s), can avoid reading config file
        if (server != null && !server.isEmpty()) {
            config = new SkeletalServiceConfig();
            for (String str : server.split(",")) {
                config.ts.cluster.addNode(new IpAndPort(str));
            }
            
        } else {
            try {
                config = mapper.readValue(f, SkeletalServiceConfig.class);
            } catch (Exception e) {
                throw new IllegalArgumentException("Fail to read config file '"+f.getAbsolutePath()+"': "+e.getMessage());
            }
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
//        BasicTSClientBootstrapper bs = new JDKBasedClientBootstrapper(clientConfig);
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

    /*
    /**********************************************************************
    /* Key conversions
    /**********************************************************************
     */
    
    protected BasicTSKey contentKey(String external) {
        return KEY_CONVERTER.stringToKey(external);
    }
    
    protected BasicTSKey contentKey(String partition, String path) {
        return KEY_CONVERTER.construct(partition, path);
    }

    protected BasicTSKey contentKey(StorableKey rawKey) {
        return KEY_CONVERTER.rawToEntryKey(rawKey);
    }

    /*
    /**********************************************************************
    /* JSON helpers
    /**********************************************************************
     */
    
    protected ObjectWriter jsonWriter(Class<?> cls) {
        return mapper.writerWithType(cls);
    }

    protected JsonGenerator jsonGenerator(OutputStream out) throws IOException {
        return mapper.getFactory().createGenerator(out);
    }

    /*
    /**********************************************************************
    /* Error reporting
    /**********************************************************************
     */

    protected void warn(String template, Object... args)
    {
        String str = (args.length == 0) ? template : String.format(template, args);
        if (!str.endsWith("\n")) {
            str += "\n";
        }
        System.err.println("WARN: "+str);
    }

    protected void error(String template, Object... args)
    {
        String str = (args.length == 0) ? template : String.format(template, args);
        if (!str.endsWith("\n")) {
            str += "\n";
        }
        System.err.println("ERROR: "+str);
    }

    protected <T> T terminateWith(Throwable e)
    {
        System.err.printf("ERROR: (%s): %s", e.getClass().getName(), e.getMessage());
        if (e instanceof RuntimeException) {
            e.printStackTrace(System.err);
        }
        System.exit(1);
        return null;
    }
    
    /*
    /**********************************************************************
    /* Path handling
    /**********************************************************************
     */
    
    /**
     * Helper method used for converting local (usually) relative filenames
     * into server-side paths.
     */
    protected static String pathFromFile(File f)
    {
        StringBuilder sb = new StringBuilder();
        _pathFrom(f, sb);
        return sb.toString();
    }
    
    protected File appendPath(File base, String path)
    {
        // can either trim leading/trailing slahs, or just skip...
        for (String segment : SLASH_PATTERN.split(path)) {
            if (segment.length() > 0) {
                base = new File(base, segment);
            }
        }
        return base;
    }
    
    /*
    /**********************************************************************
    /* Internal helper methods
    /**********************************************************************
     */
    
    protected static void _pathFrom(File f, StringBuilder sb)
    {
        String name = f.getName();
        // Not sure what's the best way to do this; but we do need ignore "." and ".." somehow, so..
        if ("..".equals(name)) {
            return;
        }
        File parent = f.getParentFile();
        if (parent != null) {
            _pathFrom(parent, sb);
        }
        if (!".".equals(name)) {
            sb.append('/');
            sb.append(f.getName());
        }
    }
}
