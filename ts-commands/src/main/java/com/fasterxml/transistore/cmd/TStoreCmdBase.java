package com.fasterxml.transistore.cmd;

import io.airlift.command.Option;
import static io.airlift.command.OptionType.GLOBAL;

import java.io.*;
import java.util.List;
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
        mapper.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    protected final static Pattern SLASH_PATTERN = Pattern.compile("/");
    
    protected final static BasicTSKeyConverter KEY_CONVERTER = BasicTSKeyConverter.defaultInstance();

    protected final boolean _canPrintVerbose;

    @Option(type = GLOBAL, name = { "-v", "--verbose"}, description = "Verbose mode")
    public boolean verbose = false;

    /**
     * Multiple instances are allowed, but we only use the last one specified if so; hence
     * type of <code>String</code>
     */
    @Option(type = GLOBAL, name = { "-c", "--config-file" }, description = "Config file to use")
    public String configFile;

    @Option(type = GLOBAL, name = { "-s", "--server" },
            description = "Server node(s) (comma-separated, if multiple) to use for boostrap (overrides config file settings)")
    public List<String> servers;
    
    @Option(type = GLOBAL, name = { "-t", "--text"}, description = "Textual output mode (vs JSON)")
    public boolean isTextual = true;

    @Option(type = GLOBAL, name = { "-j", "--json"}, description = "JSON output mode (vs textual)")
    public boolean isJSON = false;


    @Option(type = GLOBAL, name = { "--calls"}, description = "Maximum number of calls to (try to) make (default -1 for 'maximum')")
    public int callsToMake = -1;

    /**
     * Lazily constructed client configuration
     */
    protected BasicTSClientConfig _clientConfig;
    
    protected SkeletalServiceConfig _serviceConfig;

    /**
     * @param canPrintVerbose Whether this command is allowed to print things to stdout
     *   in verbose mode
     */
    protected TStoreCmdBase(boolean canPrintVerbose) {
        _canPrintVerbose = canPrintVerbose;
    }
    
    protected SkeletalServiceConfig getServiceConfig()
    {
        if (_serviceConfig == null) {
            SkeletalServiceConfig config;
    
            // If we have server definition(s), can avoid reading config file
            if (servers != null && !servers.isEmpty()) {
                config = new SkeletalServiceConfig();
                // allow both multiple entries, and comma-separated lists
                for (String server : servers) {
                    for (String str : server.split(",")) {
                        config.ts.cluster.addNode(new IpAndPort(str));
                    }
                }
                if (_canPrintVerbose && verbose) {
                    System.out.printf("INFO: using %d server nodes: %s\n", config.ts.cluster.clusterNodes.size(),
                            config.ts.cluster.clusterNodes.toString());
                }
            } else if (configFile != null && !configFile.isEmpty()) {
                // Use the one specified last:
                File f = new File(configFile);
                if (!f.exists() || !f.canRead()) {
                    throw new IllegalArgumentException("Can not read config file '"+f.getAbsolutePath()+"'");
                }
                if (_canPrintVerbose && verbose) {
                    System.out.printf("INFO: using config file '%s'\n", f.getAbsolutePath());
                }
    
                try {
                    config = mapper.readValue(f, SkeletalServiceConfig.class);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Fail to read config file '"+f.getAbsolutePath()+"': "+e.getMessage());
                }
                if (config.ts.cluster.clusterNodes == null || config.ts.cluster.clusterNodes.isEmpty()) {
                    throw new IllegalArgumentException("Missing cluster nodes definition in config file '"+f.getAbsolutePath()+"'");
                }
            } else {
                throw new IllegalArgumentException("Missing settings for configuration file, or server(s) to contact; can not initialize client");
            }
            _serviceConfig = config;
        }
        return _serviceConfig;
    }
    
    protected BasicTSClientConfig getClientConfig() {
        if (_clientConfig == null) {
            BasicTSClientConfigBuilder b = new BasicTSClientConfigBuilder();

            if (callsToMake <= 0) {
                b = b.setMinimalOksToSucceed(1)
                    .setOptimalOks(2)
                    .setMaxOks(3);
            } else {
                b = b.setMinimalOksToSucceed(callsToMake)
                        .setOptimalOks(callsToMake)
                        .setMaxOks(callsToMake);
            }
            _clientConfig = b.build();
        }
        return _clientConfig;
    }

    protected BasicTSClient bootstrapClient()
    {
        BasicTSClientConfig clientConfig = getClientConfig();
        SkeletalServiceConfig serviceConfig = getServiceConfig();
        
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
                double msecs = nanos / (1000.0 * 1000.0);
                System.out.printf(" bootstrap complete in %.1f msecs\n", msecs);
            }
            // One more thing: adapt min/optimal/max counts, if need be
            int nodeCount = client.getCluster().getServerCount();
            if (nodeCount > 0) {
                clientConfig = clientConfig.verifyWithServerCount(nodeCount);
                if (clientConfig != _clientConfig) {
                    client = client.withConfig(clientConfig);
                    if (verbose) {
                        System.out.printf(" NOTE: change ClientConfig since only %d servers available: min/opt/max"
                                +" from %d/%d/%d to %d/%d/%d\n",
                                nodeCount,
                                _clientConfig.getOperationConfig().getMinimalOksToSucceed(),
                                _clientConfig.getOperationConfig().getOptimalOks(),
                                _clientConfig.getOperationConfig().getMaxOks(),

                                client.getOperationConfig().getMinimalOksToSucceed(),
                                client.getOperationConfig().getOptimalOks(),
                                client.getOperationConfig().getMaxOks());
                    }
                    _clientConfig = clientConfig;
                }
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
    /* Value formatting
    /**********************************************************************
     */
    
    protected String size(long l)
    {
        if (l <= 9999L) {
            return String.format("%5d", l);
        }
        int kB = (int) (l >> 10);
        if (kB < 100) {
            return String.format("%4.1fk", l / 1024.0); // 99.9k (5 chars)
        }
        if (kB < 1024) {
            return String.format(" %3dk", kB); // 999k
        }
        int mB = (kB >> 10);
        if (mB < 100) {
            return String.format("%4.1fM", kB / 1024.0); // 99.9M
        }
        if (mB < 1024) { // 100 - 999
            return String.format(" %dM", mB);
        }
        return String.format("%5.1fG", mB / 1024.0); // 125.4G
    }

    protected String ageMsecs(long msecs) {
        return ageSecs(msecs / 1000);
    }

    protected String ageSecs(long secs0)
    {
        int secs = (secs0 < Integer.MAX_VALUE) ? ((int) secs0) : Integer.MAX_VALUE;
        if (secs < 60) {
          if (secs < 10) {
               return new StringBuilder(6).append(' ').append(secs).append("s   ").toString();
          }
          return new StringBuilder(6).append(secs).append("s   ").toString();
        }
        int mins = (secs / 60);
        secs -= (mins * 60);
        if (mins < 60) {
            return String.format("%2dm%02d", mins, secs);
        }
        int h = (mins / 60);
        mins -= (h * 60);
        if (h < 24) { // even hours quite common, optimize
            if (mins == 0) {
                return String.format("%2dh  ", h);
            }
            return String.format("%2dh%02d", h, mins);
        }
        int d = (h / 24);
        h -= (d * 24);
        if (h == 0) { // even days common as well
            return String.format("%2dd   ", d);
        }
        return String.format("%2dd%02d", d, h);
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

    protected <T> T terminateWith(Throwable e)
    {
        // NOTE: printStackTrace actually prints exception itself so...
        if (e instanceof RuntimeException) {
            System.err.print("ERROR/");
            e.printStackTrace(System.err);
        } else {
            System.err.println("ERROR/"+e);
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
