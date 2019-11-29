package com.fisc.infinispan.test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.concurrent.IsolationLevel;
import org.jgroups.JChannel;

public class MultipleInstanceReplicationTest
{
    private static final int NUMER_OF_INSTANCES = 25;
    private static final int START_PORT = 7010;
    private static final String HOSTNAME = "ind-hmu-dsk";
    private static final long TIME_TO_WAIT_FOR_REPLICATION = TimeUnit.MINUTES.toMillis( 1);

    public static void main( String[] args) throws Exception
    {
        List<EmbeddedCacheManager> cacheManagers = new ArrayList<>();
        try
        {
            int numberOfInstances = NUMER_OF_INSTANCES;
            String initialHosts = getInitialHosts( HOSTNAME, START_PORT, numberOfInstances);
            for (int port = 7010; port < 7010 + numberOfInstances; port++)
            {
                String jgroupsConfigXml = getJgroupsConfigXml( HOSTNAME, port, initialHosts,
                        NUMER_OF_INSTANCES - 1);
                EmbeddedCacheManager cacheManager = startCache( jgroupsConfigXml);
                cacheManager.getCache().put( "CacheKey" + (port - 7010 + 1), "NA");
                cacheManagers.add( cacheManager);
            }
            System.out.println( "Caches started");
            System.out.println( "Waiting for cache to stabilize");
            Thread.sleep( TIME_TO_WAIT_FOR_REPLICATION);
            System.out.println( "Logging cache state");
            for (int i = 0; i < cacheManagers.size(); i++)
            {
                List<String> sortedKeys = cacheManagers.get( i).getCache().keySet().stream()
                        .map( o -> (String) o).sorted().collect( Collectors.toList());
                for (Object key : sortedKeys)
                {
                    System.out.println( key);
                }
                System.out.println( "######################");
            }
            System.out.println( "Stopping cache");
        }
        finally
        {
            for (EmbeddedCacheManager embeddedCacheManager : cacheManagers)
            {
                embeddedCacheManager.stop();
            }
        }
    }

    private static EmbeddedCacheManager startCache( String jgroupsConfigXml) throws Exception
    {
        Configuration defaultCacheConfig = buildDefaultConfiguration();
        GlobalConfiguration globalConfig = buildGlobalConfiguration( jgroupsConfigXml);
        EmbeddedCacheManager embeddedCacheManager = new DefaultCacheManager( globalConfig,
                defaultCacheConfig);
        embeddedCacheManager.start();
        return embeddedCacheManager;
    }

    private static String getInitialHosts( String hostName, int startPort, int hostCount)
    {
        return IntStream.range( 0, hostCount).mapToObj( i -> hostName + "[" + (startPort + i) + "]")
                .collect( Collectors.joining( ","));
    }

    private static String getJgroupsConfigXml( String host, int port, String initialHosts, int portRange)
    {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><config>\r\n"
                + "  <TCP bind_addr=\"" + host + "\" bind_port=\"" + port
                + "\" bundler_type=\"no-bundler\" enable_diagnostics=\"false\" port_range=\"" + portRange
                + "\" send_buf_size=\"640k\" sock_conn_timeout=\"300\" thread_naming_pattern=\"pl\" thread_pool.keep_alive_time=\"60000\" thread_pool.max_threads=\"200\" thread_pool.min_threads=\"0\"/>\r\n"
                + "  <TCPPING async_discovery=\"true\" initial_hosts=\"" + initialHosts
                + "\" port_range=\"0\"/>\r\n"
                + "  <MERGE3 max_interval=\"30000\" min_interval=\"10000\"/>\r\n" + "  <FD_SOCK/>\r\n"
                + "  <FD_ALL interval=\"2000\" timeout=\"10000\" timeout_check_interval=\"1000\"/>\r\n"
                + "  <VERIFY_SUSPECT timeout=\"1000\"/>\r\n"
                + "  <ASYM_ENCRYPT asym_algorithm=\"RSA\" asym_keylength=\"2048\" sym_algorithm=\"AES/ECB/PKCS5Padding\" sym_keylength=\"256\"/>\r\n"
                + "  <pbcast.NAKACK2 resend_last_seqno=\"true\" use_mcast_xmit=\"false\" xmit_interval=\"100\" xmit_table_max_compaction_time=\"30000\" xmit_table_msgs_per_row=\"1024\" xmit_table_num_rows=\"50\"/>\r\n"
                + "  <UNICAST3 xmit_interval=\"100\" xmit_table_max_compaction_time=\"30000\" xmit_table_msgs_per_row=\"1024\" xmit_table_num_rows=\"50\"/>\r\n"
                + "  <pbcast.STABLE desired_avg_gossip=\"5000\" max_bytes=\"1M\" stability_delay=\"500\"/>\r\n"
                + "  <AUTH auth_class=\"org.jgroups.auth.SimpleToken\" auth_value=\"VrV3^JBQ@GNT\"/>\r\n"
                + "  <pbcast.GMS join_timeout=\"2000\" print_local_addr=\"false\"/>\r\n"
                + "  <UFC_NB max_credits=\"3m\" min_threshold=\"0.40\"/>\r\n"
                + "  <MFC_NB max_credits=\"3m\" min_threshold=\"0.40\"/>\r\n" + "  <FRAG3/>\r\n"
                + "</config>";
    }

    private static Configuration buildDefaultConfiguration()
    {
        ConfigurationBuilder configBuilder = new ConfigurationBuilder();
        configBuilder.clustering().cacheMode( CacheMode.REPL_SYNC);
        configBuilder.clustering().remoteTimeout( 15, TimeUnit.SECONDS);
        configBuilder.locking().isolationLevel( IsolationLevel.REPEATABLE_READ);
        configBuilder.clustering().stateTransfer().timeout( 3, TimeUnit.MINUTES);
        configBuilder.locking().lockAcquisitionTimeout( 2, TimeUnit.MINUTES);
        return configBuilder.build();
    }

    private static GlobalConfiguration buildGlobalConfiguration( String jgroupsConfigXml) throws Exception
    {
        GlobalConfigurationBuilder configBuilder = new GlobalConfigurationBuilder();
        configBuilder.transport().transport( getJGroupsTransport( jgroupsConfigXml))
                .clusterName( "FiscTestCluster");
        configBuilder.serialization().marshaller( getJavaSerializationMarshaller());
        return configBuilder.defaultCacheName( "FiscDefaultCache").build();
    }

    private static JGroupsTransport getJGroupsTransport( String jgroupsConfigXml) throws Exception
    {
        JChannel jchannel = new JChannel(
                new ByteArrayInputStream( jgroupsConfigXml.getBytes( StandardCharsets.UTF_8)));
        return new JGroupsTransport( jchannel);
    }

    private static JavaSerializationMarshaller getJavaSerializationMarshaller()
    {
        List<String> allowedClassPatterns = getRegexForSerializationAllowedClasses();
        ClassWhiteList whiteList = new ClassWhiteList( allowedClassPatterns);
        return new JavaSerializationMarshaller( whiteList);
    }

    private static List<String> getRegexForSerializationAllowedClasses()
    {
        List<String> allowedClassPatterns = new ArrayList<>();
        allowedClassPatterns.add( "java\\..*");
        allowedClassPatterns.add( "com\\.fisc\\..*");
        allowedClassPatterns.add( "com\\.fii\\..*");
        return allowedClassPatterns;
    }
}
