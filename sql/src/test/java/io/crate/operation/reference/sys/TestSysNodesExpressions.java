/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */
package io.crate.operation.reference.sys;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.crate.Build;
import io.crate.Version;
import io.crate.metadata.GlobalReferenceResolver;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.sys.SysExpression;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operation.Input;
import io.crate.operation.reference.sys.node.NodeVersionExpression;
import io.crate.operation.reference.sys.node.SysNodeExpression;
import io.crate.operation.reference.sys.node.SysNodeExpressionModule;
import io.crate.operation.reference.sys.node.fs.NodeFsDataExpression;
import io.crate.operation.reference.sys.node.fs.NodeFsExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.network.NetworkService;
import org.elasticsearch.monitor.network.NetworkStats;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.monitor.sigar.SigarService;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.threadpool.ThreadPool;
import org.hyperic.sigar.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class TestSysNodesExpressions {

    private Injector injector;
    private ReferenceResolver resolver;

    private boolean onWindows = false;
    private boolean sigarAvailable = true;
    private boolean isDataNode = true;

    class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(Settings.class).toInstance(ImmutableSettings.EMPTY);

            ClusterService clusterService = mock(ClusterService.class);
            bind(ClusterService.class).toInstance(clusterService);


            OsService osService = mock(OsService.class);
            OsStats osStats = mock(OsStats.class);
            when(osService.stats()).thenReturn(osStats);
            when(osStats.loadAverage()).thenAnswer(new Answer<double[]>() {
                @Override
                public double[] answer(InvocationOnMock invocation) throws Throwable {
                    if (onWindows) {
                        return new double[0];          // windows behaviour
                    } else {
                        return new double[]{1, 5, 15}; // unix behaviour
                    }
                }
            });
            ByteSizeValue byteSizeValue = mock(ByteSizeValue.class);
            when(byteSizeValue.bytes()).thenReturn(12345342234L);
            when(byteSizeValue.toString()).thenReturn("11.4gb");

            OsStats.Mem mem = mock(OsStats.Mem.class);
            when(osStats.mem()).thenReturn(mem);
            when(mem.actualFree()).thenReturn(byteSizeValue);
            when(mem.actualUsed()).thenReturn(byteSizeValue);
            when(mem.usedPercent()).thenReturn((short) 22);
            when(mem.freePercent()).thenReturn((short) 78);

            bind(OsService.class).toInstance(osService);

            NodeService nodeService = mock(NodeService.class);
            NodeStats nodeStats = mock(NodeStats.class);
            when(nodeService.stats()).thenReturn(nodeStats);
            when(nodeStats.getHostname()).thenReturn("localhost");

            DiscoveryNode node = mock(DiscoveryNode.class);
            when(nodeStats.getNode()).thenReturn(node);

            when(nodeStats.getOs()).thenReturn(osStats);
            when(osStats.uptime()).thenReturn(new TimeValue(3600000));
            OsStats.Cpu cpu = mock(OsStats.Cpu.class);
            when(osStats.cpu()).thenReturn(cpu);
            when(cpu.sys()).thenReturn((short) 2);
            when(cpu.user()).thenReturn((short) 4);
            when(cpu.idle()).thenReturn((short) 94);
            when(cpu.stolen()).thenReturn((short) 10);

            ProcessStats processStats = mock(ProcessStats.class);
            when(nodeStats.getProcess()).thenReturn(processStats);
            when(processStats.getOpenFileDescriptors()).thenReturn(42L);

            NodeInfo nodeInfo = mock(NodeInfo.class);
            when(nodeService.info()).thenReturn(nodeInfo);

            ProcessInfo processInfo = mock(ProcessInfo.class);
            when(nodeInfo.getProcess()).thenReturn(processInfo);
            when(processInfo.getMaxFileDescriptors()).thenReturn(1000L);

            Discovery discovery = mock(Discovery.class);
            bind(Discovery.class).toInstance(discovery);
            when(discovery.localNode()).thenReturn(node);
            when(node.getId()).thenReturn("node-id-1");
            when(node.getName()).thenReturn("node 1");
            TransportAddress transportAddress = new InetSocketTransportAddress("localhost", 44300);
            when(node.address()).thenReturn(transportAddress);

            NetworkStats.Tcp tcp = mock(NetworkStats.Tcp.class, new Answer<Long>() {
                @Override
                public Long answer(InvocationOnMock invocation) throws Throwable {
                    return 42L;
                }
            });
            NetworkStats networkStats = mock(NetworkStats.class);
            when(networkStats.tcp()).thenReturn(tcp);
            NetworkService networkService = mock(NetworkService.class);
            when(networkService.stats()).thenReturn(networkStats);
            bind(NetworkService.class).toInstance(networkService);

            bind(NodeService.class).toInstance(nodeService);

            NodeEnvironment nodeEnv = mock(NodeEnvironment.class);
            File[] dataLocations = new File[]{ new File("/foo"), new File("/bar") };
            when(nodeEnv.hasNodeFile()).then(new Answer<Boolean>() {
                @Override
                public Boolean answer(InvocationOnMock invocation) throws Throwable {
                    return isDataNode;
                }
            });
            when(nodeEnv.nodeDataLocations()).thenReturn(dataLocations);
            bind(NodeEnvironment.class).toInstance(nodeEnv);

            Sigar sigar = mock(Sigar.class);
            SigarService sigarService = mock(SigarService.class);
            when(sigarService.sigarAvailable()).then(new Answer<Boolean>() {
                @Override
                public Boolean answer(InvocationOnMock invocation) throws Throwable {
                    return sigarAvailable;
                }
            });

            FileSystem fsFoo = mock(FileSystem.class);
            when(fsFoo.getDevName()).thenReturn("/dev/sda1");
            when(fsFoo.getDirName()).thenReturn("/foo");
            when(fsFoo.getType()).thenReturn(FileSystem.TYPE_LOCAL_DISK);

            FileSystem fsBar = mock(FileSystem.class);
            when(fsBar.getDevName()).thenReturn("/dev/sda2");
            when(fsBar.getDirName()).thenReturn("/bar");
            when(fsBar.getType()).thenReturn(FileSystem.TYPE_LOCAL_DISK);


            FileSystem fsFiltered = mock(FileSystem.class);
            when(fsFiltered.getType()).thenReturn(FileSystem.TYPE_UNKNOWN);
            when(fsFiltered.getDevName()).thenReturn(("/dev/filtered"));
            when(fsFiltered.getDirName()).thenReturn(("/filtered"));

            FileSystemMap map = mock(FileSystemMap.class);
            when(map.getMountPoint("/foo")).thenReturn(fsFoo);
            when(map.getMountPoint("/bar")).thenReturn(fsBar);
            when(map.getMountPoint("/filtered")).thenReturn(fsFiltered);
            FileSystemUsage usage = mock(FileSystemUsage.class, new Answer<Long>() {
                @Override
                public Long answer(InvocationOnMock invocation) throws Throwable {
                    return 42L;
                }
            });

            try {
                when(sigar.getFileSystemList()).thenReturn(new FileSystem[]{fsFoo, fsBar, fsFiltered});
                when(sigar.getFileSystemMap()).thenReturn(map);
                when(sigar.getFileSystemUsage(anyString())).thenReturn(usage);
                assertThat(sigar.getFileSystemUsage("/"), is(usage));
            } catch (SigarException e) {
                e.printStackTrace();
            }
            when(sigarService.sigar()).thenReturn(sigar);
            bind(SigarService.class).toInstance(sigarService);
            try {
                assertThat(sigarService.sigar().getFileSystemMap(), is(map));
            } catch (SigarException e) {
                e.printStackTrace();
            }

            HttpInfo httpInfo = mock(HttpInfo.class);
            when(nodeInfo.getHttp()).thenReturn(httpInfo);
            BoundTransportAddress boundTransportAddress = new BoundTransportAddress(
                    new InetSocketTransportAddress("localhost", 44200),
                    new InetSocketTransportAddress("localhost", 44200)
            );
            when(httpInfo.address()).thenReturn(boundTransportAddress);

            JvmService jvmService = mock(JvmService.class);
            JvmStats jvmStats = mock(JvmStats.class);
            JvmStats.Mem jvmStatsMem = mock(JvmStats.Mem.class);
            ByteSizeValue heapByteSizeValueMax = mock(ByteSizeValue.class);
            when(heapByteSizeValueMax.bytes()).thenReturn(123456L);
            when(jvmStatsMem.getHeapMax()).thenReturn(heapByteSizeValueMax);
            when(jvmStatsMem.getHeapUsed()).thenReturn(heapByteSizeValueMax);

            when(jvmStats.mem()).thenReturn(jvmStatsMem);
            when(jvmService.stats()).thenReturn(jvmStats);
            bind(JvmService.class).toInstance(jvmService);

            bind(ReferenceResolver.class).to(GlobalReferenceResolver.class).asEagerSingleton();

            ThreadPool threadPool = new ThreadPool(getClass().getName());
            bind(ThreadPool.class).toInstance(threadPool);
        }
    }

    @Before
    public void setUp() throws Exception {
        injector = new ModulesBuilder().add(
                new TestModule(),
                new SysNodeExpressionModule()
        ).createInjector();
        resolver = injector.getInstance(ReferenceResolver.class);
    }

    @Test
    public void testLoad() throws Exception {

        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "load");
        SysObjectReference load = (SysObjectReference) resolver.getImplementation(ident);

        Map<String, Object> v = load.value();
        assertNull(v.get("something"));
        assertEquals(1D, v.get("1"));
        assertEquals(5D, v.get("5"));
        assertEquals(15D, v.get("15"));

        Input ci = load.getChildImplementation("1");
        assertEquals(1D, ci.value());
        ci = load.getChildImplementation("5");
        assertEquals(5D, ci.value());
        ci = load.getChildImplementation("15");
        assertEquals(15D, ci.value());
    }

    @Test
    public void testWindowsLoad() throws Exception {
        onWindows = true;
        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "load");
        SysObjectReference load = (SysObjectReference) resolver.getImplementation(ident);
        Map<String, Object> windowsValue = load.value();
        assertNull(windowsValue.get("1"));
        assertNull(windowsValue.get("5"));
        assertNull(windowsValue.get("10"));
        onWindows = false;
    }

    @Test
    public void testName() throws Exception {

        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "name");
        SysExpression<String> name = (SysExpression<String>) resolver.getImplementation(ident);

        assertEquals(new BytesRef("node 1"), name.value());
    }

    @Test
    public void testId() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "id");
        SysExpression<BytesRef> id = (SysExpression<BytesRef>) resolver.getImplementation(ident);
        assertEquals(new BytesRef("node-id-1"), id.value());
    }

    @Test
    public void testHostname() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "hostname");
        SysExpression<BytesRef> hostname = (SysExpression<BytesRef>) resolver.getImplementation(ident);
        assertEquals(new BytesRef("localhost"), hostname.value());
    }

    @Test
    public void testPorts() throws Exception {

        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "port");
        SysObjectReference port = (SysObjectReference) resolver.getImplementation(ident);

        Map<String, Object> v = port.value();
        assertEquals(44200, v.get("http"));
        assertEquals(44300, v.get("transport"));
    }

    @Test
    public void testMemory() throws Exception {

        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "mem");
        SysObjectReference mem = (SysObjectReference) resolver.getImplementation(ident);

        Map<String, Object> v = mem.value();

        assertEquals(12345342234L, v.get("free"));
        assertEquals(new Short("78"), v.get("free_percent"));

        assertEquals(12345342234L, v.get("used"));
        assertEquals(new Short("22"), v.get("used_percent"));
    }

    @Test
    public void testHeap() throws Exception {

        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "heap");
        SysObjectReference heap = (SysObjectReference) resolver.getImplementation(ident);

        Map<String, Object> v = heap.value();

        assertEquals(123456L, v.get("max"));
        assertEquals(123456L, v.get("used"));
        assertEquals(0L, v.get("free"));
    }

    @Test
    public void testFs() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, NodeFsExpression.NAME);
        SysObjectReference fs = (SysObjectReference) resolver.getImplementation(ident);

        Joiner.MapJoiner mapJoiner = Joiner.on(", ").withKeyValueSeparator(":");
        Map<String, Object> v = fs.value();
        Map<String, Object> total = (Map<String, Object>) v.get("total");
        assertThat(mapJoiner.join(total),
                is("reads:84, bytes_written:84, bytes_read:84, available:86016, " +
                        "writes:84, used:84, size:86016"));
        Object[] disks = (Object[]) v.get("disks");
        assertThat(disks.length, is(2));
        Map<String, Object> disk0 = (Map<String, Object>) disks[0];
        assertThat((String)disk0.get("dev"), is("/dev/sda1"));
        assertThat((Long)disk0.get("size"), is(42L*1024));

        Map<String, Object> disk1 = (Map<String, Object>) disks[1];
        assertThat((String)disk1.get("dev"), is("/dev/sda2"));
        assertThat((Long)disk0.get("used"), is(42L*1024));

        Object[] data = (Object[]) v.get("data");
        assertThat(data.length, is(2));
        assertThat((String)((Map<String, Object>)data[0]).get("dev"), is("/dev/sda1"));
        assertThat((String)((Map<String, Object>)data[0]).get("path"), is("/foo"));

        assertThat((String)((Map<String, Object>)data[1]).get("dev"), is("/dev/sda2"));
        assertThat((String)((Map<String, Object>)data[1]).get("path"), is("/bar"));

        ident = new ReferenceIdent(SysNodesTableInfo.IDENT, NodeFsExpression.NAME, ImmutableList.of(NodeFsDataExpression.NAME, "dev"));
        SysExpression<Object[]> fsData = (SysExpression<Object[]>)resolver.getImplementation(ident);
        for (Object arrayElement : fsData.value()) {
            assertThat(arrayElement, instanceOf(BytesRef.class));
        }

    }

    @Test
    public void testFsWithoutSigar() throws Exception {
        sigarAvailable = false;
        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, NodeFsExpression.NAME);
        SysObjectReference fs = (SysObjectReference) resolver.getImplementation(ident);

        Joiner.MapJoiner mapJoiner = Joiner.on(", ").withKeyValueSeparator(":");
        Map<String, Object> v = fs.value();
        Map<String, Object> total = (Map<String, Object>) v.get("total");

        assertThat(mapJoiner.join(total),
                is("reads:-1, bytes_written:-1, bytes_read:-1, available:-1, " +
                        "writes:-1, used:-1, size:-1"));
        Object[] disks = (Object[]) v.get("disks");
        assertThat(disks.length, is(0));

        Object[] data = (Object[]) v.get("data");
        assertThat(data.length, is(0));
        sigarAvailable = true;
    }

    @Test
    public void testFsDataOnNonDataNode() throws Exception {
        isDataNode = false;
        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, NodeFsExpression.NAME,
                ImmutableList.of("data"));
        SysObjectArrayReference fs = (SysObjectArrayReference) resolver.getImplementation(ident);
        assertThat(fs.value().length, is(0));
        isDataNode = true;
    }



    @Test
    public void testVersion() throws Exception {

        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "version");
        SysObjectReference version = (SysObjectReference) resolver.getImplementation(ident);

        Map<String, Object> v = version.value();
        assertEquals(Version.CURRENT.number(), v.get("number"));
        assertEquals(Build.CURRENT.hash(), v.get("build_hash"));
        assertEquals(Version.CURRENT.snapshot, v.get("build_snapshot"));

    }

    @Test
    public void testNetwork() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "network");
        SysObjectReference networkRef = (SysObjectReference) resolver.getImplementation(ident);

        Map<String, Object> networkStats = networkRef.value();
        assertThat(Joiner.on(", ").withKeyValueSeparator("=").join(networkStats),
                is("tcp={" +
                        "packets={sent=42, rst_sent=42, received=42, retransmitted=42, errors_received=42}, " +
                        "connections={accepted=42, dropped=42, initiated=42, embryonic_dropped=42, curr_established=42}" +
                        "}"));
    }

    @Test
    public void testNetworkTCP() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "network", Arrays.asList("tcp"));
        SysObjectReference tcpRef = (SysObjectReference) resolver.getImplementation(ident);

        Map<String, Object> tcpStats = tcpRef.value();

        assertThat(tcpStats, instanceOf(Map.class));
        assertThat(Joiner.on(", ").withKeyValueSeparator("=").join(tcpStats),
                is("packets={sent=42, rst_sent=42, received=42, retransmitted=42, errors_received=42}, " +
                        "connections={accepted=42, dropped=42, initiated=42, embryonic_dropped=42, curr_established=42}"));
    }


    @Test
    public void testCpu() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "os");
        SysObjectReference os = (SysObjectReference)resolver.getImplementation(ident);

        Map<String, Object> v = os.value();
        assertEquals(3600000L, v.get("uptime"));

        Map<String, Short> cpuObj = new HashMap<>(5);
        cpuObj.put("system", (short) 2);
        cpuObj.put("user", (short) 4);
        cpuObj.put("idle", (short) 94);
        cpuObj.put("used", (short) 6);
        cpuObj.put("stolen", (short) 10);
        assertEquals(cpuObj, v.get("cpu"));
    }

    @Test
    public void testProcess() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "process");
        SysObjectReference processRef = (SysObjectReference)resolver.getImplementation(ident);

        Map<String, Object> v = processRef.value();
        assertEquals(42L, (long) v.get("open_file_descriptors"));
        assertEquals(1000L, (long) v.get("max_open_file_descriptors"));
    }

    @Test
    public void testNestedBytesRefExpressionsString() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "version");
        SysObjectReference version = (SysObjectReference) resolver.getImplementation(ident);

        ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "version", Arrays.asList("number"));
        SysNodeExpression<BytesRef> versionNumber = (SysNodeExpression<BytesRef>)resolver.getImplementation(ident);

        assertThat(version.value().get(NodeVersionExpression.NUMBER), instanceOf(String.class));
        assertThat(versionNumber.value(), is(new BytesRef(version.value().get(NodeVersionExpression.NUMBER).toString())));

    }
}
