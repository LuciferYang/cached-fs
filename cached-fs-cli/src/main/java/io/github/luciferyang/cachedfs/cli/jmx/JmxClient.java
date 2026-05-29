/*
 * Copyright (c) 2026 The cached-fs Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.luciferyang.cachedfs.cli.jmx;

import io.github.luciferyang.cachedfs.hadoop.mbean.CachedFsBootstrapMXBean;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * Opens a JMX connection to a target JVM and returns a typed {@link CachedFsBootstrapMXBean} proxy.
 *
 * <p>Two modes:
 *
 * <ul>
 *   <li><b>Remote (URL given):</b> connects to a {@code
 *       service:jmx:rmi:///jndi/rmi://host:port/jmxrmi} (or similar) service URL via {@link
 *       JMXConnectorFactory}. {@link #close} tears the connector down.
 *   <li><b>Local (URL omitted):</b> uses the platform MBean server in the current JVM. Test mode
 *       and an escape hatch for ops running the CLI inside the target JVM (rare). {@link #close} is
 *       a no-op.
 * </ul>
 *
 * <p>{@link AutoCloseable} so callers can drive it via try-with-resources.
 */
public final class JmxClient implements AutoCloseable {

  private final JMXConnector connector;
  private final CachedFsBootstrapMXBean proxy;

  private JmxClient(JMXConnector connector, CachedFsBootstrapMXBean proxy) {
    this.connector = connector;
    this.proxy = proxy;
  }

  /** Returns the typed MBean proxy. Valid until {@link #close}. */
  public CachedFsBootstrapMXBean proxy() {
    return proxy;
  }

  @Override
  public void close() throws IOException {
    if (connector != null) {
      connector.close();
    }
  }

  /**
   * Connects to {@code serviceUrl} (e.g. {@code service:jmx:rmi:///jndi/rmi://host:9999/jmxrmi})
   * and returns a client bound to the MBean at {@code objectName}. Callers MUST close the returned
   * client when done.
   *
   * @throws IOException if the connection cannot be established
   * @throws IllegalArgumentException if {@code objectName} is malformed
   */
  public static JmxClient connect(String serviceUrl, String objectName) throws IOException {
    JMXServiceURL url = new JMXServiceURL(serviceUrl);
    JMXConnector connector = JMXConnectorFactory.connect(url);
    MBeanServerConnection conn = connector.getMBeanServerConnection();
    ObjectName name = parseObjectName(objectName);
    CachedFsBootstrapMXBean proxy = JMX.newMBeanProxy(conn, name, CachedFsBootstrapMXBean.class);
    return new JmxClient(connector, proxy);
  }

  /**
   * Returns a client bound to the platform MBean server in the current JVM. Used by tests and the
   * rare in-JVM CLI invocation. Caller still must close (no-op).
   */
  public static JmxClient local(String objectName) {
    ObjectName name = parseObjectName(objectName);
    CachedFsBootstrapMXBean proxy =
        JMX.newMBeanProxy(
            ManagementFactory.getPlatformMBeanServer(), name, CachedFsBootstrapMXBean.class);
    return new JmxClient(null, proxy);
  }

  private static ObjectName parseObjectName(String objectName) {
    try {
      return new ObjectName(objectName);
    } catch (MalformedObjectNameException ex) {
      throw new IllegalArgumentException("invalid JMX ObjectName: " + objectName, ex);
    }
  }
}
