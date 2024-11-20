/*
 * GeoTools - The Open Source Java GIS Toolkit
 * http://geotools.org
 *
 * (C) 2017, Open Source Geospatial Foundation (OSGeo)
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation;
 * version 2.1 of the License.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 */
package org.geotools.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class HdfsConnector {

    private final Configuration configuration = new Configuration();

    public HdfsConnector() {
        String hdfsConfigPath = System.getenv("HADOOP_CONF_DIR");
        if (hdfsConfigPath == null) {
            throw new IllegalStateException("Hadoop configuration path not set");
        }
        configuration.addResource(new Path(hdfsConfigPath + "/core-site.xml"));
        configuration.addResource(new Path(hdfsConfigPath + "/hdfs-site.xml"));
    }

    public Configuration getHdfsConfiguration() {
        return configuration;
    }
}
