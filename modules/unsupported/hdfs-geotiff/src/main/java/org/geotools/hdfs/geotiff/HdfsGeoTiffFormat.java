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
package org.geotools.hdfs.geotiff;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.geotools.api.parameter.GeneralParameterDescriptor;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.gce.geotiff.GeoTiffFormat;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.geotools.hdfs.HdfsImageInputStreamImpl;
import org.geotools.parameter.DefaultParameterDescriptorGroup;
import org.geotools.parameter.ParameterGroup;
import org.geotools.util.factory.Hints;

/**
 * Just a basic wrapper around GeoTiffFormat in order to support GeoTiff over HDFS. Hopefully this
 * wrapper either won't be permanent, or won't require overriding very many properties/methods
 */
public class HdfsGeoTiffFormat extends GeoTiffFormat {

    private Properties prop;

    private static final Logger LOGGER = Logger.getLogger(HdfsGeoTiffFormat.class.getName());

    public HdfsGeoTiffFormat() {
        writeParameters = null;
        mInfo = new HashMap<>();
        mInfo.put("name", "HdfsGeoTiff");
        mInfo.put(
                "description",
                "Tagged Image File Format with Geographic information hosted on HDFS");
        mInfo.put("vendor", "Big Group");
        mInfo.put("version", "1.0");

        // reading parameters
        readParameters =
                new ParameterGroup(
                        new DefaultParameterDescriptorGroup(
                                mInfo,
                                new GeneralParameterDescriptor[] {
                                    READ_GRIDGEOMETRY2D,
                                    INPUT_TRANSPARENT_COLOR,
                                    SUGGESTED_TILE_SIZE
                                }));

        // writing parameters
        writeParameters =
                new ParameterGroup(
                        new DefaultParameterDescriptorGroup(
                                mInfo,
                                new GeneralParameterDescriptor[] {
                                    RETAIN_AXES_ORDER,
                                    AbstractGridFormat.GEOTOOLS_WRITE_PARAMS,
                                    AbstractGridFormat.PROGRESS_LISTENER
                                }));
    }

    @Override
    public GeoTiffReader getReader(Object source, Hints hints) {
        // in practice here source is probably almost always going to be a string.
        try {
            // big old try block since we can't do anything meaningful with an exception anyway
            HdfsImageInputStreamImpl inStream;
            if (source instanceof File) {
                throw new UnsupportedOperationException(
                        "Can't instantiate Hdfs with a File handle");
            } else if (source instanceof String) {
                inStream = new HdfsImageInputStreamImpl((String) source);
            } else if (source instanceof URL) {
                inStream = new HdfsImageInputStreamImpl((URL) source);
            } else {
                throw new IllegalArgumentException(
                        "Can't create HdfsImageInputStream from input of "
                                + "type: "
                                + source.getClass());
            }

            return new HdfsGeoTiffReader(inStream, hints);
        } catch (Exception e) {
            LOGGER.log(
                    Level.FINE,
                    "Exception raised trying to instantiate Hdfs image input "
                            + "stream from source.",
                    e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean accepts(Object o, Hints hints) {
        if (o == null) {
            return false;
        } else {
            boolean accepts = false;
            if (o instanceof String) {
                String url = (String) o;
                if (url.contains("://")) {
                    accepts = containsHdfsPrefix(url.split("://")[0]);
                }
            } else if (o instanceof URL) {
                String protocol = ((URL) o).getProtocol();
                accepts = containsHdfsPrefix(protocol);
            }
            return accepts;
        }
    }

    @Override
    public boolean accepts(Object source) {
        return this.accepts(source, null);
    }

    private boolean containsHdfsPrefix(String prefix) {
        return "hdfs".equals(prefix);
    }
}
