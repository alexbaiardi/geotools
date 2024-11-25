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

import java.io.*;
import java.net.URL;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.imageio.stream.ImageInputStream;
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

    private static final Logger LOGGER = Logger.getLogger(HdfsGeoTiffFormat.class.getName());

    public HdfsGeoTiffFormat() {
        writeParameters = null;
        mInfo = new HashMap<>();
        mInfo.put("name", "HDFSGeoTiff");
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
                                READ_GRIDGEOMETRY2D,
                                INPUT_TRANSPARENT_COLOR,
                                SUGGESTED_TILE_SIZE,
                                RESCALE_PIXELS,
                                BANDS));

        // writing parameters
        writeParameters =
                new ParameterGroup(
                        new DefaultParameterDescriptorGroup(
                                mInfo,
                                RETAIN_AXES_ORDER,
                                WRITE_NODATA,
                                AbstractGridFormat.GEOTOOLS_WRITE_PARAMS,
                                AbstractGridFormat.PROGRESS_LISTENER));
    }

    @Override
    public GeoTiffReader getReader(Object source, Hints hints) {
        // in practice here source is probably almost always going to be a string.
        try {
            // big old try block since we can't do anything meaningful with an exception anyway
            HdfsImageInputStreamImpl inStream;
            String fileName;
            if (source instanceof File) {
                return new HdfsGeoTiffReader(source, hints);
            } else if (source instanceof String) {
                String sourceString = (String) source;
                inStream = new HdfsImageInputStreamImpl(sourceString);
                fileName = sourceString.substring(sourceString.lastIndexOf("/"));
            } else if (source instanceof URL) {
                inStream = new HdfsImageInputStreamImpl((URL) source);
                String sourceString = ((URL) source).getFile();
                fileName = sourceString.substring(sourceString.lastIndexOf("/"));
            } else {
                throw new IllegalArgumentException(
                        "Can't create HdfsImageInputStream from input of "
                                + "type: "
                                + source.getClass());
            }

            String tempDirectory = "/tmp/geotools/hdfs";
            return new HdfsGeoTiffReader(
                    saveImageStreamOnLocalFile(inStream, tempDirectory, fileName), hints);
        } catch (Exception e) {
            LOGGER.log(
                    Level.FINE,
                    "Exception raised trying to instantiate Hdfs image input "
                            + "stream from source.",
                    e);
            throw new RuntimeException(e);
        }
    }

    private File saveImageStreamOnLocalFile(
            ImageInputStream inputStream, String dirPath, String fileName) {

        File parentDirectory = new File(dirPath);
        if (!parentDirectory.exists()) {
            if (!parentDirectory.mkdirs()) {
                throw new RuntimeException("Error creating temporary fily directory");
            }
        }
        // Create a temporary file
        File file = new File(dirPath + "/" + fileName);

        // Write InputStream content to the file
        try (OutputStream outputStream = new FileOutputStream(file)) {
            byte[] buffer = new byte[8192]; // Buffer size (8KB)
            int bytesRead;

            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return file;
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
