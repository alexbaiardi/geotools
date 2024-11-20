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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import javax.imageio.stream.ImageInputStreamImpl;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** ImageInputStream implementation that fetches and caches data from S3 */
public class HdfsImageInputStreamImpl extends ImageInputStreamImpl {

    private final String fileName;
    private final String url;
    private final InputStream inputStream;

    public HdfsImageInputStreamImpl(URL input) throws IOException {
        this(input.toString());
    }

    public HdfsImageInputStreamImpl(String input) throws IOException {
        this.url = input;

        HdfsConnector hdfsConnector = new HdfsConnector();
        Path filePath = new Path(input);
        FileSystem fileSystem = filePath.getFileSystem(hdfsConnector.getHdfsConfiguration());
        this.fileName = filePath.getName();
        this.inputStream = fileSystem.open(filePath);
    }

    @Override
    public int read() throws IOException {
        return inputStream.read();
    }

    @Override
    public int read(byte[] targetBuffer, int off, int len) throws IOException {
        return inputStream.read(targetBuffer, off, len);
    }

    @Override
    public String readLine() throws IOException {
        throw new IOException("readLine NOT Supported");
    }

    String getUrl() {
        return url;
    }

    public String getFileName() {
        return fileName;
    }
}
