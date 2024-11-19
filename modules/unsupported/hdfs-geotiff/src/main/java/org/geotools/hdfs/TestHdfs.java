package org.geotools.hdfs;

import java.awt.image.BufferedImage;
import java.io.InputStream;
import java.net.URI;
import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import java.util.Map;
public class TestHdfs {

    public static void main(String[] args) {
        Path hdfsFilePath = new Path("hdfs://myhacluster/difference.tif");
        
        String hdfsConfigPath = System.getenv("HADOOP_CONF_DIR");
        if (hdfsConfigPath == null){
            hdfsConfigPath = "/opt/hadoop/etc/hadoop";
        }
        Configuration configuration = new Configuration();
        System.out.println(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml");
        System.out.println(hdfsConfigPath + "/core-site.xml");
        configuration.addResource(new Path(hdfsConfigPath + "/core-site.xml"));
        configuration.addResource(new Path(hdfsConfigPath + "/hdfs-site.xml"));
        //configuration.iterator().forEachRemaining(entry ->
        //        System.out.println(entry.getKey() + " = " + entry.getValue()));
        try {
            // Set up the HDFS filesystem instance
            FileSystem fileSystem = hdfsFilePath.getFileSystem(configuration);

            // Open the file as an InputStream
            try (final InputStream inputStream = fileSystem.open(hdfsFilePath)) {
                System.out.println(inputStream.available());
            }

            // Close the HDFS FileSystem instance
            fileSystem.close();

            String test = "hdfs://myhacluster/abds/datalake/harbour/tif_geoserver/20190802/NDVI/20190802_NDVI_TPQ.tif";
            HdfsImageInputStreamImpl hdfsImageInputStream = new HdfsImageInputStreamImpl(test);

            System.out.println(hdfsImageInputStream.getFileName());
            System.out.println(hdfsImageInputStream.getUrl());
            System.out.println(hdfsImageInputStream.read());
            byte[] output = new byte[1000];
            System.out.println(hdfsImageInputStream.read(output, 5,50));
            System.out.println(hdfsImageInputStream.read(output, 58,30));
            for (int i = 0; i <100; i++){
                System.out.print(output[i]+ " ");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
