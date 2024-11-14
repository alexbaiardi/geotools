package org.geotools.hdfs;

import java.awt.image.BufferedImage;
import java.io.InputStream;
import java.net.URI;
import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestHdfs {

    public static void main(String[] args) {
        String hdfsFilePath = "hdfs://namenode_host:port/path/to/your/image.png";
        Configuration configuration = new Configuration();

        try {
            // Set up the HDFS filesystem instance
            FileSystem fileSystem = FileSystem.get(new URI(hdfsFilePath), configuration);

            // Open the file as an InputStream
            try (InputStream inputStream = fileSystem.open(new Path(hdfsFilePath));
                    ImageInputStream imageInputStream =
                            ImageIO.createImageInputStream(inputStream)) {

                // Read the image
                BufferedImage image = ImageIO.read(imageInputStream);
                if (image != null) {
                    System.out.println("Image read successfully!");
                    // You can now work with the BufferedImage
                } else {
                    System.out.println("Failed to read the image from HDFS.");
                }
            }

            // Close the HDFS FileSystem instance
            fileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
