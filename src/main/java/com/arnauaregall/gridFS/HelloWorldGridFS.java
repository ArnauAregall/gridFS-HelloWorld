package com.arnauaregall.gridFS;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSFile;
import com.mongodb.gridfs.GridFSInputFile;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Hello World application using GridFS with the MongoDB Java Driver.
 * This program was written by following 10gen's MongoDB for Java Developers course (M101J).
 *
 * @author 10gen and ArnauAregall
 */
public class HelloWorldGridFS {

    public static void main(String [] args) {
        MongoClient client = null;
        FileInputStream inputStream = null;
        FileOutputStream outputStream = null;

        try {
            client = new MongoClient();
            DB db = client.getDB("gridfs_test");

            GridFS songs = new GridFS(db, "songs");
            songs.remove(new BasicDBObject());

            // let's read an MP3 file through our input stream
            inputStream = new FileInputStream("mySong.mp3");

            GridFSInputFile songFile = songs.createFile(inputStream, "mySong.mp3");

            // let's associate a basic metadata document to our file:
            BasicDBObject metadata = new BasicDBObject(
                    "file_description", "My favourite song")
                    .append("file_owner", System.getProperty("user.name"))
                    .append("file_tags", Arrays.asList("art", "music", "song"));

            songFile.setMetaData(metadata);
            songFile.save();

            System.out.println("Object ID in our \"songs\" collection : " +
                    songFile.get("_id"));

            System.out.println("Saved file to MongoDB :)");

            System.out.println("Let's check it : \n" + songs.find(new BasicDBObject()));

            System.out.println("Let's read and make a copy of it :");

            GridFSDBFile gridFSFile = songs.findOne(new BasicDBObject(
                    "filename", "mySong.mp3"));
            outputStream = new FileOutputStream("mySong_from_MongoDB.mp3");
            gridFSFile.writeTo(outputStream);

            System.out.println("Written file out from MongoDB :)");

        } catch(UnknownHostException e) {
            System.err.println("Ooops, can't connect to MongoDB server : \n" + e);
        } catch(IOException e)  {
            System.err.println("Ooops, something went wrong when reading/writing file : \n" + e);
        } finally {
            try {
                if (outputStream != null)
                    outputStream.close();
                if (inputStream != null)
                    inputStream.close();
                if (client != null)
                    client.close();
            } catch (Exception e) {
                System.err.println("Something went really bad : \n" + e);
            }
        }
    }
}
