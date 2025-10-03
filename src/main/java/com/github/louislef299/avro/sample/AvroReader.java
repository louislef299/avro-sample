package com.github.louislef299.avro.sample;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;

public class AvroReader {
    
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java AvroReader <avro-file>");
            return;
        }
        
        String avroFile = args[0];
        
        try {
            File file = new File(avroFile);
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader);
            
            GenericRecord user = null;
            while (dataFileReader.hasNext()) {
                user = dataFileReader.next(user);
                System.out.println(user);
            }
            dataFileReader.close();
            
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}