package com.github.louislef299.avro.sample;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class AvroUtil {
    
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java AvroUtil <schema-file> <output-file>");
            System.out.println("Reads JSON from stdin and writes Avro to output file");
            return;
        }
        
        String schemaFile = args[0];
        String outputFile = args[1];
        
        try {
            // Read schema
            String schemaJson = new String(Files.readAllBytes(Paths.get(schemaFile)));
            Schema schema = new Schema.Parser().parse(schemaJson);
            
            // Create a sample record (you can modify this to read from stdin)
            GenericRecord user = new GenericData.Record(schema);
            user.put("id", 1);
            user.put("name", "John Doe");
            user.put("email", "john@example.com");
            
            // Write to Avro file
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.create(schema, new File(outputFile));
            dataFileWriter.append(user);
            dataFileWriter.close();
            
            System.out.println("Successfully created Avro file: " + outputFile);
            
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}