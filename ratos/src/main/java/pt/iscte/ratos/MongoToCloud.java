package pt.iscte.ratos;

import org.eclipse.paho.client.mqttv3.*;
import com.mongodb.*;
import com.mongodb.util.JSON;
import java.util.Properties;
import java.io.FileInputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@SuppressWarnings("deprecation")
public class MongoToCloud {
    static MongoClient mongoClient;
    static DB db;
    static DBCollection mongocollection1;
    static DBCollection mongocollection2;
    static String mongo_user = "";
    static String mongo_password = "";
    static String mongo_address = "";
    static String cloud_server = "";
    static String mongo_host = "";
    static String mongo_replica = "";
    static String mongo_database = "";
    static String mongo_collection1 = "";
    static String mongo_collection2 = "";
    static String mongo_authentication = "";
    static String cloud_topic1 = ""; // Tópico 1 para a nuvem
    static String cloud_topic2 = ""; // Tópico 2 para a nuvem

    public static void main(String[] args) {
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("mongoToCloud.ini"));
            mongo_address = p.getProperty("mongo_address");
            mongo_user = p.getProperty("mongo_user");
            mongo_password = p.getProperty("mongo_password");
            mongo_replica = p.getProperty("mongo_replica");
            cloud_server = p.getProperty("cloud_server");
            mongo_host = p.getProperty("mongo_host");
            mongo_database = p.getProperty("mongo_database");
            mongo_authentication = p.getProperty("mongo_authentication");
            mongo_collection1 = p.getProperty("mongo_collection1");
            mongo_collection2 = p.getProperty("mongo_collection2");
            cloud_topic1 = p.getProperty("cloud_topic1");
            cloud_topic2 = p.getProperty("cloud_topic2");
        } catch (Exception e) {
            System.out.println("Error reading mongoToCloud.ini file " + e);
        }

        connectMongo();
        connectCloud();
    }

    public static void connectMongo() {
        String mongoURI = "mongodb://";

        if (mongo_authentication.equals("true"))
            mongoURI = mongoURI + mongo_user + ":" + mongo_password + "@";

        mongoURI = mongoURI + mongo_address;

        if (!mongo_replica.equals("false")) {
            if (mongo_authentication.equals("true"))
                mongoURI = mongoURI + "/?replicaSet=" + mongo_replica + "&authSource=admin";
            else
                mongoURI = mongoURI + "/?replicaSet=" + mongo_replica;
        } else {
            if (mongo_authentication.equals("true"))
                mongoURI = mongoURI + "/?authSource=admin";
        }
        try {
            @SuppressWarnings("resource")
            MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoURI));
            db = mongoClient.getDB(mongo_database);
            mongocollection1 = db.getCollection(mongo_collection1);
            mongocollection2 = db.getCollection(mongo_collection2);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("resource")
    public static void connectCloud() {
        try {
            MqttClient mqttClient = new MqttClient(cloud_server, "MongoToCloud");
            mqttClient.connect();
            while (true) {
                // Consulta MongoDB para obter os dados do tópico temperatura (mongocollection1)
                DBCursor cursor1 = mongocollection1.find();
                while (cursor1.hasNext()) {
                    DBObject document = cursor1.next();
                    
                    // Verifica se o sensor tem como valor 1 ou 2
                    Integer sensorValue = (Integer) document.get("Sensor");
                    // Verifica se o campo "Hora" contém a data e hora atual
                    String hora = (String) document.get("Hora");
                    
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    LocalDateTime horaAtual = LocalDateTime.now();
                    String formattedHoraAtual = horaAtual.format(formatter);
                    
                    LocalDateTime horaDocumento = LocalDateTime.parse(hora, formatter);
                    LocalDateTime horaAtualFormatada = LocalDateTime.parse(formattedHoraAtual, formatter);
                    
                    
//                    try {
//                        DateTimeFormatter formatterMili = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
//                        LocalDateTime horaDocumentoMili = LocalDateTime.parse(hora, formatterMili);
//                        
                    if (sensorValue != null && (sensorValue == 1 || sensorValue == 2) && horaDocumento.isEqual(horaAtualFormatada)) {
//                        if (sensorValue != null && (sensorValue == 1 || sensorValue == 2) && horaDocumentoMili.isEqual(horaAtual)) {
//                            // Convertendo o documento para JSON e publicando na nuvem
                            MqttMessage message = new MqttMessage(JSON.serialize(document).getBytes());
                            message.setQos(1);
                            mqttClient.publish(cloud_topic1, message);
                    } else {
                        System.out.println("Recebi um dado de temperatura incorreto! Sensor: " + sensorValue + horaAtualFormatada);
                    }
//                    } catch (DateTimeParseException e) {
//                        System.out.println("Formato de hora inválido! Hora: " + hora);
//                    }
                }

                // Consulta MongoDB para obter os dados do tópico passagens (mongocollection2)
                DBCursor cursor2 = mongocollection2.find();
                while (cursor2.hasNext()) {
                    DBObject document = cursor2.next();
                    
                    // Verifica se a sala destino tem valor maior que a sala de origem
                    Integer origem = (Integer) document.get("SalaOrigem");
                    Integer destino = (Integer) document.get("SalaDestino");
//                    
//                    // Verifica se o campo "Hora" contém a data e hora atual
//                    String hora1 = (String) document.get("Hora");
//                    LocalDateTime horaAtual1 = LocalDateTime.now();
//                    try {
//                        DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
//                        LocalDateTime horaDocumento1 = LocalDateTime.parse(hora1, formatter1);
//                        
                    if (origem != null && destino != null && destino > origem && destino - origem == 1) {
//                        if (origem != null && destino != null && destino > origem && destino - origem == 1 && horaDocumento1.isEqual(horaAtual1)) {
//                            // Convertendo o documento para JSON e publicando na nuvem
                            MqttMessage message = new MqttMessage(JSON.serialize(document).getBytes());
                            message.setQos(2);
                            mqttClient.publish(cloud_topic1, message);
                    } else {
                        System.out.println("Recebi um dado de passagens incorreto! Origem: " + origem + ", Destino: " + destino);
                    }
//                    } catch (DateTimeParseException e) {
//                        System.out.println("Formato de hora inválido! Hora: " + hora1);
//                    }
                }
                // Aguarda um intervalo antes de verificar novamente o banco de dados
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}

