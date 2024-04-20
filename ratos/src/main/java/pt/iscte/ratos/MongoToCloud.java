package pt.iscte.ratos;

import org.bson.types.ObjectId;
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
    static DBCollection mongocollection3;
    static String mongo_user = "";
    static String mongo_password = "";
    static String mongo_address = "";
    static String cloud_server = "";
    static String mongo_host = "";
    static String mongo_replica = "";
    static String mongo_database = "";
    static String mongo_collection1 = "";
    static String mongo_collection2 = "";
    static String mongo_collection3 = "";
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
            mongo_collection3 = p.getProperty("mongo_collection3");
            
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
            mongocollection3 = db.getCollection(mongo_collection3);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("resource")
    public static void connectCloud() {
        try {
            MqttClient mqttClient = new MqttClient(cloud_server, "MongoToCloud");
            mqttClient.connect();
            
            // Obtém o último ID adicionado para cada coleção
            String lastTempId = getLastId("sensores_temp");
            String lastMovId = getLastId("sensores_mov");
            
            while (true) {
            	// Consulta MongoDB para obter os dados do tópico temperatura (mongocollection1)
                BasicDBObject queryTemp = new BasicDBObject("_id", new BasicDBObject("$gt", new ObjectId(lastTempId)));
                DBCursor cursor1 = mongocollection1.find(queryTemp);
                while (cursor1.hasNext()) {
                    DBObject document = cursor1.next();
                    
                    // Verifica se o sensor tem como valor 1 ou 2
                    Integer sensorValue = (Integer) document.get("Sensor");
                    // Verifica se o campo "Hora" contém a data e hora atual
                    String hora = (String) document.get("Hora");
                    
                    try {
                        DateTimeFormatter formatterMili = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");                       
                        LocalDateTime horaDocumentoMili = LocalDateTime.parse(hora, formatterMili);                       
                            
                        if (sensorValue != null && (sensorValue == 1 || sensorValue == 2)) {
                                // Convertendo o documento para JSON e publicando na nuvem
                        	MqttMessage message = new MqttMessage(JSON.serialize(document).getBytes());
                            message.setQos(1);
                            mqttClient.publish(cloud_topic1, message);
                            // Escreve o último ID passado para a cloud na mongocollection3
                            writeLastId("sensores_temp", document.get("_id").toString());
                        } else {
                            System.out.println("Recebi um dado de temperatura incorreto! Sensor: " + sensorValue);
                        }

                    } catch (DateTimeParseException e) {
                        System.out.println("Formato de hora e data incorretos! Hora: " + hora);
                    }
                }

                // Consulta MongoDB para obter os dados do tópico passagens (mongocollection2)
                BasicDBObject queryMov = new BasicDBObject("_id", new BasicDBObject("$gt", new ObjectId(lastMovId)));
                DBCursor cursor2 = mongocollection2.find(queryMov);
                while (cursor2.hasNext()) {
                    DBObject document = cursor2.next();
                    
                    // Verifica se a sala destino tem valor maior que a sala de origem
                    Integer origem = (Integer) document.get("SalaOrigem");
                    Integer destino = (Integer) document.get("SalaDestino");
                    
                    String hora2 = (String) document.get("Hora");                   
                    
                    try {
                        DateTimeFormatter formatterMili = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
                        LocalDateTime horaDocumentoMili2 = LocalDateTime.parse(hora2, formatterMili);
                      
	                    if (origem != null && destino != null && destino > origem && destino - origem == 1) {
	                            MqttMessage message = new MqttMessage(JSON.serialize(document).getBytes());
	                            message.setQos(2);
	                            mqttClient.publish(cloud_topic1, message);
	                            // Escreve o último ID passado para a cloud na mongocollection3
	                            writeLastId("sensores_mov", document.get("_id").toString());
	                    } else {
	                        System.out.println("Recebi um dado de passagens incorreto! Origem: " + origem + ", Destino: " + destino);
	                    }

                    } catch (DateTimeParseException e) {
                        System.out.println("Formato de hora e data incorretos! Hora: " + hora2);
                    }
                }
                // Aguarda um intervalo antes de verificar novamente o banco de dados
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Método para escrever o último ID passado para a cloud na mongocollection3
    public static void writeLastId(String collectionName, String lastId) {
        try {
            // Consulta se já existe um documento para esta coleção
            DBObject query = new BasicDBObject("collectionName", collectionName);
            DBCursor cursor = mongocollection3.find(query);

            if (cursor.hasNext()) {
                // Se já existe, atualiza o último ID
                DBObject updateObject = new BasicDBObject("$set", new BasicDBObject("lastId", lastId));
                mongocollection3.update(query, updateObject);
            } else {
                // Se não existe, insere um novo documento
                DBObject document = new BasicDBObject("collectionName", collectionName)
                    .append("lastId", lastId);
                mongocollection3.insert(document);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    // Método para obter o último ID passado para a cloud na mongocollection3
    public static String getLastId(String collectionName) {
        try {
            // Consulta o último ID para esta coleção
            DBObject query = new BasicDBObject("collectionName", collectionName);
            DBObject result = mongocollection3.findOne(query);
            if (result != null) {
                return result.get("lastId").toString();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}

