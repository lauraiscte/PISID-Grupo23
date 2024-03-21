package pt.iscte.ratos;

import org.eclipse.paho.client.mqttv3.*;
import com.mongodb.*;
import com.mongodb.util.JSON;
import java.util.Properties;
import java.io.FileInputStream;

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
    static String cloud_topic = ""; // Tópico único para a nuvem

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
            cloud_topic = p.getProperty("cloud_topic");
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
                // Consulta MongoDB para obter os dados do tópico
                DBCursor cursor1 = mongocollection1.find();
                while (cursor1.hasNext()) {        
                    DBObject document = cursor1.next();           
                    // Convertendo o documento para JSON e publicando na nuvem
                    MqttMessage message = new MqttMessage(JSON.serialize(document).getBytes());
                    mqttClient.publish(cloud_topic, message);
                }
                
                DBCursor cursor2 = mongocollection2.find();
                while (cursor2.hasNext()) {
                    DBObject document = cursor2.next();
                    // Convertendo o documento para JSON e publicando na nuvem
                    MqttMessage message = new MqttMessage(JSON.serialize(document).getBytes());
                    mqttClient.publish(cloud_topic, message);
                }
                // Aguarda um intervalo antes de verificar novamente o banco de dados
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

