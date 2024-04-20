package pt.iscte.ratos;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.mongodb.*;
import com.mongodb.util.JSON;

import java.util.*;
//import java.util.Vector;
//import java.io.File;
import java.io.*;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;

@SuppressWarnings("deprecation")
public class CloudToMongo  implements MqttCallback {
    MqttClient mqttclient;
    static MongoClient mongoClient;
    static DB db;
    static DBCollection mongocol;
	static String mongo_user = new String();
	static String mongo_password = new String();
	static String mongo_address = new String();
	static String cloud_server = new String();
    static String cloud_topic1 = new String();
    static String cloud_topic2 = new String();
    static String mongo_host = new String();
    static String mongo_replica = new String();
	static String mongo_database = new String();
    static String mongo_collection = new String();
	static String mongo_authentication = new String();
	static JTextArea documentLabel = new JTextArea("\n");  
	

	private static void createWindow() {      	
	JFrame frame = new JFrame("Cloud to Mongo");    
	frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);       
	JLabel textLabel = new JLabel("Data from broker: ",SwingConstants.CENTER);       
	textLabel.setPreferredSize(new Dimension(600, 30));   
	JScrollPane scroll = new JScrollPane (documentLabel, JScrollPane.VERTICAL_SCROLLBAR_ALWAYS, JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);	
	scroll.setPreferredSize(new Dimension(600, 200)); 	
	JButton b1 = new JButton("Stop the program");
	frame.getContentPane().add(textLabel, BorderLayout.PAGE_START);
	frame.getContentPane().add(scroll, BorderLayout.CENTER);
	frame.getContentPane().add(b1, BorderLayout.PAGE_END);	
	frame.setLocationRelativeTo(null);
	frame.pack();      
	frame.setVisible(true);    
	b1.addActionListener(new ActionListener() {
		public void actionPerformed(ActionEvent evt) {
			System.exit(0);
		}
	});
}
	
    public static void main(String[] args) {
		createWindow();
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("cloudToMongo.ini"));
			mongo_address = p.getProperty("mongo_address");
            mongo_user = p.getProperty("mongo_user");
            mongo_password = p.getProperty("mongo_password");						
            mongo_replica = p.getProperty("mongo_replica");
            cloud_server = p.getProperty("cloud_server");			
            cloud_topic1 = p.getProperty("cloud_topic1");
            cloud_topic2 = p.getProperty("cloud_topic2");
            mongo_host = p.getProperty("mongo_host");
            mongo_database = p.getProperty("mongo_database");
            mongo_authentication = p.getProperty("mongo_authentication");			
            mongo_collection = p.getProperty("mongo_collection");
        } catch (Exception e) {
            System.out.println("Error reading CloudToMongo.ini file " + e);
            JOptionPane.showMessageDialog(null, "The CloudToMongo.inifile wasn't found.", "CloudToMongo", JOptionPane.ERROR_MESSAGE);
        }
        new CloudToMongo().connecCloud();
        new CloudToMongo().connectMongo();
    }

    public void connecCloud() {
		int i;
        try {
			i = new Random().nextInt(100000);
            mqttclient = new MqttClient(cloud_server, "CloudToMongo_"+String.valueOf(i)+"_"+cloud_topic1);
            mqttclient.connect();
            mqttclient.setCallback(this);
            mqttclient.subscribe(cloud_topic1);					
            mqttclient.subscribe(cloud_topic2);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public void connectMongo() {
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
            mongocol = db.getCollection(mongo_collection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage c) throws Exception {
        try {
            DBObject document_json = (DBObject) JSON.parse(c.toString());
            
            // Verificar qual tópico MQTT está sendo recebido
            if (topic.equals(cloud_topic1)) {
                DBCollection collection = db.getCollection("sensores_temp");
                collection.insert(document_json);
            } else if (topic.equals(cloud_topic2)) {
                DBCollection collection = db.getCollection("sensores_mov");
                collection.insert(document_json);
            } else {
                System.out.println("Tópico desconhecido: " + topic);
            }

            documentLabel.append(c.toString() + "\n");
        } catch (Exception e) {
            System.out.println(e);
        }
    }


    @Override
    public void connectionLost(Throwable cause) {
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }	
}