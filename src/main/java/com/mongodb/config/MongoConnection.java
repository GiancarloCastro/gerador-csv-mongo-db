package com.mongodb.config;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public enum MongoConnection {
    HLG(        "mongodb://"+System.getenv("MONGODB_HLG_USER")+":"+System.getenv("MONGODB_HLG_PASS")+ "@mdbq-via-1.dc.nova:27017,mdbq-via-2.dc.nova:27017,mdbq-via-3.dc.nova:27017/catalogo?replicaset=rsviaQA&readPreference=secondaryPreferred&authSource=admin&compressors=snappy,zlib&retryWrites=true&maxPoolSize=5&maxIdleTimeMS=1000"),
    STRESS_TEST("mongodb://"+System.getenv("MONGODB_STRESS_USER")+":"+System.getenv("MONGODB_STRESS_PASS")+"@mdbh-viatest-1.dc.nova:27017,mdbh-viatest-2.dc.nova:27017,mdbh-viatest-3.dc.nova:27017/admin?retryWrites=false&replicaSet=rsViaTest&readPreference=primary&maxIdleTimeMS=60000&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-1"),
    PRD(        "mongodb://"+System.getenv("MONGODB_PRD_USER")+":"+System.getenv("MONGODB_PRD_PASS")+"@mdbp-via-1.dc.nova:27017,mdbp-via-2.dc.nova:27017,mdbp-via-3.dc.nova:27017,mdbp-via-4.dc.nova:27017,mdbp-via-5.dc.nova:27017/admin?retryWrites=false&replicaSet=rsMULT&readPreference=secondaryPreferred&readPreferenceTags=dc:eqx&maxIdleTimeMS=300000&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-1"),
    SIT ( "mongodb://"+System.getenv("MONGODB_SIT_USER")+":"+System.getenv("MONGODB_SIT_PASS")+"@mdbd-via-1.dc.nova:27017,mdbd-via-2.dc.nova:27017,mdbd-via-3.dc.nova:27017/admin?retryWrites=false&replicaSet=rsviaSIT&readPreference=secondaryPreferred&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-1");

    private String connectionString;

    private MongoConnection(String connectionString) {
        this.connectionString = connectionString;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public static MongoConnection getMongoConnection(String[] args) {
        MongoConnection selected = null;
        if (args == null || args.length == 0) {
            selected = MongoConnection.HLG;
        } else if (args.length == 1) {
            if (args[0].length() == 1) {
                try {
                    selected = MongoConnection.values()[Integer.valueOf(args[0])];
                } catch (Throwable e) {
                    throw new RuntimeException("Invalid position: " + args[0], e);
                }
            }
        } else {
            try {
                selected = MongoConnection.valueOf(args[0]);
            } catch (Throwable e) {
                throw new RuntimeException("Invalid profile: " + args[0], e);
            }
        }
        System.out.println("Selected Mongo Connection: " + selected);
        return selected;
    }
    
    public static MongoClient createConnection(String[] args) {
        MongoConnection mongo = MongoConnection.getMongoConnection(args);
        return MongoClients.create(mongo.getConnectionString());
    }

};

