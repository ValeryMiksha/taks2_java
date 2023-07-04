import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.util.Clock;
public class Connector implements Runnable{
    private static final Logger LOGGER = LoggerFactory.getLogger(Connector.class);
    private static final String APP_NAME = "debezium";
    private static final String HOSTNAME = "localhost";
    private static final String USER = "postgres";
    private static final String DB_NAME = "work";
    private static final String PASSWORD = "admin";
    private static final String URL = "jdbc:postgresql://localhost/history";
    private final Configuration config;
    private EmbeddedEngine engine;
    private Connection connection;
    private Statement statement;
    public Connector() {
        config = Configuration.empty().withSystemProperties(Function.identity()).edit()
                .with(EmbeddedEngine.CONNECTOR_CLASS,"io.debezium.connector.postgresql.PostgresConnector")
                .with(EmbeddedEngine.ENGINE_NAME,APP_NAME)
                .with(PostgresConnectorConfig.TOPIC_PREFIX,APP_NAME)
                .with(PostgresConnectorConfig.HOSTNAME, HOSTNAME)
                .with(PostgresConnectorConfig.DATABASE_NAME , DB_NAME)
                .with(PostgresConnectorConfig.USER,USER)
                .with(PostgresConnectorConfig.PASSWORD,PASSWORD)
                .with(PostgresConnectorConfig.PLUGIN_NAME, "pgoutput")
                .with(EmbeddedEngine.OFFSET_STORAGE,"org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
                .with("schemas.task2_java",true)
                .build();

                try {
                    DriverManager.registerDriver(new org.postgresql.Driver());
                    connection = DriverManager.getConnection(URL,USER,PASSWORD);
                    statement = connection.createStatement();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
    }

    @Override
    public void run() {
            engine = EmbeddedEngine.create()
                    .using(config)
                    .using(this.getClass().getClassLoader())
                    .using(Clock.SYSTEM)
                    .notifying(this::sendRecord)
                    .build();

            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(engine);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Requesting embedded engine to shut down");
                engine.stop();
            }));
            // the submitted task keeps running, only no more new ones can be added
            executor.shutdown();
            awaitTermination(executor);
            LOGGER.info("Engine terminated");
    }
    private void sendRecord(SourceRecord record) {
        // We are interested only in data events not schema change events
        if (record.topic().equals(APP_NAME)) {
            return;
        }
        System.out.println(record);
        if ( null == record.keySchema() ) {
            LOGGER.error("The keySchema is missing. Something is wrong.");
            return;
        }
        String model_name = record.topic().substring(record.topic().lastIndexOf('.')+1);
        String sql = "Create table if not exists "+ model_name+"_history" + "(hist_id serial primary key,";
        String sql2 ="Insert into " + model_name+"_history (";
        Long upd_time =(Long)((Struct) record.value()).get("ts_ms");
        Struct values = ((Struct) record.value()).getStruct("after");
        for(Field field: record.valueSchema().field("after").schema().fields())
        {
            sql+=field.name() + " varchar(255),";
            sql2+= field.name()+", ";
        }
        sql2+="upd_time) Values (";
        sql += "upd_time varchar(50));";
        List<Field> list = record.valueSchema().field("after").schema().fields();
        for(int i = 0;i< list.size();i++)
        {
            sql2+="'"+values.get(list.get(i).name()).toString()+"', ";
        }
        sql2+="'"+ new Date(upd_time) +"');";
        try {
            statement.execute(sql);
            statement.execute(sql2);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private void awaitTermination(ExecutorService executor) {
        try {
            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOGGER.info("Waiting another 10 seconds for the embedded engine to complete");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    public static void main(String[] args) {
        new Connector().run();
    }
}
