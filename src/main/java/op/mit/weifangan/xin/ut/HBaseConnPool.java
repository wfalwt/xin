package op.mit.weifangan.xin.ut;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HBaseConnPool {


    static Connection connection;

    /**
     * Get Table to operate
     * @param tableName
     * @return
     * @throws IOException
     */
    public static Table getHBaseTable(String tableName, String quorum) throws IOException {
        if (connection == null){
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum",quorum);
            connection = ConnectionFactory.createConnection(configuration);
        }
        TableName tbl = TableName.valueOf(tableName);
        return connection.getTable(tbl);
    }

    /**
     * Get a connection to HBase
     * @return Connection
     * @throws IOException
     */
    public static Connection getHConnection(String quorum) throws IOException {
        if (connection == null){
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum",quorum);
            connection = ConnectionFactory.createConnection(configuration);
        }
        return connection;
    }
}