package op.mit.weifangan.xin.scope;

import op.mit.weifangan.xin.ut.HBaseConnPool;
import op.mit.weifangan.xin.ut.MCP;
import org.apache.commons.dbcp.BasicDataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class AbastractScope {
    protected Connection mysqlConnection;
    protected org.apache.hadoop.hbase.client.Connection hbaseConnection;
    protected String quorum;

    /**
     * construct method
     * @param jdbc  jdbc url of MySQL connection info.
     * @param user  MySQL username to connect
     * @param pwd   MySQL password to connect
     * @param quorum  HBase zookeeper quorum address
     * @throws SQLException
     * @throws IOException
     */
    AbastractScope(String jdbc, String user, String pwd, String quorum) throws SQLException, IOException {
        BasicDataSource dataSource = MCP.DataSource(jdbc,user,pwd);
        mysqlConnection = dataSource.getConnection();
        hbaseConnection = HBaseConnPool.getHConnection(quorum);
        this.quorum = quorum;
    }

    /**
     * static method to get query result use MySQL connection
     * @param conn MySQL Connection
     * @param sql  sql to query
     * @return ResultSet query to result
     * @throws SQLException
     */
    public static ResultSet getQueryResult(Connection conn, String sql) throws SQLException {
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        rs.setFetchSize(10000);
        return rs;
    }

    /**
     * Get scope data from MySQL
     * @param sql query sql
     * @return ResultSet
     * @throws SQLException
     */
    protected ResultSet getQueryRS(String sql) throws SQLException {
        Statement statement = mysqlConnection.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        rs.setFetchSize(10000);
        return rs;
    }

    /**
     * specify fetch row to get resultset
     * @param sql  query sql
     * @param fetchsize fetch row in memory buffer
     * @return ResultSet
     * @throws SQLException
     */
    protected ResultSet getQueryRS(String sql,int fetchsize) throws SQLException {
        Statement statement = mysqlConnection.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        rs.setFetchSize(fetchsize);
        return rs;
    }

    /**
     *
     * @throws IOException
     */
    public abstract void createHtable() throws IOException;

    /**
     *
     * @throws SQLException
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract void toHtable() throws SQLException, IOException, InterruptedException;

}
