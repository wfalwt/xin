package op.mit.weifangan.xin.ut;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class MCP {

    /**
     * singleton object of DataSource
     */
    private static BasicDataSource DataSource = null;

    /**
     * Singleton instance of getting an Oject of BasicDatasource
     * @param jdbc jdbc url of MySQL connection information
     * @param user MySQL username
     * @param pwd  password of MySQL connect
     * @return BasicDataSource
     */
    public static BasicDataSource DataSource(String jdbc, String user, String pwd){
        if(DataSource == null){
            DataSource = getDataSource(jdbc,user,pwd);
        }
        return DataSource;
    }


    /**
     * Instantiation of BasicDataSource to use connection pool
     * @param jdbc jdbc url of MySQL connection information
     * @param user MySQL username
     * @param pwd  password of MySQL connect
     * @return BasicDataSource
     */
    private static BasicDataSource getDataSource(String jdbc,String user,String pwd){

        BasicDataSource dataSource = new BasicDataSource();
        // authorize information
        dataSource.setUsername(user);
        dataSource.setPassword(pwd);
        dataSource.setUrl(jdbc);
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");

        // connection pool settings
        dataSource.setInitialSize(10);
        dataSource.setMaxActive(10);
        dataSource.setMaxIdle(10);
        dataSource.setMinIdle(3);
        dataSource.setMaxWait(GenericObjectPool.DEFAULT_MAX_WAIT);
        dataSource.setTestOnBorrow(GenericObjectPool.DEFAULT_TEST_ON_BORROW);
        dataSource.setTestOnReturn(GenericObjectPool.DEFAULT_TEST_ON_RETURN);
        dataSource.setTestWhileIdle(GenericObjectPool.DEFAULT_TEST_WHILE_IDLE);
        return dataSource;
    }

    /**
     * Convert MySQL query Resultset to map struct
     *  ps: The empty values is ignored
     * @param rs ResultSet the result of query to mysql
     * @return Map<String,String>  mapping of result to query
     * @throws SQLException
     */
    public static Map<String, String> getResultMap(ResultSet rs)
            throws SQLException {
        Map<String, String> hm = new HashMap<String, String>();
        ResultSetMetaData rsmd = rs.getMetaData();
        int count = rsmd.getColumnCount();
        for (int i = 1; i <= count; i++) {
            String key = rsmd.getColumnLabel(i);
            String value = rs.getString(i);
            if(value != null && !value.equals("")) {
                hm.put(key, value);
            }
        }
        return hm;
    }

}
