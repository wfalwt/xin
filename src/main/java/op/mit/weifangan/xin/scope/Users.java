package op.mit.weifangan.xin.scope;

import op.mit.weifangan.xin.ut.MCP;
import op.mit.weifangan.xin.ut.MultiThreadsExecutor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class Users extends AbastractScope{

    String htblname = "ht_users";
    String mytblname = "my_user";
    public Users(String jdbc, String user, String pwd, String quorum) throws IOException, SQLException {
        super(jdbc,user,pwd,quorum);
    }

    /**
     * create HBase table
     * @throws IOException
     */
    public void createHtable() throws IOException {
        Admin admin = hbaseConnection.getAdmin();
        TableName tableName = TableName.valueOf(htblname);
        if (admin.tableExists(tableName)) {
            System.out.println("create table error : " + htblname + " table exists");
            return;
        }
        HTableDescriptor htd = new HTableDescriptor(tableName);
        /**
         * Family attr
         */
        HColumnDescriptor attr = new HColumnDescriptor("attr");
        attr.setMaxVersions(1);
        attr.setBloomFilterType(BloomType.ROW);
        //attr.setCompressionType(Compression.Algorithm.LZO);

        /**
         * timed tags
         */
        HColumnDescriptor timed_tags = new HColumnDescriptor("timed_info");
        timed_tags.setMaxVersions(Integer.MAX_VALUE);
        //timed_tags.setCompressionType(Compression.Algorithm.LZO);
        timed_tags.setBloomFilterType(BloomType.ROW);

        htd.addFamily(attr);
        htd.addFamily(timed_tags);
        admin.createTable(htd);
    }

    /**
     *
     * @throws SQLException
     * @throws IOException
     * @throws InterruptedException
     */
    public void toHtable() throws SQLException, IOException, InterruptedException {
        MultiThreadsExecutor executor = new MultiThreadsExecutor();
        executor.init();
        ExecutorService pool = executor.getCustomThreadPoolExecutor();
        TableName tbl = TableName.valueOf(htblname);
        final Table table =hbaseConnection.getTable(tbl);
        String fetchCustSQL = "SELECT * FROM " + mytblname;
        ResultSet custs = getQueryRS(fetchCustSQL,5000);
        while (custs.next()){
            final Map<String,String> record = MCP.getResultMap(custs);
            pool.execute(new Runnable() {
                public void run() {
                    try {
                        String uid = record.get("uid");
                        Put put = new Put(Bytes.toBytes(uid));
                        if (record.get("cellphone") != null) {
                            put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("cellphone"), Bytes.toBytes(record.get("mobile")));
                        }
                        if (record.get("id_no") != null ) {
                            put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("id_no"), Bytes.toBytes(record.get("id_no")));
                        }
                        if ( record.get("email") != null ) {
                            put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("email"), Bytes.toBytes(record.get("email")));
                        }
                        if ( record.get("login_name") != null ) {
                            put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("login_name"), Bytes.toBytes(record.get("login_name")));
                        }
                        if ( record.get("pwd") != null ) {
                            put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("pwd"), Bytes.toBytes(record.get("pwd")));
                        }
                        if ( record.get("nickname") != null ) {
                            put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("nickname"), Bytes.toBytes(record.get("nickname")));
                        }
                        if ( record.get("avatar") != null ) {
                            put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("avatar"), Bytes.toBytes(record.get("avatar")));
                        }

                        String fetchTimedInfo = "SELECT * FROM timed_info WHERE uid = " + uid;
                        ResultSet trs = getQueryRS(fetchTimedInfo);
                        while (trs.next()){
                            if (trs.getTimestamp("ts").getTime() > 0 ) {
                                put.addColumn(Bytes.toBytes("timed_info"),
                                        Bytes.toBytes(trs.getString("tid")),
                                        trs.getTimestamp("ts").getTime(),
                                        Bytes.toBytes(trs.getString("tv"))
                                );
                            }
                        }
                        table.put(put);
                        System.out.println("put " + uid + " to HBase");
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        pool.awaitTermination(1000, TimeUnit.SECONDS);
    }
}
