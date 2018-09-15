package op.mit.weifangan.xin;

import org.apache.commons.cli.*;

public class Xin {

    public static void main(String[] args) throws ParseException {

        CommandLineParser parser = new BasicParser();
        Options opt = new Options();
        opt.addOption("t","type",true,"data type customer,order");
        opt.addOption("u","user",true,"source data username");
        opt.addOption("d","db",true,"source data database");
        opt.addOption("h","host",true,"source data host");
        opt.addOption("p","port",false,"soource data port");
        opt.addOption("e","pwd",true,"source data password");
        opt.addOption("q","quorum",true,"hbase zookeeper quorum");

        CommandLine cli = parser.parse(opt,args);
        String dataType = "";
        String host = "";
        String user = "";
        String db = "";
        String pwd = "";
        String port = "3306";
        String quorum = "";

        if (!cli.hasOption('t')){
            System.err.println("data type required");
            System.exit(5);
        }else{
            dataType = cli.getOptionValue('t');
            System.out.println("data type " + dataType);
        }

        if (!cli.hasOption('h')) {
            System.err.println("source data username required ");
            System.exit(5);
        }else{
            host = cli.getOptionValue('h');
        }

        if(!cli.hasOption('u')){
            System.err.println("source data username requred");
            System.exit(5);
        }else{
            user = cli.getOptionValue('u');
        }

        if (!cli.hasOption('d')){
            System.err.println("source data database required");
            System.exit(5);
        }else{
            db = cli.getOptionValue('d');
        }

        if(!cli.hasOption('e')) {
            System.err.println("source data password require");
            System.exit(5);
        }else{
            pwd = cli.getOptionValue('e');
        }

        if(!cli.hasOption('q')){
            System.err.println("zookeeper quorum require");
            System.exit(5);
        }else{
            quorum = cli.getOptionValue('q');
        }
        //attention : JDBC url options
        String jdbc = "jdbc:mysql://" + host + ":" + port + "/" + db + "?characterEncoding=utf8&useSSL=false&zeroDateTimeBehavior=convertToNull";
        if(dataType.equals("customer")){
            System.out.println("start to process customer data");

        }else {
            System.out.println("invalid dataType " + dataType);
        }
    }
}
