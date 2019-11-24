package hadoop.chp06.v17034460217;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDemo63 {
    public static void main(String[] args) throws Exception {
        createTable();
        //insert("student", "emp17034460217", column1, value1, column2, value2, column3, value3);
        insert();
        //get("emp17034460217", "student");
        get();
        //scan("emp17034460217", "student", "student");
        scan();
    }

    /**
     * 创建表格
     *
     * @throws Exception
     */
    private static void createTable() throws Exception {
        //0 配置连接HBase
//        Configuration conf = HBaseConfiguration.create();
        Configuration conf = new Configuration();
                //选项可以参照 HBase/conf/hbase-site.xml的配置。这里给出的是完全分布式配置
        //这里如果在本地Windows系统运行，需要修改Windows 的 hosts 文件
        //用管理员权限编辑 C:\Windows\System32\drivers\etc\hosts，
        //并加入 IP 对应的host
        //192.168.30.131 node1
        //192.168.30.132 node2
        //192.168.30.133 node3
        //conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
        conf.set("hbase.zookeeper.quorum", "192.168.30.131");
//        conf.set("hbase.rootdir", "hdfs://192.168.30.131:8020/hbase");
//        conf.set("hbase.cluster.distributed", "true");
        //1 创建到 HBase 的连接
        Connection connect = ConnectionFactory.createConnection(conf);
        Admin admin = connect.getAdmin();
//        HBaseAdmin client = new HBaseAdmin(conf);
//        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("students"));
        //2 设置创建表的名称为emp17034460217
        TableName tn = TableName.valueOf("emp17034460217");
        //3 设置该表的列族为"member_id","address","info"
        String[] family = new String[]{"member_id", "address", "info"};
        //4 表的描述用表名初始化
        HTableDescriptor desc = new HTableDescriptor(tn);
        //5 将列族添加到表的描述当中
        for (int i = 0; i < family.length; i++) {
            desc.addFamily(new HColumnDescriptor(family[i]));
        }
//        HColumnDescriptor h1 = new HColumnDescriptor("info");
//        HColumnDescriptor h2 = new HColumnDescriptor("grade");
//        htd.addFamily(h1);
//        htd.addFamily(h2);
//        client.createTable(htd);
//        client.close();
        //6 检测表是否存在，存在则提示，不存在则创建
        if (admin.tableExists(tn)) {
            //已存在则提示
            System.out.println("table Exists!");
            System.exit(0);
        } else {
            //不存在则创建
            admin.createTable(desc);
            System.out.println("create table Success!");
        }
//        if (client.tableExists(tn)) {
    }

    /**
     * 实现此方法，按实验要求2往表里插入数据
     *
     * @throws Exception
     */
    //private static void insert(String rowKey, String tableName,
    //                           String[] column1, String[] value1, String[] column2, String[] value2,String[] column3, String[] value3) throws Exception {
    private static void insert()throws Exception {
       try {
           String[] column1 = {"id"};
           String[] value1 = {"stu001"};
           String[] column2 = {"country", "province"};
           String[] value2 = {"China", "Guangdong"};
           String[] column3 = {"name", "age"};
           String[] value3 = {"Tom", "20"};
            //0 配置连接HBase
            Configuration conf1 = new Configuration();
            conf1.set("hbase.zookeeper.quorum", "192.168.30.131");
//        conf.set("hbase.rootdir", "hdfs://node1:8020/hbase");
//        conf.set("hbase.cluster.distributed", "true");
            //1 创建到 HBase 的连接
            Connection connect = ConnectionFactory.createConnection(conf1);
            //2 设置创建表的名称为emp17034460217（通过传参）
            TableName tn = TableName.valueOf("emp17034460217");
            //3 通过表名初始化创建表
            Table table = connect.getTable(tn);
            //4 通过行键构造一条数据
            Put put = new Put(Bytes.toBytes("student"));
            //首先
            HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
            for (int i = 0; i < columnFamilies.length; i++) {
                String f = columnFamilies[i].getNameAsString();
                if (f.equals("member_id")) {
                    for (int j = 0; j < column1.length; j++) {
                        put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                    }
                }
                if (f.equals("address")) {
                    for (int j = 0; j < column2.length; j++) {
                        put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                    }
                }
                if (f.equals("info")) {
                    for (int j = 0; j < column3.length; j++) {
                        put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column3[j]), Bytes.toBytes(value3[j]));
                    }

                }
            }
            table.put(put);
            System.out.println("add data Success!");
        }catch(Exception e){
            System.out.println(e);
        }
    }

    /**
     * 实现此方法，按实验要求3查询数据，
     * 并打印查询结果到控制台（System,.out.println)
     *
     * @throws Exception
     */
    //private static void get(String tableName, String rowKey) throws Exception {
    private static void get() throws Exception {

     try{
        //0 配置连接HBase
        Configuration conf2 = new Configuration();
        conf2.set("hbase.zookeeper.quorum", "192.168.30.131");
//        conf.set("hbase.rootdir", "hdfs://node1:8020/hbase");
//        conf.set("hbase.cluster.distributed", "true");
        //1 创建到 HBase 的连接
        Connection connect = ConnectionFactory.createConnection(conf2);
        Table table = connect.getTable(TableName.valueOf("emp17034460217"));
        Get get = new Get(Bytes.toBytes("student"));

        Result result = table.get(get);
        for (Cell cell : result.listCells()) {
            System.out.println("------------------------------------");
            System.out.println("rowkey: " + new String(CellUtil.cloneRow(cell)));
            System.out.println("family: " + new String(CellUtil.cloneFamily(cell)));
            System.out.println("column: " + new String(CellUtil.cloneQualifier(cell)));
            System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
            System.out.println("timest: " + cell.getTimestamp());
        }
    }catch(Exception e){
            System.out.println(e);
        }
    }

    /**
     * 实现此方法，按实验要求4扫描表数据，
     * 并打印查询结果到控制台（System,.out.println)
     *
     * @throws Exception
     */
    //private static void scan(String tableName, String start_rowkey,
      //                       String stop_rowkey) throws Exception {
    private static void scan() throws Exception {
        try {
            Configuration conf3 = new Configuration();
            conf3.set("hbase.zookeeper.quorum", "192.168.30.131");
            Connection connect = ConnectionFactory.createConnection(conf3);
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes("student"));
            scan.setStopRow(Bytes.toBytes("student"));
            ResultScanner rs = null;
            Table table = connect.getTable(TableName.valueOf("emp17034460217"));
            try {
                rs = table.getScanner(scan);
                for (Result r : rs) {
                    for (Cell cell : r.listCells()) {
                        System.out.println("------------------------------------");
                        System.out.println("rowkey: " + new String(CellUtil.cloneRow(cell)));
                        System.out.println("family: " + new String(CellUtil.cloneFamily(cell)));
                        System.out.println("column: " + new String(CellUtil.cloneQualifier(cell)));
                        System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
                        System.out.println("timest: " + cell.getTimestamp());
                    }
                }
            } finally {
                rs.close();
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}


