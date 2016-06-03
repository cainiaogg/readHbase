import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import javax.naming.ConfigurationException;
import java.security.PrivilegedExceptionAction;

/**
 * Created by jingwei on 16/6/3.
 */

class OperateTable{
    private static Configuration conf;
    private static String tableName = "ns_hero:sinaad_rtlabel";
    public HTable table;
    static {
        String relatetivelyPath = System.getProperty("user.dir");
        String filePath = relatetivelyPath + "/hbase-site.xml";
        Path path = new Path(filePath);
        conf = new Configuration();
        conf.addResource(path);
        conf = HBaseConfiguration.create(conf);
    }
    public void  getImf() throws Exception{
        Scan scan = new Scan();
        table = new HTable(conf,tableName);
        ResultScanner results = table.getScanner(scan);
        for(Result result:results){
            for(KeyValue rowKV:result.raw()){
                String s = new String(rowKV.getValueArray());
                int cnt = 0;
                for(int i = 0;i<s.length();i++){
                    char  c = s.charAt(i);
                    if(c=='\t') cnt++;
                }
                System.out.println(cnt);
            }
        }
        Thread.sleep(4000);
    }
}
public class readHbase {
    public static void main(String []args){
        Object ret = null;
        try{
            ret= UserGroupInformation.createRemoteUser("hero").doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception{
                    OperateTable table = new OperateTable();
                    try{
                        table.getImf();
                    }catch(Exception e){
                        System.out.println(e+"Jijiji");
                    }
                    return null;
                }
            });
        }catch (Exception e){
            System.out.println(e);
        }
    }
}
