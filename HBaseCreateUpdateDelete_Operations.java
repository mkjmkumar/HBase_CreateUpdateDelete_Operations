import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

public class HBaseCreateUpdateDelete_Operations {
public static void main(String[] args) throws IOException {
Configuration conf = HBaseConfiguration.create();

String tblName = "t2";
String colFamily = "usr_data";

HBaseAdmin admin = new HBaseAdmin(conf);

// Explicitly specify connection
HConnection connection = HConnectionManager.createConnection(conf);

System.out.println("Connection to HConnection...");

if (!admin.tableExists(tblName)) {
HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tblName));
HColumnDescriptor family = new HColumnDescriptor(colFamily);
desc.addFamily(family);
admin.createTable(desc);

// Insert initial values into table
HTableInterface tblIface = connection.getTable(tblName);

Map<String, String> users = new HashMap<String, String>();
users.put("Paul", "paul01@mail.com");
users.put("Mike", "mike@mail.com");
users.put("User3", "unknown@unknown.com");

System.out.println("Users are added = Paul, Mike, User3...");

for (Map.Entry usr : users.entrySet()) {
String usrName = usr.getKey().toString();
String usrMail = usr.getValue().toString();
System.out.println(MessageFormat.format("Add user: {0}, mail: {1}", usrName, usrMail));

String rowKey = usrName + "RK"; //System.currentTimeMillis();

Put put = new Put(Bytes.toBytes(rowKey));
put.add(Bytes.toBytes(colFamily), Bytes.toBytes("user_name"),
Bytes.toBytes(usrName));
put.add(Bytes.toBytes(colFamily), Bytes.toBytes("user_mail"),
Bytes.toBytes(usrMail));

tblIface.put(put);
}
tblIface.close();
}
if (admin.tableExists(tblName)){ 
System.out.println("Table Exists...");
HTableInterface tblIface = connection.getTable(tblName);


// Table Scan
System.out.println("Table Scan...");
byte[] startRow = Bytes.toBytes("P"); //inclusive
byte[] endRow = Bytes.toBytes("S"); //exclusive

//Scan scan = new Scan(); // Full Scan
Scan scan = new Scan(startRow, endRow);
ResultScanner scanner = tblIface.getScanner(scan);

for (Result r : scanner) {
// extract user name
byte[] b = r.getRow();
String rowKey = Bytes.toString(b);
b = r.getValue(Bytes.toBytes(colFamily), Bytes.toBytes("user_name"));
String user = Bytes.toString(b);
// extract user mail
b = r.getValue(Bytes.toBytes(colFamily), Bytes.toBytes("user_mail"));
String mail = Bytes.toString(b);
System.out.println(rowKey + '\t' + user + '\t' + mail);
}

// Get particular field if we know rowkey
System.out.println("Get particular Field...");
Get get = new Get(Bytes.toBytes("MikeRK")); //rowKey
get.addFamily(Bytes.toBytes(colFamily));
//get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("user_mail"));

Result res = tblIface.get(get);
byte[] usrName = res.getValue(Bytes.toBytes(colFamily), Bytes.toBytes("user_name"));
byte[] usrMail = res.getValue(Bytes.toBytes(colFamily), Bytes.toBytes("user_mail"));

System.out.println("rowKey = MikeRK, user_name: " + Bytes.toString(usrName));
System.out.println("rowKey = MikeRK, user_mail: " + Bytes.toString(usrMail));

//Deallocate resources
tblIface.close();
}

if (admin.tableExists(tblName)){ 
System.out.println("For Update Table Exists...");
HTableInterface tblIface = connection.getTable(tblName);

// Update, just use Put as for inserting
System.out.println("Updating record for MikeRK...");
Put upd = new Put(Bytes.toBytes("MikeRK"));
upd.add(Bytes.toBytes(colFamily), Bytes.toBytes("user_name"), Bytes.toBytes("MantriJI_KI"));
tblIface.put(upd);


//Deallocate resources
tblIface.close();
				}

if (admin.tableExists(tblName)){ 
System.out.println("For Deletion Table Exists...");
HTableInterface tblIface = connection.getTable(tblName);
 
Delete delete = new Delete(Bytes.toBytes("MikeRK"));
delete.deleteColumn(Bytes.toBytes(colFamily), Bytes.toBytes("user_name"));
tblIface.delete(delete);
System.out.println("Record Deleted Successfully...");
//Deallocate resources
tblIface.close();
				}
}
}
