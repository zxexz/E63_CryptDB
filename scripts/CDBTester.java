/**************************************************************************************************************+
 + Copyright © Eric Marriott                                                                                   |
 | eric.marriott@yandex.ru                                                                                     +
 +                                                                                                             |
 | In order for this to work, you must                                                                         +
 +  - have compiled CryptDB successfully (See the README file in the CryptDB directory)                        |
 |  - have the compiled mysql-proxy running, connected to mysql (See the README file in the CryptDB directory) +
 +  - have the mysql JDBC driver jarfile in your compile-time AND run-time classpaths                          |
 |                                                                                                             +
 + Get the MySQL JDBC driver here:                                                                             |
 |  http://dev.mysql.com/downloads/connector/j/                                                                +
 +*************************************************************************************************************/


//import MySQL driver and neccessary MySQL tools...
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

//import clases needed for reading and parsing CSV files
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

class CDBTester{
  //
  private static final boolean DEBUG = true;
  //methods for ease of coding
  private static void print(String p) {
    System.out.print(p);
  }
  private static void println(String p) {
    System.out.println(p);
  }

  //make sure driver is present
  private static void testDriverPresence() {
    try{
      Class.forName("com.mysql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      println("No JDBC driver found!  Exiting after printing stack trace...\n");
      e.printStackTrace();
      System.exit(0);
    }
  }
  //connect to mysql through cryptdb proxy layer
  private static Connection connectToDb(String ProxyInfo,String user, String pass) {
    //try to connect to the db
    try {
      return DriverManager.getConnection(ProxyInfo,user,pass);
    } catch (SQLException e) {
      e.printStackTrace();
      println("Failed to establish connection to CryptDB");
      System.exit(0);
      return null;
    }
  }
  private static void rawUpdate(Connection con,String query,boolean exitOnException) {
    try {
      Statement s = con.createStatement();
      if(DEBUG) { println(query+"\n"); }
      s.executeUpdate(query);
    } catch(SQLException e) {
      println("Failed to execute statement \""+query+"\"\n Printing stack trace ");
      if(exitOnException) {
        print("and then exiting...\n");
      } else {
        print(" and then continuing code execution...\n");
      }
      e.printStackTrace();
      if(exitOnException) {
        System.exit(0);
      }
    }
  }
  //Create a database...
  private static void createDb(Connection con, String name) {
    //
    String query = "CREATE DATABASE "+name;
    try {
      Statement s = con.createStatement();
      if(DEBUG) { println(query+"\n"); }
      s.executeUpdate(query);
    } catch(SQLException e) {
      println("Failed to create database \""+name+"\" ...exiting after printing stack trace...\n ");
      e.printStackTrace();
      System.exit(0);
    }
  }
  //Select a database...
  private static void selectDb(Connection con, String name) {
    //
    String query = "USE "+name;
    try {

      Statement s = con.createStatement();
      if(DEBUG) { println(query+"\n"); }
      s.executeUpdate(query);
    } catch(SQLException e) {
      println("Failed to select database \""+name+"\" ...exiting after printing stack trace...\n ");
      e.printStackTrace();
      System.exit(0);
    }
  }
  private static void createTable(Connection con,String name,String[] columns,String[] types) {
    //quit if not enough info oto create columns...
    if(columns.length != types.length) {
      println("Arrays columns or types not equal length, exiting...");
      System.exit(0);
    }
    String query = "CREATE TABLE "+name+" (\n";
    for(int i = 0; i < columns.length; i++) {
      if(i == (columns.length - 1)) {
        query = query+columns[i]+" "+types[i]+"\n)";
      } else {
        query = query+columns[i]+" "+types[i]+",\n";
      }
    }
    try {
      Statement s = con.createStatement();
      if(DEBUG) { println(query+"\n"); }
      s.executeUpdate(query);
    } catch(SQLException e) {
      println("Failed to create table \""+name+"\" ...exiting after printing stack trace...\n ");
      e.printStackTrace();
      System.exit(0);
    }
  }
  //parse and store CSV
  private static void parseAndStore(Connection con,String fname,String table,String[] col,String[] markers) {
    //
    BufferedReader br = null;
    String f = "./"+fname;
    String line = "";
    String delim = ",";
    String query = "";
    String queryBase = "INSERT INTO "+table+" (";
    for(int i = 0; i < col.length; i++) {
      if(i == (col.length - 1)) { queryBase = queryBase+col[i]+") VALUES(";}
      else { queryBase = queryBase+col[i]+", "; }
    }
    try {
      br = new BufferedReader(new FileReader(f));
      while((line = br.readLine()) != null) {
        query = queryBase;
        String[] splitline = line.split(delim);
        for(int i = 0; i < splitline.length; i++) {
          if(i == (splitline.length - 1)) { query = query+markers[i]+splitline[i]+markers[i]+"\n)"; }
          else { query = query+markers[i]+splitline[i]+markers[i]+",\n"; }
        }
        rawUpdate(con,query,true);
      }
    } catch(FileNotFoundException e) {
      println("Can't find file "+f+" ...exiting after printing stack trace...\n");
      e.printStackTrace();
    } catch(IOException e) {
      println("An IOException has been thrown...exiting after printing stack trace...\n");
      e.printStackTrace();
    } finally {
      if(br != null) {
        try {
          br.close();
        } catch(IOException e) {
          println("An IOException has been thrown...exiting after printing stack trace...\n");
          e.printStackTrace();
        }
      }
    }
  }
  //main method, to show example
  public static void main(String[] args) {
    //database name to create
    String db = "cdbexample";
    //table name to create
    String tb = "USERDATA";
    //column names
    String[] columns = {"first","last","balance","userid","height","zipcode"};
    //mysql datatypes for columns
    String[] columnTypes = {"VARCHAR(255)","VARCHAR(255)","INT","VARCHAR(255)","INT","INT"};
    //these should be zero "" unless pertaining to a non-char or non-string value, then they should be "'"
    String[] typeMarkers = {"'","'","","'","",""};
    //
    testDriverPresence();
    //
    Connection cdbConnection = connectToDb("jdbc:mysql://127.0.0.1:3307","root","letmein");
    println("Connection to CryptDB successful!");
    //
    println("Creating database "+db+"...");
    createDb(cdbConnection,db);
    print("success\n");
    //
    println("Selecting database"+db+"...");
    selectDb(cdbConnection,db);
    print("success\n");
    //
    println("Creating table with name "+tb+" and columns "+Arrays.toString(columns)+" of types "+Arrays.toString(columnTypes)+"...");
    createTable(cdbConnection,tb,columns,columnTypes);
    print("success\n");
    //
    println("Now putting DATA.csv into database...");
    parseAndStore(cdbConnection,"data/DATA.csv",tb,columns,typeMarkers);
    print("success\n");
	}
}
