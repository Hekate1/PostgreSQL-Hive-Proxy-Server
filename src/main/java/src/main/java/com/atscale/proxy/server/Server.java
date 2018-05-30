package src.main.java.com.atscale.proxy.server;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import org.json.*;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.DocumentBuilder;

import org.apache.log4j.BasicConfigurator;

public class Server {	
    public static void main(String[] args){
    	try {
        	System.out.println("Here");
        	BasicConfigurator.configure();
        	Socket socket = null;
            ServerSocket serverSocket = new ServerSocket(5433);
            
            while (true) {
                try {
                    socket = serverSocket.accept();
                } catch (IOException e) {
                    System.out.println("I/O error: " + e);
                }
                // new thread for a client
                new EchoThread(socket).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

final class EchoThread extends Thread{
	
	private static byte[] PARSE_COMP = {49,0,0,0,4};
	private static byte[] BIND_COMP = {50,0,0,0,4};
	private static byte[] READY = {90,0,0,0,5,73};
	
    protected Socket socket;
    
    private int rowCounter = 0;
    private int endIndex = 0;

    public EchoThread(Socket clientSocket) {
        this.socket = clientSocket;
    }

    public void run(){
    	try {
	        System.out.println("New Thread");
	        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
	        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
	        
	        ArrayList<String> output = new ArrayList<String>();
	        
			File inputFile = new File("rep2.txt");
	        Scanner sc = new Scanner(inputFile);
	        
	        byte[] rep2 = new byte[315];
	        
	        for(int i = 0; i < 315; i++)
	        {
	        	rep2[i] = (byte)Integer.parseInt(sc.nextLine());
	        }
	        
	        Class.forName("org.apache.hive.jdbc.HiveDriver");
	        Class.forName("org.postgresql.Driver");
	        
	        Properties props = new Properties();
	        props.setProperty("user","admin");
	        props.setProperty("password","admin");
	        
	        Connection con = null;
	        Statement statement = null;
	        
	        ResultSet rs = null;
			String db = null;
	        
	    	while(socket.isClosed() == false)
	    	{
	    		char[] buffer = new char[5000];
	    		int bytesRead = in.read(buffer);
	    		if(bytesRead > 0)
	    		{
	    			System.out.println("Bytes Read");
	                
	                String query = "";
					for(char c : buffer)
					{
						if(c > 31) //32 and above are normal characters
						{
							query = query.concat(Character.toString(c));
						}
					}
	    			if(buffer[8] == 'u' && buffer[9] == 's' && buffer[10] == 'e' && buffer[11] == 'r')
	    			{	
	    	            out.write(rep2);
	    	            System.out.println("Repsponded");
	    	            if(db == null)
	    	            {
		    	            db = query.substring(query.indexOf("database") + 8, query.indexOf("client"));
		    	            db.substring(query.indexOf('.') + 1);
		    	            con = DriverManager.getConnection("jdbc:hive2://local.infra.atscale.com:11112/" + db, props);
		    	            statement = con.createStatement();
	    	            }
	    			}
	    			else if(query.contains("SET extra_float_digits"))
	    			{
	    				byte[] efdResponse = new byte[]{49,0,0,0,4,50,0,0,0,4,67,0,0,0,8,83,69,84,0,90,0,0,0,5,73};
	    				
	    				out.write(efdResponse);
	    			}
	    			else if (buffer[0] == 88)//Termination Command
	    			{
	    				System.out.println("Terminating");
	    				socket.close();
	    			}
	    			else if(query.contains("SELECT NULL AS TABLE_CAT")) //Meta data query
	    			{
	    				Connection tempCon = DriverManager.getConnection("jdbc:postgresql://localhost");
	    		        Statement tempStatement = tempCon.createStatement();
	    		        System.out.println("Postgresql server");
	    		        rs = execute(tempStatement, buffer, false, db);
	    		        if(rs == null)
	    				{
	    					out.write(READY);
	    				}
	    				else
	    				{
	    					processing(rs, query, buffer, out, db);
	    				}
	    			}
	    			else if(query.contains("SELECT * FROM (SELECT n.nspname")) //Different meta data query
	    			{
	    				Connection tempCon = DriverManager.getConnection("jdbc:postgresql://localhost");
	    		        Statement tempStatement = tempCon.createStatement();
	    		        rs = execute(tempStatement, buffer, true, db);
	    		        
	    		        //GET PROJECT NAME
  		        	  	URL url = new URL("http://local.infra.atscale.com:10502/projects/orgId/default/envId/test?includeCubes=true");
  		        	  	HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
  		        	  	httpCon.setRequestMethod("GET");
  		        	 
  		        	  	//Request Headers
  		        	  	httpCon.setRequestProperty("Authorization", "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcGlzdXBlcnVzZXIiOnRydWUsImF1dGhlbnRpY2F0ZWQiOnRydWUsImRlZmF1bHQiOnsicXVlcnlfZGF0YXNl"
  		        	  			+ "dF9hcGlfYWxsb3dlZCI6dHJ1ZSwiZGVzaWduX2NlbnRlcl9hcGlfYWxsb3dlZCI6dHJ1ZX0sImV4cCI6MTUyNzYzMzA0OCwiaW1wZXJzb25hdGlvbnMiOnsib3JnX2ltcGVyc29uYXRpb24iOm51bGx9LCJwZXJtaXNzaW"
  		        	  			+ "9ucyI6eyJvcmdfcGVybWlzc2lvbnMiOnsiZGVmYXVsdCI6eyJhZ2dyZWdhdGVzLm1hbmFnZSI6dHJ1ZSwiYWdncmVnYXRlcy52aWV3Ijp0cnVlLCJkYXRhd2FyZWhvdXNlcy5hZG1pbiI6dHJ1ZSwiZGVzaWduY2VudGVy"
  		        	  			+ "LnVzZXIiOnRydWUsImVudmlyb25tZW50LmFkbWluIjp0cnVlLCJvYmplY3QuY3JlYXRlIjp0cnVlLCJvcmdhbml6YXRpb24uYWRtaW4iOnRydWUsInF1ZXJpZXMubWFuYWdlIjp0cnVlLCJxdWVyaWVzLnZpZXciOnRydWU"
  		        	  			+ "sInJ1bnRpbWUudXNlci5zZXR0aW5ncyI6dHJ1ZSwidmlldy5zdXBwb3J0LmxvZ3MiOnRydWV9fX0sInN1YiI6ImFkbWluIiwic3VwZXJ1c2VyIjp0cnVlfQ.rjvckaQ47wLX-h2B7AHDfe9lKQPGpkJHmUDx5YsNlHdMhnj"
  		        	  			+ "pGHKWisN5jZSWD-Bc06IpmsUosK5lH3zDJBhyFHRIPp_y0XlKLruwCugmslhjaCPm9fStYs719K33FakARrQL0P0G0hmYnJYsKykd5RTj5qo27vNPerJbov33760Lwqwk_QC5SAsTj5hB5efNLB0gfR0qrDMWaXkbXKcDnC"
  		        	  			+ "avuFDLHLTJUxjhq6EgMU8VHc6PPTB2PJsfeAKOC2-Yw62Qp8OYqW1v85kYqWeDaA8Euf2_8X0N9tDN1X0joz37sBcrQzMrylk2QW7PDvnZIcT0rLvmP90zwWjpbWugxw");
  		        	 
  		        	  	int responseCode = httpCon.getResponseCode();
  		        	  	System.out.println("Sending get request : "+ url);
  		        	  	System.out.println("Response code : "+ responseCode);
  		        	 
  		        	  	// Reading response from input Stream
  		        	  	BufferedReader httpIn = new BufferedReader(
  		        	          new InputStreamReader(httpCon.getInputStream()));
  		        	  	String httpOutput;
  		        	  	StringBuffer response = new StringBuffer();
  		        	  	while ((httpOutput = httpIn.readLine()) != null) {
  		        	  		response.append(httpOutput);
  		        	  	}
  		        	  	httpIn.close();
  		        	  	//printing result from response
  		        	  	JSONObject obj = new JSONObject(response.toString());
  		        	  	
  		        		obj = obj.getJSONObject("response");
  		        		JSONArray projects = obj.getJSONArray("projects");
  		        		
  		        		String projectName = null;
  		        		for(int i = 0; i < projects.length(); i++)
  		        		{
  		        			obj = (JSONObject) projects.get(i);
  	  		        		JSONArray results = obj.getJSONArray("cubes");
  	  		        		for (int j = 0; j < results.length(); j++) {
  	  		  		        	if(results.getJSONObject(j).getString("name").compareTo(db) == 0)
  	  		  		        	{
  	  		  		        		projectName = obj.getString("name");
  	  		  		        	}
  	  	  		        	}
  		        		}
  		        		
  		        		if(projectName == null)
  		        		{
  		        			throw new IOException("Cube Not Found");
  		        		}
  		        		
  		        	  	//END GET PROJECT NAME
  		        		//GET XML
  		        		projectName = projectName.replace(" ", "%20");
  		        		url = new URL("http://localhost:10502/tds/default/"+ db +"/" + projectName + "/"+ db + ".tds?browser=true");
  		        		httpCon = (HttpURLConnection) url.openConnection();
  		        	  	httpCon.setRequestMethod("GET");
  		        	  	
  		        	  	httpCon.setRequestProperty("Accept", "text/xml");
  		        	  	
  		        	  	responseCode = httpCon.getResponseCode();
		        	  	System.out.println("Sending get request : "+ url);
		        	  	System.out.println("Response code : "+ responseCode);
		        	 
		        	  	// Reading response from input Stream
		        	  	httpIn = new BufferedReader(
		        	          new InputStreamReader(httpCon.getInputStream()));
		        	  	response = new StringBuffer();
		        	  	while ((httpOutput = httpIn.readLine()) != null) {
		        	  		response.append(httpOutput);
		        	  	}
		        	  	httpIn.close();
		        	  	
		        	  	output.add(response.toString());
		    	    	Path file = Paths.get("log.txt");
		    	    	
		    	    	Files.write(file, output);
		    	        output.clear();
		        	  	
		    	        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		    	        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		    	        Document doc = dBuilder.parse(new File("log.txt"));
		    	        doc.getDocumentElement().normalize();
		    	        NodeList columns = doc.getDocumentElement().getElementsByTagName("column");

		    	        String[] columnNames = new String[columns.getLength()];
		    			String[] columnTypes = new String[columns.getLength()];
		    			for(int i = 0; i < columns.getLength(); i++)
		    			{
		    				columnNames[i] = columns.item(i).getAttributes().getNamedItem("name").getNodeValue();
		    				System.out.println(columnNames[i]);
		    				columnTypes[i] = columns.item(i).getAttributes().getNamedItem("datatype").getNodeValue();
		    			}
		    	        
  		        		//END GET XML
  		        	  	
	    		        System.out.println("Executed on Postgresql server");
	    		        if(rs == null)
	    				{
	    					out.write(READY);
	    				}
	    				else
	    				{
	    					while (rs.next()) {
	    						for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
	    							if (i > 1) System.out.print(",  ");
	    					        String columnValue = rs.getString(i);
	    					        System.out.print(columnValue + " : " + rs.getMetaData().getColumnName(i));
	    						}
	    					    System.out.println("[END OF ROW]");
	    					}
	    					
	    					ArrayList<ArrayList<String>> rows = new ArrayList<ArrayList<String>>();
	    					for(int i = 0; i < columnNames.length; i++)
	    					{
	    						ArrayList<String> columnVals = new ArrayList<String>();
	    						columnVals.add("public");
	    						columnVals.add(db);
	    						columnVals.add(columnNames[i].substring(1, columnNames[i].length() - 1));
	    						
	    						String type = columnTypes[i];
	    						if(/*type == "varchar" || */type.equals("string"))
	    						{
	    							columnVals.add("1043");
	    							columnVals.add("f");
	    							columnVals.add("259");
	    							columnVals.add("-1");
	    						}
	    						if(/*type == "int4" || */type.equals("integer") || type.equals("real"))
    							{
    								columnVals.add("23");
	    							columnVals.add("f");
	    							columnVals.add("-1");
	    							columnVals.add("4");
    							}
    							/*else if(type == "oid")
    							{
    								columnVals.add("26");
	    							columnVals.add("f");
	    							columnVals.add("-1");
	    							columnVals.add("4");
    							}
    							else if(type == "text")
    							{
    								columnVals.add("25");
	    							columnVals.add("f");
	    							columnVals.add("-1");
	    							columnVals.add("255");
    							}
    							else if(type == "name")
    							{
    								columnVals.add("19");
	    							columnVals.add("f");
	    							columnVals.add("-1");
	    							columnVals.add("255");
    							}
    							else if(type == "int2")
    							{
    								columnVals.add("21");
	    							columnVals.add("f");
	    							columnVals.add("-1");
	    							columnVals.add("2");
    							}
    							else if(type == "int8")
    							{
    								columnVals.add("20");
	    							columnVals.add("f");
	    							columnVals.add("-1");
	    							columnVals.add("8");
    							}
    							else if(type == "char")
    							{
    								columnVals.add("18");
	    							columnVals.add("f");
	    							columnVals.add("-1");
	    							columnVals.add("1");
    							}
    							else if(type == "bool")
    							{
    								columnVals.add("16");
	    							columnVals.add("f");
	    							columnVals.add("-1");
	    							columnVals.add("1");
    							}*/
	    						Integer count = i+1;
	    						columnVals.add(count.toString());
	    						columnVals.add("null");
	    						columnVals.add("null");
	    						columnVals.add("0");
	    						columnVals.add("b");
	    						
	    						rows.add(columnVals);
	    					}
	    					
	    					short columnCount = (short) rs.getMetaData().getColumnCount();
	    					//ROW DESC CREATION
	    					byte[] rowDesc = createRowDesc(rs, columnCount);
	    					
	    					//DATA ROW CREATION
	    					byte[] dataRow = createDataRowsAlternate(rs, (short) 12, rows);

	    					//COMMAND COMPLETION CREATION
	    					byte[] comComp = createCommandCompletion(query);
	    					
	    					byte[] combinded = merge(new byte[][] {PARSE_COMP, BIND_COMP, rowDesc, dataRow, comComp, READY});
	    					out.write(combinded);
	    				}
	    			}
	    			else
	    			{
	    				rs = execute(statement, buffer, false, db);
	    				System.out.println("Connection Success");
	    				if(rs == null)
	    				{
	    					out.write(READY);
	    				}
	    				else
	    				{
	    					processing(rs, query, buffer, out, null);
	    				}
	    			}
	    		}
	    	}
	        
	        sc.close();
	        System.out.println("Finished");
    	} catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
    }
    
    private ResultSet execute(Statement statement, char[] buffer, boolean replaceName, String db) throws SQLException
    {
    	int startIndex = -1;
		endIndex = 0;
		boolean set = false;
		for(int i = 0; i < buffer.length - 1; i++)
		{
			if(buffer[i] == 'S' && buffer[i+1] == 'E' && set == false) //For Select
			{
				set = true;
				startIndex = i;
			}
		}
		set = false;
		if(startIndex == -1)
		{
			return null;
		}
		else
		{
			for(int i = startIndex; i < buffer.length; i++)
			{
				if(buffer[i] == (byte)0 && set == false)
				{
					set = true;
					endIndex = i;
				}
			}
			String query = "";
			for(int i = startIndex; i < endIndex; i++)
			{
				query = query.concat(Character.toString(buffer[i]));
			}
			
			if(replaceName)
			{
				 query = query.replace(db, "connor");
			}
			else
			{
				query = query.replace(db, "`Sales Demo`.`" + db + "`");
				if(query.contains("CustomerName"))
				{
					query = query.replaceAll("CustomerName", "`Customer Name`");
				}			
				if(query.contains("OrderDayofWeek"))
				{
					query = query.replaceAll("OrderDayofWeek", "`Order Day of Week`");
				}
				if(query.contains("OrderDayMonth"))
				{
					query = query.replaceAll("OrderDayMonth", "`Order DayMonth`");
				}
				if(query.contains("OrderDayWeek"))
				{
					query = query.replaceAll("OrderDayWeek", "`Order DayWeek`");
				}
				if(query.contains("OrderDimensions"))
				{
					query = query.replaceAll("OrderDimensions", "`Order Dimensions`");
				}
				if(query.contains("OrderMonth"))
				{
					query = query.replaceAll("OrderMonth", "`Order Month`");
				}
				if(query.contains("OrderQuarter"))
				{
					query = query.replaceAll("OrderQuarter", "`Order Quarter`");
				}
				if(query.contains("OrderWeek"))
				{
					query = query.replaceAll("OrderWeek", "`Order Week`");
				}
				if(query.contains("OrderYearMonth"))
				{
					query = query.replaceAll("OrderYearMonth", "`Order YearMonth`");
				}
				if(query.contains("OrderYearWeek"))
				{
					query = query.replaceAll("OrderYearWeek", "`Order YearWeek`");
				}
				//**
				if(query.contains("ProductCategory"))
				{
					query = query.replaceAll("ProductCategory", "`Product Category`");
				}
				if(query.contains("ProductLine"))
				{
					query = query.replaceAll("ProductLine", "`Product Line`");
				}
				if(query.contains("ProductName"))
				{
					query = query.replaceAll("ProductName", "`Product Name`");
				}
				//**
				if(query.contains("ShipDayofWeek"))
				{
					query = query.replaceAll("ShipDayofWeek", "`Ship Day of Week`");
				}
				if(query.contains("ShipDayMonth"))
				{
					query = query.replaceAll("ShipDayMonth", "`Ship DayMonth`");
				}
				if(query.contains("ShipMonth"))
				{
					query = query.replaceAll("ShipMonth", "`Ship Month`");
				}
				if(query.contains("ShipDayWeek"))
				{
					query = query.replaceAll("ShipDayWeek", "`Ship DayWeek`");
				}
				if(query.contains("ShipQuarter"))
				{
					query = query.replaceAll("ShipQuarter", "`Ship Quarter`");
				}
				if(query.contains("ShipWeek"))
				{
					query = query.replaceAll("ShipWeek", "`Ship Week`");
				}
				if(query.contains("ShipYearMonth"))
				{
					query = query.replaceAll("ShipYearMonth", "`Ship YearMonth`");
				}
				if(query.contains("ShipYearWeek"))
				{
					query = query.replaceAll("ShipYearWeek", "`Ship YearWeek`");
				}
				//**
				if(query.contains("ZipCode"))
				{
					query = query.replace("ZipCode", "`Zip Code`");
				}
			}
			
			System.out.println(query);
			System.out.println("Connecting");
			return statement.executeQuery(query);
		}
    }
    
    private void processing(ResultSet rs, String query, char[] buffer, DataOutputStream out, String nameChange) throws SQLException, IOException
    {
    	short columnCount = (short) rs.getMetaData().getColumnCount();
		System.out.println("Data Recived");
		
		String flags = "";
		for(int i = endIndex; i < buffer.length; i++)
		{
			flags = flags.concat(Character.toString(buffer[i]));
		}
		
		byte[] combinded;
		
		if(flags.indexOf('E') != -1)
		{
			//ROW DESC CREATION
			byte[] rowDesc = createRowDesc(rs, columnCount);

			//DATA ROW CREATION
			byte[] dataRow = createDataRows(rs, columnCount, nameChange);

			//COMMAND COMPLETION CREATION
			byte[] comComp = createCommandCompletion(query);
			
			combinded = merge(new byte[][] {PARSE_COMP, BIND_COMP, rowDesc, dataRow, comComp, READY});
		}
		else
		{
			//ROW DESC CREATION
			byte[] rowDesc = createRowDesc(rs, columnCount);
			
			combinded = merge(new byte[][] {PARSE_COMP, new byte[]{(byte)'t',0,0,0,6,0,0}, rowDesc, READY}); //Missing ParameterDescription ('t') but prob not needed
		}
		
		out.write(combinded);
	}
    
    private byte[] createRowDesc(ResultSet rs, short columnCount) throws SQLException
    {
    	byte [] temp = ByteBuffer.allocate(2).putShort(columnCount).array();
		byte[] column;
		char[] chars;
		byte[] name;
		int length = 4 + 2;
		for(int i = 1; i <= columnCount; i++)
		{
			chars = rs.getMetaData().getColumnName(i).toCharArray();
			name = new byte[chars.length + 1];
			for(int j = 0; j < chars.length; j++)
			{
				name[j] = (byte)chars[j];
			}
			name[chars.length] = 0; //Terminating null at end of string
			column = merge(new byte[][] {name, new byte[] {0,0,0,0}/*TableOID (not needed)*/,new byte[] {0,0}/*Column Index (not needed)*/});
			
			String type = rs.getMetaData().getColumnTypeName(i);;
			
			/*System.out.println("*");
			System.out.println(rs.getMetaData().getColumnName(i));
			System.out.println(rs.getMetaData().getColumnTypeName(i));
			System.out.println("*");*/
			
			if(type == "varchar")
			{
				column = merge(new byte[][] {column, ByteBuffer.allocate(4).putInt(1043).array(),/*Data type OID*/ new byte[] {(byte) 0xff,(byte) 0xff}, ByteBuffer.allocate(4).putInt(259).array(), new byte[] {0,0}});
			}
			else
			{
				if(type == "int4")
				{
					column = merge(new byte[][] {column, ByteBuffer.allocate(4).putInt(23).array(),/*Data type OID*/ new byte[] {0,4}});
				}
				else if(type == "oid")
				{
					column = merge(new byte[][] {column, ByteBuffer.allocate(4).putInt(26).array(),/*Data type OID*/ new byte[] {0,4}});
				}
				else if(type == "text")
				{
					column = merge(new byte[][] {column, ByteBuffer.allocate(4).putInt(25).array(),/*Data type OID*/ ByteBuffer.allocate(2).putShort((short)255).array()});
				}
				else if(type == "name")
				{
					column = merge(new byte[][] {column, ByteBuffer.allocate(4).putInt(19).array(),/*Data type OID*/ ByteBuffer.allocate(2).putShort((short)255).array()});
				}
				else if(type == "int2")
				{
					column = merge(new byte[][] {column, ByteBuffer.allocate(4).putInt(21).array(),/*Data type OID*/ new byte[] {0,2}});
				}
				else if(type == "int8")
				{
					column = merge(new byte[][] {column, ByteBuffer.allocate(4).putInt(20).array(),/*Data type OID*/ new byte[] {0,8}});
				}
				else if(type == "char")
				{
					column = merge(new byte[][] {column, ByteBuffer.allocate(4).putInt(18).array(),/*Data type OID*/ new byte[] {0,1}});
				}
				else if(type == "bool")
				{
					column = merge(new byte[][] {column, ByteBuffer.allocate(4).putInt(16).array(),/*Data type OID*/ new byte[] {0,1}});
				}
				
				column = merge(new byte[][] {column, new byte[] {(byte) 0xff,(byte) 0xff,(byte) 0xff,(byte) 0xff},new byte[] {0,0}});
			}
			
			temp = merge(new byte[][] {temp,column});
			length += name.length + 4 + 2 + 4 + 2 + 4 + 2;
		}
		byte[] byteLength = ByteBuffer.allocate(4).putInt(length).array();
		return merge(new  byte[][] {new byte[] {84}, byteLength, temp});
    }
    
    private byte[] createDataRows(ResultSet rs, short columnCount, String nameChange) throws SQLException
    {
		byte[] dataRow = new byte[0];
		while(rs.next())
		{
			rowCounter++;
			byte[] drId = new byte[] {'D'};
			int length = 4 + 2;
			
			byte[][] columns = new byte[columnCount][];
			for(int i = 0; i < columnCount; i++)
			{
				if(rs.getObject(i+1) == null)
				{
					columns[i] = ByteBuffer.allocate(4).putInt(-1).array();
				}
				else
				{
					String data = "";
					if(nameChange != null && i == 2)
					{
						data = nameChange;
					}
					else
					{
						data = rs.getObject(i+1).toString();
					}
					int colLength = data.length();
					length += colLength + 4;
					byte[] byteColLength = ByteBuffer.allocate(4).putInt(colLength).array();
					columns[i] = new byte[data.length()];
					for(int j = 0; j < data.length(); j++)
					{
						columns[i][j] = (byte)data.charAt(j);
					}
					columns[i] = merge(new byte[][] {byteColLength, columns[i]});
				}
			}
			byte[] colData = merge(columns);
			byte[] byteLength = ByteBuffer.allocate(4).putInt(length).array();
			byte [] byteColumnCount = ByteBuffer.allocate(2).putShort(columnCount).array();
			dataRow = merge(new byte[][] {dataRow, drId,byteLength,byteColumnCount, colData});
		}
		return dataRow;
    }
    
    private byte[] createDataRowsAlternate(ResultSet rs, short columnCount, ArrayList<ArrayList<String>> tableColumns) throws SQLException
    {
		byte[] dataRow = new byte[0];
		while(!tableColumns.isEmpty())
		{
			rowCounter++;
			byte[] drId = new byte[] {'D'};
			int length = 4 + 2;
			
			byte[][] columns = new byte[columnCount][];
			for(int i = 0; i < columnCount; i++)
			{
				String data = "";
				data = tableColumns.get(0).get(0);
				tableColumns.get(0).remove(0);
				
				int colLength = data.length();
				length += colLength + 4;
				byte[] byteColLength = ByteBuffer.allocate(4).putInt(colLength).array();
				columns[i] = new byte[data.length()];
				for(int j = 0; j < data.length(); j++)
				{
					columns[i][j] = (byte)data.charAt(j);
				}
				columns[i] = merge(new byte[][] {byteColLength, columns[i]});
			}
			tableColumns.remove(0);
			byte[] colData = merge(columns);
			byte[] byteLength = ByteBuffer.allocate(4).putInt(length).array();
			byte [] byteColumnCount = ByteBuffer.allocate(2).putShort(columnCount).array();
			dataRow = merge(new byte[][] {dataRow,drId,byteLength,byteColumnCount,colData});
		}
		return dataRow;
    }
    
    private byte[] createCommandCompletion(String query)
    {
    	byte[] ccId = new byte[] {'C'};
		String command = query.substring(0, query.indexOf(' ')).toUpperCase();
		char[] charCommand = command.toCharArray();
		System.out.println("Row Count: " + rowCounter);
		String strRowCount = Integer.toString(rowCounter);
		rowCounter = 0;
		byte[] byteCommand = new byte[charCommand.length + 1 + strRowCount.length() + 1];
		for(int i = 0; i < charCommand.length; i++)
		{
			byteCommand[i] = (byte)charCommand[i];
		}
		byteCommand[charCommand.length] = 32; //Space
		for(int i = 0; i < strRowCount.length(); i++)
		{
			byteCommand[i + 1 + charCommand.length] = (byte)strRowCount.charAt(i);
		}
		byteCommand[byteCommand.length-1] = 0;
		int length = 4 + byteCommand.length;
		byte[] byteLength = ByteBuffer.allocate(4).putInt(length).array();
		return merge(new byte[][] {ccId,byteLength,byteCommand});
    }
    
    private static byte[] merge(byte[][] array)
    {
    	int i = 0;
    	int pos = 1;
    	while(pos < array.length)
    	{
    		byte[] combinded = new byte[array[0].length + array[pos].length];
    		for(i = 0; i < array[0].length; i++)
    		{
    			combinded[i] = array[0][i];
    		}
    		for(int j = 0; j < array[pos].length; j++)
    		{
    			combinded[i+j] = array[pos][j];
    		}
    		array[0] = combinded;
    		pos++;
    	}
    	return array[0];
    }
}
