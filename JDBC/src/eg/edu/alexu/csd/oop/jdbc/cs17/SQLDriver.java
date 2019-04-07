package eg.edu.alexu.csd.oop.jdbc.cs17;

import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Logger;


public class SQLDriver implements Driver {
	private Log log=new Log();


	@Override
	public boolean acceptsURL(String url) throws SQLException {
		 if(url.equals("jdbc:xmldb://localhost")) { 
			 log.getLogger().info("Url accepted");
	    	   return true;
	       }
	       else {
	    	   log.getLogger().warning("Invalid url");
	    	   return false;
	       }
	}

	@Override
	public java.sql.Connection connect(String url, Properties info) throws SQLException {
		if(!acceptsURL(url)) {
			log.getLogger().warning("Invalid url");
			return null;
		} else {
			File f = (File) info.get("path");
			String path = f.getAbsolutePath();
			Connection con=new SQLConnection(path);
			log.getLogger().info("Connection returned successfully!");
			return con;
		}
	}

	@Override
	public int getMajorVersion() {
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public int getMinorVersion() {
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		DriverPropertyInfo[] arr=new DriverPropertyInfo[info.size()];
		int i=0;
		for(Entry entry:info.entrySet()) {
			arr[i]=(DriverPropertyInfo) entry.getValue();
			i++;
		}
		return arr;
	}

	@Override
	public boolean jdbcCompliant() {
		throw new java.lang.UnsupportedOperationException();
	}
}
