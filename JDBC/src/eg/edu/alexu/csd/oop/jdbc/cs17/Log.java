package eg.edu.alexu.csd.oop.jdbc.cs17;

import java.io.File;
import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Log {
	private Logger logger;
	private FileHandler fh;
	public Log() {
		File file=new File("log.txt");
		if(!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			fh=new FileHandler("log.txt", true);
		} catch (SecurityException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger=Logger.getLogger("test");
		logger.addHandler(fh);
		logger.setLevel(Level.INFO);
		SimpleFormatter formatter=new SimpleFormatter();
		fh.setFormatter(formatter);	
	}
	public Logger getLogger() {
		return this.logger;
	}

}
