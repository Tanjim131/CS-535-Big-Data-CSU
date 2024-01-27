package util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class HashtagLogger {
   
    public static String addAngleBrackets(String s){
        return "<" + s + ">";
    }

    public static String getLogEntry(List<String> topHashtags){
        StringBuilder logEntry = new StringBuilder(addAngleBrackets(Long.toString(System.currentTimeMillis())));

        for(String hashtag: topHashtags){
            logEntry.append(addAngleBrackets(hashtag));
        }

		logEntry.append(System.lineSeparator());

        return logEntry.toString();
    }

    public static void logTopHashtags(List<String> topHashtags, String hashtagsLogFileName){
        String logEntry = getLogEntry(topHashtags);

        File hashtagLogFile = new File(hashtagsLogFileName);

        try {
            hashtagLogFile.createNewFile();
            Path file = Paths.get(hashtagsLogFileName);
            Files.write(file, logEntry.getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

	public static void log(String str, String hashtagsLogFileName){
        File logfile = new File(hashtagsLogFileName);

        try {
            logfile.createNewFile();
            Path file = Paths.get(hashtagsLogFileName);
			Files.write(file, str.concat(System.lineSeparator()).getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
