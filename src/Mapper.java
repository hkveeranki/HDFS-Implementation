import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Mapper implements Mapperdef {

    private String search_str;

    public Mapper(String search_regex) {
        search_str = search_regex;
        System.err.println("Mappers Query is: " + search_str);
    }

    public String map(String query) {
        if (query != null && !query.isEmpty() && !search_str.isEmpty()) {
            Pattern r = Pattern.compile(search_str);
            Matcher m = r.matcher(query);
            if (m.find()) {
                return query + ":true" + "\n";
            }
        }
        return "";
    }
}
