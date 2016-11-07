import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Mapper implements Mapperdef {

    private Helper helper;

    Mapper(Helper helper) {
        this.helper = helper;
    }

    public String map(String query) {
        String search_str = helper.read_from_hdfs("job.xml");
        if (query != null && !query.isEmpty() && search_str != null && !search_str.isEmpty()) {
            Pattern r = Pattern.compile(search_str);
            Matcher m = r.matcher(query);
            if (m.find()) {
                return query + ":true";
            }
        }
        return "";
    }
}
