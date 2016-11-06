
public class Reducer implements Reducerdef {
    public String reduce(String query) {
        if(query != null && query.contains(":true")) {
            return query.split(":")[0];
        }
        return "";
    }
}
