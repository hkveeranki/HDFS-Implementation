
public class Reducer implements Reducerdef {

    public Reducer() { }

    public String reduce(String query) {
        if (query != null && query.contains(":true")) {
            return query.split(":")[0] + "\n";
        }
        return "";
    }

}
