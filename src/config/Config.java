package config;

public class Config {

    //Postgres connection credentials
    public static String dbURL = "jdbc:postgresql://localhost:5432/gis"; //160.40.63.119
//    public static String dbURL = "jdbc:postgresql://localhost:15432/gis"; //localhost
    public static String uName = "postgres";
    public static String uPass= "password";

    //Postgres tables
    public static String tblTraffic = "_traffic_way";

}