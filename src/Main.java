import config.Config;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

public class Main {

    public static void main(String[] args) throws IOException, ClassNotFoundException, JSONException, ParserConfigurationException, SAXException {

        //connect to db
        Class.forName("org.postgresql.Driver");

        Connection con = null;
        PreparedStatement stmt = null;
        Statement statement;

        try {
            con = DriverManager.getConnection(Config.dbURL, Config.uName, Config.uPass);
            con.setAutoCommit(false);

            statement = con.createStatement();

            //truncate old data
//            statement.executeUpdate("TRUNCATE " + Config.tblTraffic);
//            con.commit();

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        Map<String, String> idCongestion = parseCongestion();

        for (Map.Entry<String, String> entry : idCongestion.entrySet())
        {
            System.out.println(entry.getKey());

            String select = "SELECT * FROM _traffic_way WHERE link_id='"+entry.getKey()+"'";
            try {
                stmt = con.prepareStatement(select);
                ResultSet rs = stmt.executeQuery();
                if(!rs.next()){

                    List<String> nodes = parseNodes(entry.getKey());

                    for(String node : nodes){
                        //insert to db
                        try {
                            String sql = "INSERT INTO _traffic_way(link_id,node,lat,lon) VALUES (?,?,?,?)";

                            stmt = con.prepareStatement(sql);

                            stmt.setString(1, entry.getKey());
                            stmt.setString(2, node);
                            stmt.setDouble(3, Double.parseDouble(getLatLon(node).split("_")[0]));
                            stmt.setDouble(4, Double.parseDouble(getLatLon(node).split("_")[1]));

                            stmt.executeUpdate();
                            con.commit();
                        } catch (Exception e) {
                            System.err.println(e.getMessage());
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        try {
            stmt.close();
            con.close();

            System.out.println("\n--- data inserted ---");
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

    }

    public static Map<String, String> parseCongestion() throws IOException, JSONException {
        Map<String, String> idCongestion = new HashMap<String, String>();

        //get data
        URL url = new URL("http://feed.opendata.imet.gr:23577/fcd/congestions.json?offset=0&limit=-1");
        URLConnection yc = url.openConnection();
        BufferedReader br = new BufferedReader(new InputStreamReader(yc.getInputStream()));

        String jsonData = "", line;

        while ((line = br.readLine()) != null) {
            jsonData += line + "\n";
        }

        br.close();

        JSONArray jsonarray = new JSONArray(jsonData);

        for (int i = 0; i < jsonarray.length(); i++) {
            JSONObject jsonobject = jsonarray.getJSONObject(i);
            String linkId = jsonobject.getString("Link_id");
            String congestion = jsonobject.getString("Congestion");

            idCongestion.put(linkId, congestion);
        }

        return idCongestion;
    }

    public static List<String> parseNodes(String linkId) throws IOException, SAXException, ParserConfigurationException {
        List<String> nodes = new ArrayList<String>();

        //get data
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        URL url = new URL("http://www.openstreetmap.org/api/0.6/way/" + linkId);

        //check connection
        HttpURLConnection huc =  (HttpURLConnection)  url.openConnection ();
        huc.setRequestMethod ("GET");
        huc.connect () ;
        int code = huc.getResponseCode() ;

        //parse xml data
        if(code == 200){
            Document doc = builder.parse(url.openStream());
            NodeList nList = doc.getElementsByTagName("nd");

            for (int i = 0; i < nList.getLength(); i++) {
                Node nNode = nList.item(i);

                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) nNode;
                    nodes.add(eElement.getAttribute("ref"));
                }
            }
        }

        return nodes;
    }

    public static String getLatLon(String node) throws ParserConfigurationException, IOException, SAXException {
        String latLon = null;

        //get data
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        URL url = new URL("http://www.openstreetmap.org/api/0.6/node/" + node);

        //check connection
        HttpURLConnection huc =  (HttpURLConnection)  url.openConnection ();
        huc.setRequestMethod ("GET");
        huc.connect () ;
        int code = huc.getResponseCode() ;

        //parse xml data
        if(code == 200){
            Document doc = builder.parse(url.openStream());
            NodeList nList = doc.getElementsByTagName("node");

            for (int i = 0; i < nList.getLength(); i++) {
                Node nNode = nList.item(i);

                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) nNode;
                    String lat = eElement.getAttribute("lat");
                    String lon = eElement.getAttribute("lon");

                    latLon = lat + "_" + lon;
                }
            }
        }

        return latLon;
    }
}