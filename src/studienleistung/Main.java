package studienleistung;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Main {
    static Connection connex;


    static List<String> ResultToList(ResultSet RS)
    {
        List<String> SL = new ArrayList<>();
        StringBuilder SB = new StringBuilder();
        try
        {
            while (RS.next())
            {
                SB.setLength(0);
                for (int I = 1; true; I++)
                    try
                    {
                        SB.append(RS.getObject(I)).append('\t');
                    }
                    catch (Exception ignored)
                    {
                        SB.deleteCharAt(SB.length() - 1);
                        break;
                    }
                SL.add(SB.toString());
            }
        }
        catch (SQLException E)
        {
            System.out.println("Exception: " + E.getMessage());
        }
        return SL;
    }
    static String ResultToString(ResultSet RS)
    {
        List<String> RL = ResultToList(RS);
        return String.join(System.lineSeparator(), RL);
    }

    static void StructDB() {
        try {
            Statement stmt = connex.createStatement();
            int result;
            // Tabelle Genre: [GenreID | Genre | MarketShare]
            result = stmt.executeUpdate("DROP TABLE IF EXISTS Genre CASCADE ");
            result = stmt.executeUpdate(("CREATE TABLE Genre(GenreID integer UNIQUE NOT NULL PRIMARY KEY, Genre text NOT NULL, MarketShare integer NOT NULL)"));

            // Tabelle GameInfo : [Game | Developer | ReleaseYear | SoldCopies]
            result = stmt.executeUpdate("DROP TABLE IF EXISTS GameInfo CASCADE ");
            result = stmt.executeUpdate("CREATE TABLE GameInfo(Game text UNIQUE NOT NULL PRIMARY KEY , Developer text NOT NULL, ReleaseYear integer NOT NULL, SoldCopies integer NOT NULL)");


            // Tabelle Game: [GenreID | Game]

            result = stmt.executeUpdate("DROP TABLE IF EXISTS Game CASCADE ");
            result = stmt.executeUpdate("CREATE TABLE Game(GenreID integer NOT NULL REFERENCES Genre, Game text NOT NULL REFERENCES GameInfo)");
            stmt.close();

        } catch (SQLException E) {
            System.out.println("Exception: " + E.getMessage());
        }

    }

    static void UpdateDB() {
        try {
            Statement stmt = connex.createStatement();
            int result;

            // Tabelle Genre
            result = stmt.executeUpdate("DELETE FROM Genre"); //wipe tabelle clean

            result = stmt.executeUpdate("INSERT INTO Genre VALUES (0, 'Shooter', 66)");
            result = stmt.executeUpdate("INSERT INTO Genre VALUES (1, 'Action adventure', 62)");
            result = stmt.executeUpdate("INSERT INTO Genre VALUES (2, 'Simulation', 43)");

            stmt.close();
        } catch(SQLException E) {
            System.out.println("UpdateDB error: " + E.getMessage());
        }
    }

    static void QueryAll() {
        try {
            Statement stmt = connex.createStatement();
            ResultSet resultSet;

            resultSet = stmt.executeQuery("SELECT * FROM Genre");
            System.out.println(ResultToString(resultSet));
            resultSet.close();


            stmt.close();
        } catch(SQLException E) {
            System.out.println("QueryAll error: " + E.getMessage());
        }
    }
    public static void main(String[] args) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch(ClassNotFoundException E) {
            System.out.println("Exception : " + E.getMessage());
        }

        try {
            connex = DriverManager.getConnection("jdbc:postgresql:GameGenres", "postgres", "blubb");


            StructDB();
            UpdateDB();
            QueryAll();


            connex.close();
        } catch(SQLException E) {
            System.out.println("Connection error: " + E.getMessage());
        }
    }
}
