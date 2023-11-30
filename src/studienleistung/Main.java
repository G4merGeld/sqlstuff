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


    //Create Tables
    static void StructDB() {
        try {
            Statement stmt = connex.createStatement();
            int result;
            //Tabelle Game: [Game | ReleaseDate | PeakViewerShip]
            result = stmt.executeUpdate("DROP TABLE IF EXISTS Game CASCADE");
            result = stmt.executeUpdate("CREATE TABLE Game(Game text UNIQUE NOT NULL PRIMARY KEY, ReleaseDate date NOT NULL, PeakViewership integer NOT NULL)");

            //Tabelle Player: [PlayerID | PlayerForname | GamerTag | PlayerSurname| Game]
            result = stmt.executeUpdate("DROP TABLE IF EXISTS Player CASCADE");
            result = stmt.executeUpdate("CREATE TABLE Player(PlayerID integer UNIQUE NOT NULL PRIMARY KEY, PlayerForname text NOT NULL, GamerTag text NOT NULL, PlayerSurname text NOT NULL, Game text NOT NULL REFERENCES Game)");

            //Tabelle Owner: [OwnerID | OwnerSurname | OwnerForname]
            result = stmt.executeUpdate("DROP TABLE IF EXISTS Owner CASCADE");
            result = stmt.executeUpdate("CREATE TABLE Owner(OwnerID integer UNIQUE NOT NULL PRIMARY KEY, OwnerSurname text NOT NULL, OwnerForname text NOT NULL)");

            //Tabelle Team: [Team | OwnerID | Founded]
            result = stmt.executeUpdate("DROP TABLE IF EXISTS Team CASCADE");
            result = stmt.executeUpdate("CREATE TABLE Team(Team text unique NOT NULL PRIMARY KEY, OwnerID integer UNIQUE NOT NULL REFERENCES Owner, Founded date NOT NULL)");

            //Tabelle PlaysIn: [PlayerID | Team]
            result = stmt.executeUpdate("DROP TABLE IF EXISTS PlaysIn CASCADE ");
            result = stmt.executeUpdate("CREATE TABLE PlaysIn(PlayerID integer NOT NULL REFERENCES Player, Team text NOT NULL REFERENCES Team)");
            stmt.close();
        } catch (SQLException E) {
            System.out.println("Exception: " + E.getMessage());
        }

    }

    static void UpdateDB() {
        try {
            Statement stmt = connex.createStatement();
            int result;

            //Game
            result = stmt.executeUpdate("DELETE FROM Game");
            stmt.addBatch("INSERT INTO Game VALUES('League of Legends', '2009-10-27', 6402760)");
            stmt.addBatch("INSERT INTO Game VALUES('Dota 2', '2013-07-09', 2741514)");
            stmt.addBatch("INSERT INTO Game VALUES('Counter-Strike: Global Offensive','2012-08-21', 2748434)");
            stmt.addBatch("INSERT INTO Game VALUES('Fortnite', '2017-07-21', 2334826)");
            stmt.addBatch("INSERT INTO Game VALUES('Valorant', '2020-06-02', 1505804)");
            stmt.executeBatch();

            //Player
            result = stmt.executeUpdate("DELETE FROM Player");
            stmt.addBatch("INSERT INTO Player VALUES(0, 'Lee', 'Faker', 'Sang-hyeok', 'League of Legends')");
            stmt.addBatch("INSERT INTO Player VALUES(1, 'Oleksandr', 's1mple', 'Kostyljev', 'Counter-Strike: Global Offensive')");
            stmt.addBatch("INSERT INTO Player VALUES(2, 'Danil', 'Dendi', 'Ishutin', 'Dota 2')");
            stmt.addBatch("INSERT INTO Player VALUES(3, 'Olof', 'olofmeister', 'Kajbjer', 'Counter-Strike: Global Offensive')");
            stmt.addBatch("INSERT INTO Player Values(4, 'Martin', 'Rekkles', 'Larsson', 'League of Legends')");
            stmt.executeBatch();

            //Owner
            result = stmt.executeUpdate("DELETE FROM Owner");
            stmt.addBatch("INSERT INTO Owner VALUES(0, 'Matthews', 'Sam')");
            stmt.addBatch("INSERT INTO Owner VALUES(1, 'Marsh', 'Joe')");
            stmt.addBatch("INSERT INTO Owner VALUES(2, 'Zolotarov', 'Yevhen')");
            stmt.addBatch("INSERT INTO Owner VALUES(3, 'Etienne', 'Jack')");
            stmt.addBatch("INSERT INTO Owner VALUES(4, 'Dinh', 'Andy')");
            stmt.addBatch("INSERT INTO Owner VALUES(5, 'Bengtson', 'Richard')");
            stmt.executeBatch();

            //Team
            result = stmt.executeUpdate("DELETE FROM Team");
            stmt.addBatch("INSERT INTO Team VALUES('FaZe Clan', 5, '2010-05-30')");
            stmt.addBatch("INSERT INTO Team VALUES('T1', 1, '2014-04-13')");
            stmt.addBatch("INSERT INTO TEAM VALUES('Cloud9', 3, '2013-04-01')");
            stmt.addBatch("INSERT INTO Team VALUES('Fnatic', 0, '2004-07-23')");
            stmt.addBatch("INSERT INTO Team VALUES('Natus Vincere', 2, '2009-12-17')");
            stmt.addBatch("INSERT INTO TEAM VALUES('Team SoloMid', 4, '2011-01-01')");
            stmt.executeBatch();

            //PlaysIn
            result = stmt.executeUpdate("DELETE FROM PlaysIn");
            stmt.addBatch("INSERT INTO PlaysIn VALUES(0, 'T1')");
            stmt.addBatch("INSERT INTO PlaysIn VALUES(1, 'Natus Vincere')");
            stmt.addBatch("INSERT INTO PlaysIn VALUES(2, 'Natus Vincere')");
            stmt.addBatch("INSERT INTO PlaysIn VALUES(3, 'Fnatic')");
            stmt.addBatch("INSERT INTO PlaysIn VALUES(4, 'Fnatic')");
            stmt.executeBatch();

            stmt.close();
        } catch(SQLException E) {
            System.out.println("UpdateDB error: " + E.getMessage());
        }
    }

    static void QueryAll() {
        try {
            Statement stmt = connex.createStatement();
            ResultSet resultSet;

            System.out.println("GAMES");
            resultSet = stmt.executeQuery("SELECT * FROM Game");
            System.out.println(ResultToString(resultSet));

            System.out.println("PLAYERS");
            resultSet = stmt.executeQuery("SELECT * FROM Player");
            System.out.println(ResultToString(resultSet));
            resultSet.close();

            System.out.println("OWNERS");
            resultSet = stmt.executeQuery("SELECT * FROM Owner");
            System.out.println(ResultToString(resultSet));
            resultSet.close();

            System.out.println("TEAMS");
            resultSet = stmt.executeQuery("SELECT * FROM Team");
            System.out.println(ResultToString(resultSet));
            resultSet.close();

            System.out.println("PlaysIn");
            resultSet = stmt.executeQuery("SELECT * FROM PlaysIn");
            System.out.println(ResultToString(resultSet));
            resultSet.close();

            stmt.close();
        } catch(SQLException E) {
            System.out.println("QueryAll error: " + E.getMessage());
        }
    }

    //i)
    static void QueryDB_1() {
        try {
            Statement stmt = connex.createStatement();
            ResultSet resultSet;
            resultSet = stmt.executeQuery("SELECT GamerTag FROM Player");
            System.out.println("All gamertags");
            System.out.println(ResultToString(resultSet));
            resultSet.close();

            stmt.close();
        } catch (SQLException E) {
            System.out.println("Query 1 error: " + E.getMessage());
        }
    }

    static void QueryDB_2() {
        try {
            Statement stmt = connex.createStatement();
            ResultSet resultSet;
            resultSet = stmt.executeQuery("SELECT Team FROM Team WHERE Founded BETWEEN '20000101' AND '20101230'");
            System.out.println("All teams formed between 2001 and 2011");
            System.out.println(ResultToString(resultSet));
            resultSet.close();

            stmt.close();
        } catch (SQLException E) {
            System.out.println("Query 2 error: " + E.getMessage());
        }
    }

    static void QueryDB_3() {
        try {
            Statement stmt = connex.createStatement();
            ResultSet resultSet;
            resultSet = stmt.executeQuery("SELECT OwnerSurname, OwnerForname FROM Owner WHERE OwnerSurname LIKE 'M%' OR OwnerForname LIKE 'J%'");
            System.out.println("All team owners whose surname start with m or forname start with j");
            System.out.println(ResultToString(resultSet));
            resultSet.close();

            stmt.close();
        } catch (SQLException E) {
            System.out.println("Query 3 error: " + E.getMessage());
        }
    }

    static void QueryDB_4() {
        try {
            Statement stmt = connex.createStatement();
            ResultSet resultSet;
            resultSet = stmt.executeQuery("SELECT Game, PeakViewership FROM Game ORDER BY PeakViewership DESC");
            System.out.println("All games descendengly ordered by peak viewership");
            System.out.println(ResultToString(resultSet));
            resultSet.close();

            stmt.close();
        } catch (SQLException E) {
            System.out.println("Query 4 error: " + E.getMessage());
        }
    }

    public static void main(String[] args) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch(ClassNotFoundException E) {
            System.out.println(" Driver Exception : " + E.getMessage());
        }

        try {
            connex = DriverManager.getConnection("jdbc:postgresql:ESport", "postgres", "blubb");


            StructDB();
            UpdateDB();
            //QueryAll();
            QueryDB_1();
            QueryDB_2();
            QueryDB_3();
            QueryDB_4();

            connex.close();
        } catch(SQLException E) {
            System.out.println("Connection error: " + E.getMessage());
        }
    }
}
