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

            //Tabelle ProPlayer: [PlayerID | PlayerForename | GamerTag | PlayerSurname| Game]
            result = stmt.executeUpdate("DROP TABLE IF EXISTS ProPlayer CASCADE");
            result = stmt.executeUpdate("CREATE TABLE ProPlayer(PlayerID integer UNIQUE NOT NULL PRIMARY KEY, PlayerForename text NOT NULL, GamerTag text NOT NULL, PlayerSurname text NOT NULL, Game text NOT NULL REFERENCES Game)");

            //Tabelle Owner: [OwnerID | OwnerSurname | OwnerForename]
            result = stmt.executeUpdate("DROP TABLE IF EXISTS Owner CASCADE");
            result = stmt.executeUpdate("CREATE TABLE Owner(OwnerID integer UNIQUE NOT NULL PRIMARY KEY, OwnerSurname text NOT NULL, OwnerForename text NOT NULL)");

            //Tabelle Team: [Team | OwnerID | Founded]
            result = stmt.executeUpdate("DROP TABLE IF EXISTS Team CASCADE");
            result = stmt.executeUpdate("CREATE TABLE Team(Team text unique NOT NULL PRIMARY KEY, OwnerID integer UNIQUE NOT NULL REFERENCES Owner, Founded date NOT NULL)");

            //Tabelle PlaysIn: [PlayerID | Team]
            result = stmt.executeUpdate("DROP TABLE IF EXISTS PlaysIn CASCADE ");
            result = stmt.executeUpdate("CREATE TABLE PlaysIn(PlayerID integer NOT NULL REFERENCES ProPlayer, Team text NOT NULL REFERENCES Team)");
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

            //ProPlayer
            result = stmt.executeUpdate("DELETE FROM ProPlayer");
            stmt.addBatch("INSERT INTO ProPlayer VALUES(0, 'Lee', 'Faker', 'Sang-hyeok', 'League of Legends')");
            stmt.addBatch("INSERT INTO ProPlayer VALUES(1, 'Oleksandr', 's1mple', 'Kostyljev', 'Counter-Strike: Global Offensive')");
            stmt.addBatch("INSERT INTO ProPlayer VALUES(2, 'Danil', 'Dendi', 'Ishutin', 'Dota 2')");
            stmt.addBatch("INSERT INTO ProPlayer VALUES(3, 'Olof', 'olofmeister', 'Kajbjer', 'Counter-Strike: Global Offensive')");
            stmt.addBatch("INSERT INTO ProPlayer VALUES(4, 'Martin', 'Rekkles', 'Larsson', 'League of Legends')");
            stmt.addBatch("INSERT INTO ProPlayer VALUES(5, 'Michael', 'MoneyBoy', 'Geld', 'League of Legends')");
            stmt.addBatch("INSERT INTO ProPlayer VALUES(6, 'Tyson', 'TenZ', 'Ngo', 'Valorant')");
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

            System.out.println("PROPLAYERS");
            resultSet = stmt.executeQuery("SELECT * FROM ProPlayer");
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

    //  i)
    static void QueryDB_1() {
        try {
            Statement stmt = connex.createStatement();
            ResultSet resultSet;
            resultSet = stmt.executeQuery("SELECT GamerTag FROM ProPlayer");
            System.out.println("===All GamerTags===");
            System.out.println(ResultToString(resultSet));
            System.out.println();
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
            System.out.println("===All Teams formed between 2001 and 2011===");
            System.out.println(ResultToString(resultSet));
            System.out.println();
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
            resultSet = stmt.executeQuery("SELECT OwnerSurname, OwnerForename FROM Owner WHERE OwnerSurname LIKE 'M%' OR OwnerForename LIKE 'J%'");
            System.out.println("===All Team Owners whose Surname start with m or Forename start with j===");
            System.out.println(ResultToString(resultSet));
            System.out.println();
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
            System.out.println("===All games descendengly ordered by peak viewership===");
            System.out.println(ResultToString(resultSet));
            System.out.println();
            resultSet.close();

            stmt.close();
        } catch (SQLException E) {
            System.out.println("Query 4 error: " + E.getMessage());
        }
    }
    static void QueryDB_5() {
        try {
            Statement stmt = connex.createStatement();
            ResultSet resultSet;
            resultSet = stmt.executeQuery("SELECT * FROM Game WHERE Game.releasedate = (SELECT MAX(Game.releasedate) FROM Game)");
            System.out.println("===Select newest Game===");
            System.out.println(ResultToString(resultSet));
            System.out.println();
            resultSet.close();

            stmt.close();
        } catch (SQLException E) {
            System.out.println("Query 5 error: " + E.getMessage());
        }
    }

    //  ii)
    static void QueryDB_6() {
        try {
            Statement stmt = connex.createStatement();
            ResultSet resultSet;
            //resultSet = stmt.executeQuery("SELECT ProPlayer.Gamertag, Team.Team FROM ProPlayer, Team, PlaysIN  WHERE ProPlayer.PlayerID = PlaysIn.PlayerID AND Team.team = PlaysIn.team");
            resultSet = stmt.executeQuery("SELECT ProPlayer.Gamertag, PlaysIn.team FROM ProPlayer FULL OUTER JOIN PlaysIN USING(PlayerId)");
            System.out.println("===Players GamerTag and their corresponding Team they are playing in===");
            System.out.println(ResultToString(resultSet));
            System.out.println();
            resultSet.close();

            stmt.close();
        } catch (SQLException E) {
            System.out.println("Query 6 error: " + E.getMessage());
        }
    }

    static void QueryDB_7() {
        try {
            Statement stmt = connex.createStatement();
            ResultSet resultSet;
            resultSet = stmt.executeQuery("SELECT ProPlayer.* FROM ProPlayer WHERE NOT EXISTS(SELECT PlaysIn.playerid FROM PlaysIn WHERE PlaysIn.playerid = ProPlayer.PlayerId)");
            System.out.println("===Players who are not playing in a Team===");
            System.out.println(ResultToString(resultSet));
            System.out.println();
            resultSet.close();

            stmt.close();
        } catch (SQLException E) {
            System.out.println("Query 7 error: " + E.getMessage());
        }
    }

    static void QueryDB_8() {
        try {
            Statement stmt = connex.createStatement();
            ResultSet resultSet;
            resultSet = stmt.executeQuery("SELECT Game.*, ProPlayer.gamertag FROM Game LEFT OUTER JOIN ProPlayer ON Game.game = ProPlayer.Game ORDER BY Game.Game");
            System.out.println("===Games and Players playing that Game===");
            System.out.println(ResultToString(resultSet));
            System.out.println();
            resultSet.close();

            stmt.close();
        } catch (SQLException E) {
            System.out.println("Query 8 error: " + E.getMessage());
        }
    }

    static void QueryDB_9() {
        try {
            Statement stmt = connex.createStatement();
            ResultSet resultSet;
            //resultSet = stmt.executeQuery("SELECT ProPlayer.GamerTag, Game.* FROM ProPlayer, Game WHERE ProPlayer.Game = Game.Game AND Game.releasedate = (SELECT MAX(releasedate) FROM Game)");
            resultSet = stmt.executeQuery("SELECT * FROM(SELECT ProPlayer.GamerTag, Game.* FROM ProPlayer FULL OUTER JOIN Game ON ProPlayer.Game = Game.Game) AS Subtable WHERE Subtable.releasedate = (SELECT MAX(Game.releasedate) FROM GAME)");
            System.out.println("===ProPlayers playing the newest Game===");
            System.out.println(ResultToString(resultSet));
            System.out.println();
            resultSet.close();

            stmt.close();
        } catch (SQLException E) {
            System.out.println("Query 9 error: " + E.getMessage());
        }
    }

    static void QueryDB_10() {
        try {
            Statement stmt = connex.createStatement();
            ResultSet resultSet;
            resultSet = stmt.executeQuery("SELECT Team.team, COUNT(PlaysIn.team) FROM Team LEFT JOIN PlaysIn ON Team.team = PlaysIn.team GROUP BY Team.team");
            System.out.println("===How many players in a team===");
            System.out.println(ResultToString(resultSet));
            System.out.println();
            resultSet.close();

            stmt.close();
        } catch (SQLException E) {
            System.out.println("Query 10 error: " + E.getMessage());
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
            QueryDB_5();
            QueryDB_6();
            QueryDB_7();
            QueryDB_8();
            QueryDB_9();
            QueryDB_10();
            connex.close();
        } catch(SQLException E) {
            System.out.println("Connection error: " + E.getMessage());
        }
    }
}
