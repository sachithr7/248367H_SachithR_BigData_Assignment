package org.example;

import au.com.bytecode.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Main {
    // Load data from s3 bucket to flight_delay_data table
    public static class Map {
        public void map(Connection con) throws SQLException {
            System.out.println("Map Executed");
            try{
                Class.forName(driverName);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }
            Statement stmt = con.createStatement();
            stmt.execute("CREATE EXTERNAL TABLE IF NOT EXISTS delay_flights (\n" +
                    "    Id INT,\n" +
                    "    Year INT,\n" +
                    "    Month INT,\n" +
                    "    DayofMonth INT,\n" +
                    "    DayOfWeek INT,\n" +
                    "    DepTime INT,\n" +
                    "    CRSDepTime INT,\n" +
                    "    ArrTime FLOAT,\n" +
                    "    CRSArrTime INT,\n" +
                    "    UniqueCarrier STRING,\n" +
                    "    FlightNum INT,\n" +
                    "    TailNum STRING,\n" +
                    "    ActualElapsedTime FLOAT,\n" +
                    "    CRSElapsedTime INT,\n" +
                    "    AirTime FLOAT,\n" +
                    "    ArrDelay FLOAT,\n" +
                    "    DepDelay INT,\n" +
                    "    Origin STRING,\n" +
                    "    Dest STRING,\n" +
                    "    Distance INT,\n" +
                    "    TaxiIn FLOAT,\n" +
                    "    TaxiOut INT,\n" +
                    "    Cancelled INT,\n" +
                    "    CancellationCode STRING,\n" +
                    "    Diverted INT,\n" +
                    "    CarrierDelay FLOAT,\n" +
                    "    WeatherDelay FLOAT,\n" +
                    "    NASDelay FLOAT,\n" +
                    "    SecurityDelay FLOAT,\n" +
                    "    LateAircraftDelay INT\n" +
                    ")\n" +
                    "ROW FORMAT DELIMITED\n" +
                    "FIELDS TERMINATED BY ','\n" +
                    "LOCATION 'flightdata.csv';");
        }
    }
    // Calculate Response time for 5 transactions
    public static class Reduce {
        public void reduce(Connection con) throws SQLException, IOException {
            System.out.println("Reduce Executed");
            Statement stmt = con.createStatement();

            FileWriter outputfile = new FileWriter("./");
            CSVWriter writer = new CSVWriter(outputfile);

            String[] header = {"time in millisecodes"};
            writer.writeNext(header);

            List executionTimeList = new ArrayList();
            executeCarrierDelayQueryIterateFiveTimes(stmt, executionTimeList, writer);
        }

        private void executeCarrierDelayQueryIterateFiveTimes(Statement stmt, List executionTimeList, CSVWriter writer) throws SQLException {
            for (int i = 0; i < 5; i++) {
                long startTime = System.nanoTime();
                stmt.execute("SELECT Year as YEAR, SUM((CarrierDelay/ArrDelay)*100) as CarrerDalyPresentage FROM flight_delay_data WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year");
                long endTime = System.nanoTime();
                long executionTimeInMilliseconds = (endTime - startTime) / 1_000_000;
                executionTimeList.add(executionTimeInMilliseconds);
                String[] executionTimeArray = { Long.toString(executionTimeInMilliseconds) };
                writer.writeNext(executionTimeArray);
                System.out.println("executionTimeInMilliseconds: " + executionTimeInMilliseconds);
            }
        }

        private void executeCarrierDelayQuery(Statement stmt, List executionTimeList, CSVWriter writer) throws SQLException {
            long startTime = System.nanoTime();
            stmt.execute("SELECT Year as YEAR, SUM((CarrierDelay/ArrDelay)*100) as CarrerDalyPresentage FROM flight_delay_data WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year");
            long endTime = System.nanoTime();
            long executionTimeInMilliseconds = (endTime - startTime) / 1_000_000;
            executionTimeList.add(executionTimeInMilliseconds);
            String[] executionTimeArray = { Long.toString(executionTimeInMilliseconds) };
            writer.writeNext(executionTimeArray);
            System.out.println("executionTimeInMilliseconds: " + executionTimeInMilliseconds);
        }
    }

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args) {
        try {
            Connection con = DriverManager.getConnection(
                    "jdbc:hive2://localhost:10000/default", "hadoop", "");
            Map map = new Map();
            map.map(con);

            Reduce reduce = new Reduce();
            reduce.reduce(con);
        } catch (SQLException e) {
            System.out.println("SQL Exception triggered");
        } catch (IOException e) {
            System.out.println("IO Exception triggered");
        } catch (Exception e) {
            System.out.println("Generic Exception triggered");
        }
    }
}