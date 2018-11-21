package com.company;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.sql.DriverManager.getConnection;

public class Main {
    private static final String question_1 = "Number of users in each class or project?";
    private static final String sql_1 = "Select project.pname, project.ptype, count(*) as numberOfUsers from project left join member_of on project.pid = member_of.pid group by project.pid";

    private static final String question_2 = "computing time for each job";
    private static final String sql_2 = "select jobid, (end_time - start_time) as comput_time from job order by jobid asc";

    private static final String question_3 = "Number of success runs";
    private static final String sql_3 = "select count(*) as numOfSuccess from job where status = 'success'";

    private static final String question_4 = "Number and reason of failure runs";
    private static final String sql_4 = "select jobid, status, descr from job where status = 'fail'";

    private static final String question_5 = "How much resources used by each project team";
    private static final String sql_5 = "SELECT * FROM resourceUsed";

    private static final String question_6 = "How much resources wasted by each team";
    private static final String sql_6 = "select A.pid, B.pname, (A.cores - B.coresUsed) as coresWaste, (A.gpu - B.gpuUsed) as gpuWaste, (A.ram - B.ramUsed) as ramWaste from Resource_set AS A, resourceUsed AS B where A.pid = B.pid";

    private static final String question_7 = "Usage of classes vs researches. Is there a usage pattern difference between classes and researches ? (see diff among cores, gpu, ram)";
    private static final String sql_7 = "select ptype, sum(coresUsed) as coresUsed, sum(gpuUsed) as gpuUsed, sum(ramUsed) as ramUsed from resourceUsed group by ptype";

    private static final String question_8 = "The high and low utilization resources in terms of number of users";
    private static final String sql_8 = "Select resourceUsed.pid, pname, numOfUsers, cast(coresUsed as float)/numOfUsers as avg_cores, cast(gpuUsed as float)/numOfUsers as avg_gpu, cast(ramUsed as float)/numOfUsers as avg_ram from userInProj, resourceUsed where userInProj.pid = resourceUsed.pid";

    private static final String question_9 = "Computing time used for each project";
    private static final String sql_9 = "select * from timeUsed";

    private static final String question_10 = "The high and low utilization time in terms of number of users";
    private static final String sql_10 = "select timeUsed.pid, numOfUsers, tot_time, cast(tot_time as float)/numOfUsers as avg_time_used from timeUsed, userInProj where timeUsed.pid = userInProj.pid order by avg_time_used";

    private static final String question_11 = "Usage by category of users";
    private static final String sql_11 = "select usertype, sum(end_time - start_time) as tot_time, sum(cores) as cores_Sum, sum(gpu) as gpu_sum, sum(ram) as ram_sum from validJobInfo, users where validJobInfo.submit_by = users.userid group by usertype";

    private static Map<Integer, String> getQueryMap() {
        Map<Integer, String> queryMap = new HashMap<>();
        queryMap.put(1, sql_1);
        queryMap.put(2, sql_2);
        queryMap.put(3, sql_3);
        queryMap.put(4, sql_4);
        queryMap.put(5, sql_5);
        queryMap.put(6, sql_6);
        queryMap.put(7, sql_7);
        queryMap.put(8, sql_8);
        queryMap.put(9, sql_9);
        queryMap.put(10, sql_10);
        queryMap.put(11, sql_11);
        return queryMap;
    }

    private static Map<Integer, String> getQuestionMap() {
        Map<Integer, String> questionMap = new HashMap<>();
        questionMap.put(1, question_1);
        questionMap.put(2, question_2);
        questionMap.put(3, question_3);
        questionMap.put(4, question_4);
        questionMap.put(5, question_5);
        questionMap.put(6, question_6);
        questionMap.put(7, question_7);
        questionMap.put(8, question_8);
        questionMap.put(9, question_9);
        questionMap.put(10, question_10);
        questionMap.put(11, question_11);
        return questionMap;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, ArgumentParserException {
        Map<Integer, String> queryMap = getQueryMap();
        Map<Integer, String> questionMap = getQuestionMap();

        ArgumentParser parser = ArgumentParsers.newFor("project280").build()
                .description("Process some Database Queries.");

        parser.addArgument("indexList")
                .metavar("index:")
                .type(Integer.class)
                .nargs("*")
                .help("a list of indexes for query statements");

        Namespace res = parser.parseArgs(args);
        List<Integer> list = res.get("indexList");
        Connection conn = connectDB();
        if (list == null) {
            return;
        }
        for(Integer index : list) {
            System.out.println("---------------------------------------------------------------------------");
            System.out.println("Question " + index + " : " + questionMap.get(index) + "\n");
            executeQuery(conn, queryMap.get(index));

        }
    }

    private static Connection connectDB() throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        String url = "jdbc:postgresql://localhost/postgres?currentSchema=project280";
        Properties props = new Properties();
        props.setProperty("user","postgres");
        props.setProperty("password","postgres");
        return getConnection(url, props);
    }

    private static void executeQuery(Connection conn, String query) throws SQLException {
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(query);

        // print the header of the result table.
        ResultSetMetaData rsmd = rs.getMetaData();
        int numColumns = rsmd.getColumnCount();
        for (int i = 1; i <= numColumns; i++) {
            System.out.print(String.format("%15s", rsmd.getColumnName(i)));
        }
        System.out.println();

        // print the rows in the resulting table.
        while (rs.next()) {
            for (int i = 1; i <= numColumns; i++) {
                System.out.print(String.format("%15s", rs.getString(i)));
            }
            System.out.println();
        }

        System.out.println("\n\n");
        rs.close();
        st.close();
    }
}

