package techflix;

import techflix.business.Movie;
import techflix.business.MovieRating;
import techflix.business.ReturnValue;
import techflix.business.Viewer;
import techflix.data.DBConnector;
import techflix.data.PostgresSQLErrorCodes;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class Solution {

    public static void createTables(){
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        PreparedStatement statement3 = null;
        PreparedStatement statement4 = null;
        try {
            statement1 = conn.prepareStatement("CREATE TABLE Viewer" +
                    "(\n" +
                    "id INTEGER UNIQUE,\n" +
                    "Name TEXT NOT NULL,\n" +
                    "PRIMARY KEY (id),\n" +
                    "CHECK (id > 0)\n" +
                    ")");
            statement1.execute();
        } catch (SQLException e) {
//            //e.printStackTrace();
        }
        try{
            statement2 = conn.prepareStatement("CREATE TABLE Movie"+
                    "(\n"+
                    "id INTEGER UNIQUE,\n" +
                    "Name TEXT NOT NULL,\n"+
                    "Description TEXT NOT NULL,\n"+
                    "PRIMARY KEY (id),\n"+
                    "CHECK (id > 0)\n"+
                    ")");
            statement2.execute();
        } catch (SQLException e) {
//            //e.printStackTrace();
        }
        try{
            statement4 = conn.prepareStatement("CREATE TYPE RATE_T AS ENUM ('LIKE', 'DISLIKE', 'UNRATED')");
            statement4.execute();
        }catch (SQLException e){

        }
        try{
            statement3 = conn.prepareStatement(/*"CREATE TYPE RATE_T AS ENUM ('LIKE', 'DISLIKE', 'UNRATED');\n " +*/
                    "CREATE TABLE View_t" +
                    "(\n"+
                    "viewer_id INTEGER, \n" +
                    "movie_id INTEGER, \n" +
                    "rating RATE_T , \n" +
                    "FOREIGN KEY (viewer_id) REFERENCES Viewer(id) ON DELETE CASCADE , \n" +
                    "FOREIGN KEY (movie_id) REFERENCES Movie(id) ON DELETE CASCADE, \n" +
                            "PRIMARY KEY (viewer_id, movie_id)\n" +
                    ")");
            statement3.execute();
        } catch (SQLException e) {
//            //e.printStackTrace();
        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                statement2.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                statement3.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                statement4.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }

    }

    public static void clearTables()
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        PreparedStatement statement3 = null;
        try {
            statement1 = conn.prepareStatement("DELETE FROM Viewer");
            statement1.executeUpdate();

            statement2 = conn.prepareStatement("DELETE FROM Movie");
            statement2.executeUpdate();

            statement3 = conn.prepareStatement("DELETE FROM View");
            statement3.executeUpdate();

        } catch (SQLException e) {
//            //e.printStackTrace();
        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                statement2.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                statement3.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
    }

    public static void dropTables()
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        PreparedStatement statement3 = null;
        PreparedStatement statement4 = null;
        try {
            statement1 = conn.prepareStatement("DROP TABLE IF EXISTS Viewer;");
            statement1.execute();
        } catch(SQLException e) {

        }
        try {
            statement2 = conn.prepareStatement("Drop TABLE IF EXISTS Movie;");
            statement2.execute();
        } catch (SQLException e) {

        }
        try {
            statement3 = conn.prepareStatement("Drop TABLE IF EXISTS View_t;");
            statement3.execute();
        } catch (SQLException e) {
            //e.printStackTrace();
        }
        try {
            statement4 = conn.prepareStatement("Drop TYPE rate_t;");
            statement4.execute();
        } catch (SQLException e){

        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                statement2.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                statement3.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
    }


    public static ReturnValue createViewer(Viewer viewer)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        try {
            statement1 = conn.prepareStatement("INSERT INTO Viewer"+
                    " VALUES (?,?)");
            statement1.setInt(1,viewer.getId());
            statement1.setString(2,viewer.getName());

            statement1.execute();
        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()){
                return ReturnValue.ALREADY_EXISTS;
            } else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue() ||
                    Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()){
                return ReturnValue.BAD_PARAMS;
            } else {
                return ReturnValue.ERROR;
            }
        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return ReturnValue.OK;
    }

    public static ReturnValue deleteViewer(Viewer viewer)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        try {
            statement1 = conn.prepareStatement("DELETE FROM Viewer" +
                    " WHERE id = ?");
            statement1.setInt(1,viewer.getId());
            int affectedRows =  statement1.executeUpdate();
            if (affectedRows == 0){
                return ReturnValue.NOT_EXISTS;
            }
        } catch (SQLException e) {
            return ReturnValue.ERROR;
        }

        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return ReturnValue.OK;
    }

    public static ReturnValue updateViewer(Viewer viewer)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        try {
            statement1 = conn.prepareStatement("UPDATE Viewer" +
                    " SET Name = ?" +
                    " WHERE ID = ?");
            statement1.setInt(2,viewer.getId());
            statement1.setString(1,viewer.getName());

            int affectedRows =  statement1.executeUpdate();
            if (affectedRows == 0){
                return ReturnValue.NOT_EXISTS;
            }
        } catch (SQLException e) {
            int err = e.getErrorCode();
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue() ||
            Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()){
                return ReturnValue.BAD_PARAMS;
            } else {
                return ReturnValue.ERROR;
            }
        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return ReturnValue.OK;
    }

    public static Viewer getViewer(Integer viewerId)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;

        Viewer viewer = new Viewer();
        viewer.setId(viewerId);
        try {
            statement1 = conn.prepareStatement("SELECT * FROM Viewer " +
                        "WHERE ID = ?");
            statement1.setInt(1,viewerId);
            ResultSet result = statement1.executeQuery();
            if (result.next()) {
                viewer.setName(result.getString(2));
                return viewer;
            } else {
                return Viewer.badViewer();
            }


        } catch (SQLException e) {
            return Viewer.badViewer();
        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
    }


    public static ReturnValue createMovie(Movie movie)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        try {

            statement1 = conn.prepareStatement("INSERT INTO Movie"+
                    " VALUES (?,?,?)");
            statement1.setInt(1,movie.getId());
            statement1.setString(2,movie.getName());
            statement1.setString(3,movie.getDescription());

            statement1.execute();
        } catch (SQLException e) {
            int err = e.getErrorCode();
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()){
                return ReturnValue.ALREADY_EXISTS;
            } else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue() ||
                    Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()){
                return ReturnValue.BAD_PARAMS;
            } else {
                return ReturnValue.ERROR;
            }
        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return ReturnValue.OK;
    }

    public static ReturnValue deleteMovie(Movie movie)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        try {
            statement1 = conn.prepareStatement("DELETE FROM Movie" +
                    " WHERE id = ?");
            statement1.setInt(1,movie.getId());

            int affectedRows =  statement1.executeUpdate();
            if (affectedRows == 0){
                return ReturnValue.NOT_EXISTS;
            }
        } catch (SQLException e) {
            return ReturnValue.ERROR;
        }

        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return ReturnValue.OK;
    }

    public static ReturnValue updateMovie(Movie movie)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        try {
            statement1 = conn.prepareStatement("UPDATE Movie" +
                    " SET Description = ? " +
                    /*"AND SET Description = ?" +*/
                    " WHERE ID = ?");
            statement1.setInt(2,movie.getId());
//            statement1.setString(1,movie.getName());
            statement1.setString(1,movie.getDescription());

            int affectedRows =  statement1.executeUpdate();
            if (affectedRows == 0){
                return ReturnValue.NOT_EXISTS;
            }
        } catch (SQLException e) {
            int err = e.getErrorCode();
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue() ||
                    Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()){
                return ReturnValue.BAD_PARAMS;
            } else {
                return ReturnValue.ERROR;
            }
        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return ReturnValue.OK;
    }

    public static Movie getMovie(Integer movieId)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;

        Movie movie = new Movie();
        movie.setId(movieId);
        try {
            statement1 = conn.prepareStatement("SELECT * FROM Movie" +
                    " WHERE ID = ?");
            statement1.setInt(1,movieId);
            ResultSet result = statement1.executeQuery();

            if (result.next()) {
                movie.setName(result.getString(2));
                movie.setDescription(result.getString(3));
                return movie;
            } else {
                return Movie.badMovie();
            }

        } catch (SQLException e) {
            return Movie.badMovie();
        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
    }



    public static ReturnValue addView(Integer viewerId, Integer movieId)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;

        try {
            statement1 = conn.prepareStatement("INSERT INTO View_t"+
                    " VALUES (?,?,'UNRATED')");
            statement1.setInt(1,viewerId);
            statement1.setInt(2,movieId);

            statement1.execute();
        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {
                return ReturnValue.NOT_EXISTS;
            } else if(Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;
            } else {
                return ReturnValue.ERROR;
            }
        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return ReturnValue.OK;
    }

    public static ReturnValue removeView(Integer viewerId, Integer movieId)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        try {
            statement1 = conn.prepareStatement("DELETE FROM View_t" +
                    " WHERE viewer_id = ? AND movie_id = ?");
            statement1.setInt(1,viewerId);
            statement1.setInt(2,movieId);
            int affectedRows =  statement1.executeUpdate();
            if (affectedRows == 0){
                return ReturnValue.NOT_EXISTS;
            }
        } catch (SQLException e) {
            return ReturnValue.ERROR;
        }

        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return ReturnValue.OK;
    }

    public static Integer getMovieViewCount(Integer movieId)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        int counterRes = 0;
        try {
            statement1 = conn.prepareStatement("SELECT COUNT(movie_id) FROM View_t" +
                    " WHERE movie_id = ?");
            statement1.setInt(1, movieId);
            ResultSet result = statement1.executeQuery();

            if (result.next()) {
                counterRes = result.getInt(1);
//                return counterRes;
            } else {
                assert (1==2);
            }

//            counterRes = result.getInt(1);
        } catch (SQLException e) {
            return 0;
        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return counterRes;
    }


    public static ReturnValue addMovieRating(Integer viewerId, Integer movieId, MovieRating rating)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        try {
            statement1 = conn.prepareStatement("UPDATE View_t " +
                    "SET rating = '" + rating.toString() + "' " +
                    "WHERE viewer_id = ? AND movie_id = ?");
//            statement1.setString(1,rating.toString());
            statement1.setInt(1,viewerId);
            statement1.setInt(2,movieId);

            int affectedRows =  statement1.executeUpdate();
            if (affectedRows == 0){
                return ReturnValue.NOT_EXISTS;
            }
        } catch (SQLException e) {
            return ReturnValue.ERROR;
        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return ReturnValue.OK;
    }

    public static ReturnValue removeMovieRating(Integer viewerId, Integer movieId)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;

        try {
            statement2 = conn.prepareStatement("SELECT * FROM view_t WHERE viewer_id = ? AND movie_id = ? AND rating = 'UNRATED'");
            statement2.setInt(1, viewerId);
            statement2.setInt(2, movieId);
            ResultSet result = statement2.executeQuery();

            if (result.next()) {
//                int i = result.getInt(1);
//                if (i == 1) {
                return ReturnValue.NOT_EXISTS;
//                }
            }
        } catch (SQLException e){

        }

        try {
            statement1 = conn.prepareStatement("UPDATE View_t" +
                    " SET rating = 'UNRATED'" +
                    " WHERE viewer_id = ? AND movie_id = ?");
//            statement1.setString(1,"NULL");
            statement1.setInt(1,viewerId);
            statement1.setInt(2,movieId);

            int affectedRows =  statement1.executeUpdate();
            if (affectedRows == 0){
                return ReturnValue.NOT_EXISTS;
            }
        } catch (SQLException e) {
            return ReturnValue.ERROR;
        }
        finally {
            if (statement1 != null) {
                try {
                    statement1.close();
                } catch (SQLException e) {
                    //e.printStackTrace();
                }
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return ReturnValue.OK;
    }

    public static int getMovieLikesCount(int movieId)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        int counterRes = 0;
        try {
            statement1 = conn.prepareStatement("SELECT COUNT(movie_id) FROM View_t" +
                    " WHERE movie_id = ? AND rating = 'LIKE'");
            statement1.setInt(1, movieId);
            ResultSet result = statement1.executeQuery();

            if (result.next()) {
                counterRes = result.getInt(1);
//                return counterRes;
            } else {
                assert (1==2);
            }

//            counterRes = result.getInt(1);

        } catch (SQLException e) {
            return 0;
        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return counterRes;
    }

    public static int getMovieDislikesCount(int movieId)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        int counterRes = 0;
        try {
            statement1 = conn.prepareStatement("SELECT COUNT(movie_id) FROM View_t" +
                    " WHERE movie_id = ? AND rating = 'DISLIKE'");
            statement1.setInt(1, movieId);
            ResultSet result = statement1.executeQuery();

            if (result.next()) {
                counterRes = result.getInt(1);
//                return counterRes;
            } else {
                assert (1==2);
            }

//            counterRes = result.getInt(1);

        } catch (SQLException e) {
            return 0;
        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return counterRes;
    }


    public static ArrayList<Integer> getSimilarViewers(Integer viewerId)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        ArrayList<Integer> returnList = new ArrayList();
        try {
            statement1 = conn.prepareStatement("SELECT tbl.viewer_id " +
                                                    "FROM (SELECT viewer_id ,COUNT(movie_id) AS cnt_movies " +
                                                          "FROM View_t " +
                                                          "WHERE viewer_id != ? AND movie_id IN (SELECT movie_id " +
                                                                                                "FROM View_t " +
                                                                                                "WHERE viewer_id = ?) " +
                                                          "GROUP BY viewer_id) AS tbl " +
                                                    "WHERE tbl.cnt_movies/(0.75) >= (SELECT COUNT(movie_id) " +
                                                                                    "FROM View_t " +
                                                                                    "WHERE viewer_id = ?) " +
                                                            "AND tbl.viewer_id != ? AND tbl.cnt_movies != 0 " +
                                                    "ORDER BY tbl.viewer_id ASC");
            statement1.setInt(1, viewerId);
            statement1.setInt(2, viewerId);
            statement1.setInt(3, viewerId);
            statement1.setInt(4, viewerId);
            ResultSet result = statement1.executeQuery();
            while (result.next()){
                returnList.add(result.getInt(1));
            }

//            returnList = (ArrayList<Integer>) result.getArray(1);

        } catch (SQLException e) {
            return returnList;
        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return returnList;
    }

    public static ArrayList<Integer> mostInfluencingViewers()
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        ArrayList<Integer> returnList = new ArrayList();
        try {
            statement1 = conn.prepareStatement("SELECT tbl3.viewer_id " +
                    "FROM ((SELECT viewer_id, COUNT(movie_id) AS cnt_views " +
                            "FROM View_t " +
                            "GROUP BY viewer_id) AS tbl1 " +
                        "LEFT OUTER JOIN (SELECT viewer_id AS viewer_id2, COUNT (movie_id) AS cnt_ratings " +
                            "FROM View_t " +
                            "WHERE rating = 'LIKE' OR rating = 'DISLIKE' " +
                            "GROUP BY viewer_id) AS tbl2 " +
                        "ON (tbl1.viewer_id = tbl2.viewer_id2)) AS tbl3 " +
                    "ORDER BY tbl3.cnt_views DESC, tbl3.cnt_ratings DESC, tbl3.viewer_id ASC " +
                    "LIMIT 10");
            ResultSet result = statement1.executeQuery();
            while (result.next()){
                returnList.add(result.getInt(1));
            }
        } catch (SQLException e) {
            return returnList;
        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return returnList;
    }

    public static ArrayList<Integer> getMoviesRecommendations(Integer viewerId)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        ArrayList<Integer> returnList = new ArrayList();
        try {
            statement1 = conn.prepareStatement("select final_tbl.movie_id " +
                    "from ((SELECT  movie_id,COUNT(viewer_id) AS cnt_likes " +
                    "FROM (SELECT tbl.viewer_id AS viewer_id " +
                    "FROM (SELECT viewer_id ,COUNT(movie_id) AS cnt_movies " +
                    "FROM View_t " +
                    "WHERE viewer_id != ? AND movie_id IN (SELECT movie_id " +
                    "FROM View_t " +
                    "WHERE viewer_id = ?) " +
                    "GROUP BY viewer_id) AS tbl " +
                    "WHERE (tbl.cnt_movies/(0.75) >= (SELECT COUNT(movie_id) " +
                    "FROM view_t " +
                    "WHERE viewer_id = ?)) " +
                    "AND (tbl.viewer_id != ? AND tbl.cnt_movies != 0) " +
                    "ORDER BY tbl.viewer_id ASC) AS tbl_similar_viewers, " +
                    "(SELECT DISTINCT id as movie_id FROM movie WHERE movie.id NOT IN (SELECT movie_id FROM view_t WHERE viewer_id = ?)) AS tbl_movies_not_seen " +
                    "WHERE tbl_movies_not_seen.movie_id IN (SELECT movie_id FROM view_t WHERE rating = 'LIKE' AND tbl_similar_viewers.viewer_id = viewer_id) " +
                    "GROUP BY movie_id) " +
                    "UNION " +
                    "(select Distinct movie_id, 0 as cnt_rating " +
                    "from view_t " +
                    "where rating not in ('LIKE') " +
                    "AND movie_id IN (SELECT DISTINCT id as movie_id FROM movie WHERE movie.id NOT IN (SELECT movie_id FROM view_t WHERE viewer_id = ?) " +
                            "AND viewer_id IN (SELECT tbl.viewer_id AS viewer_id " +
                                    "FROM (SELECT viewer_id ,COUNT(movie_id) AS cnt_movies " +
                                            "FROM View_t " +
                                            "WHERE viewer_id != ? AND movie_id IN (SELECT movie_id " +
                                                    "FROM View_t " +
                                                    "WHERE viewer_id = ?) " +
                                            "GROUP BY viewer_id) AS tbl " +
                                    "WHERE (tbl.cnt_movies/(0.75) >= (SELECT COUNT(movie_id) " +
                                            "FROM view_t " +
                                            "WHERE viewer_id = ?)) " +
                            "AND (tbl.viewer_id != ? AND tbl.cnt_movies != 0) " +
                            "ORDER BY tbl.viewer_id ASC)) " +
            "group by movie_id " +
            "Order by movie_id)) as final_tbl " +
            "ORDER BY final_tbl.cnt_likes DESC, final_tbl.movie_id ASC " +
            "limit 10");
            statement1.setInt(1, viewerId);
            statement1.setInt(2, viewerId);
            statement1.setInt(3, viewerId);
            statement1.setInt(4, viewerId);
            statement1.setInt(5, viewerId);
            statement1.setInt(6, viewerId);
            statement1.setInt(7, viewerId);
            statement1.setInt(8, viewerId);
            statement1.setInt(9, viewerId);
            statement1.setInt(10, viewerId);
            ResultSet result = statement1.executeQuery();
            while (result.next()){
                returnList.add(result.getInt(1));
            }

        } catch (SQLException e) {
            return returnList;
        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return returnList;
    }

    public static ArrayList<Integer> getConditionalRecommendations(Integer viewerId, int movieId)
    {
        Connection conn = DBConnector.getConnection();
        PreparedStatement statement1 = null;
        ArrayList<Integer> returnList = new ArrayList();
        try {
            statement1 = conn.prepareStatement("select final_tbl.movie_id " +
                    "from ((SELECT  movie_id,COUNT(viewer_id) AS cnt_likes " +
                            "FROM (SELECT tbl.viewer_id " +
                                    "FROM (SELECT viewer_id ,COUNT(movie_id) AS cnt_movies " +
                                            "FROM View_t " +
                                            "WHERE viewer_id != ? AND movie_id IN (SELECT movie_id " +
                                                    "FROM View_t " +
                                                    "WHERE viewer_id = ?) " +
                                            "GROUP BY viewer_id) AS tbl " +
                                    "WHERE tbl.cnt_movies/(0.75) >= (SELECT COUNT(movie_id) " +
                                    "FROM view_t " +
                                    "WHERE viewer_id = ?) " +
                            "AND tbl.viewer_id != ? AND tbl.cnt_movies != 0 " +
                            "AND tbl.viewer_id IN (SELECT viewer_id from view_t where movie_id = ? AND rating != 'UNRATED' AND rating IN (SELECT rating from view_t where viewer_id = ? AND movie_id = ?)) " +
                            "ORDER BY tbl.viewer_id ASC) AS tbl_similar_viewers, " +
                    "(SELECT DISTINCT id as movie_id FROM movie WHERE movie.id NOT IN (SELECT movie_id FROM view_t WHERE viewer_id = ?)) AS tbl_movies_not_seen " +
            "WHERE tbl_movies_not_seen.movie_id IN (SELECT movie_id FROM view_t WHERE rating = 'LIKE' AND tbl_similar_viewers.viewer_id = viewer_id) " +
            "GROUP BY movie_id) " +
            "UNION " +
                    "(select Distinct movie_id, 0 as cnt_rating " +
                            "from view_t " +
                            "where rating not in ('LIKE') " +
                            "AND movie_id IN (SELECT DISTINCT id as movie_id FROM movie WHERE movie.id NOT IN (SELECT movie_id FROM view_t WHERE viewer_id = ?) " +
                            "AND viewer_id IN (SELECT tbl.viewer_id AS viewer_id " +
                                    "FROM (SELECT viewer_id ,COUNT(movie_id) AS cnt_movies " +
                                            "FROM View_t " +
                                            "WHERE viewer_id != ? AND movie_id IN (SELECT movie_id " +
                                                    "FROM View_t " +
                                                    "WHERE viewer_id = ?) " +
                                            "GROUP BY viewer_id) AS tbl " +
                                    "WHERE (tbl.cnt_movies/(0.75) >= (SELECT COUNT(movie_id) " +
                                            "FROM view_t " +
                                            "WHERE viewer_id = ?)) " +
                            "AND (tbl.viewer_id != ? AND tbl.cnt_movies != 0) " +
                            "AND tbl.viewer_id IN (SELECT viewer_id from view_t where movie_id = ? AND rating != 'UNRATED' AND rating IN (SELECT rating from view_t where viewer_id = ? AND movie_id = ?)) " +
                            "ORDER BY tbl.viewer_id ASC)) " +
            "group by movie_id " +
            "Order by movie_id)) as final_tbl " +
            "ORDER BY final_tbl.cnt_likes DESC, final_tbl.movie_id ASC " +
            "limit 10");
            statement1.setInt(1, viewerId);
            statement1.setInt(2, viewerId);
            statement1.setInt(3, viewerId);
            statement1.setInt(4, viewerId);
            statement1.setInt(5, movieId);

            statement1.setInt(6, viewerId);
            statement1.setInt(7, movieId);
            statement1.setInt(8, viewerId);
            statement1.setInt(9, viewerId);
            statement1.setInt(10, viewerId);
            statement1.setInt(11, viewerId);
            statement1.setInt(12, viewerId);
            statement1.setInt(13, viewerId);
            statement1.setInt(14, movieId);
            statement1.setInt(15, viewerId);
            statement1.setInt(16, movieId);
            ResultSet result = statement1.executeQuery();
            while (result.next()){
                returnList.add(result.getInt(1));
            }

        } catch (SQLException e) {
            return returnList;
        }
        finally {
            try {
                statement1.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return returnList;
    }

}


