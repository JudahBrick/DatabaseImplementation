package edu.yu.dbimpl;
import edu.yu.dbimpl.*;

import java.sql.*;
import java.util.*;

public class QueryEngineImpl implements QueryEngine {


    final String DATABASE_URL = "jdbc:derby:books";
    final String USERNAME = "dbimpl_user";
    final String PASSWORD = "dbimpl_password";

    public QueryEngineImpl(){

    }

    private ResultSet getResultSet(String sqlQuery){
        try(Connection con = DriverManager.getConnection(
                DATABASE_URL, USERNAME, PASSWORD);
            PreparedStatement pstmt = con.prepareStatement(sqlQuery);
            ResultSet rs = pstmt.executeQuery()){
            return rs;
        }
         catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

    }

    public Set<AuthorInfo> getAllAuthorInfos() {
        final String GET_ALL_AUTHORS = "SELECT authorID, firstName, lastName FROM authors";
        HashSet<AuthorInfo> rtn = new HashSet<>();

        try (Connection con = DriverManager.getConnection(
                DATABASE_URL, USERNAME, PASSWORD);
             PreparedStatement pstmt = con.prepareStatement(GET_ALL_AUTHORS);
             ResultSet rs = pstmt.executeQuery()) {

            if (rs == null) {
                return null;
            }

            int authorID;
            String firstName;
            String lastName;


            while (rs.next()) {
                authorID = (int) rs.getObject("AUTHORID");
                firstName = (String) rs.getObject("FIRSTNAME");
                lastName = (String) rs.getObject("LASTNAME");
                rtn.add(new AuthorInfoImpl(authorID, firstName, lastName));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rtn;
    }

    //+
    //                " INNER JOIN authorisbn ON " +
    //                "authors.authorid = authorisbn.authorid "
    // ON AUTHORISBN.AUTHORID = author.AUTHORID


//    (SELECT authorID FROM authors "  +
//             +
//            ON AUTHORISBN.AUTHORID = author.AUTHORID
    //TODO this method
    public Author authorByName(String lastName, String firstName) {
        final String GET_AUTHOR_BY_NAME =
                "SELECT T.TITLE, T.EDITIONNUMBER, T.ISBN, T.COPYRIGHT, B.AUTHORID FROM " +
                    "(SELECT ISBN, AUTHORS.AUTHORID FROM AUTHORS INNER JOIN AUTHORISBN A on AUTHORS.AUTHORID = A.AUTHORID " +
                    "WHERE AUTHORS.FIRSTNAME=\'" + firstName + "\' AND AUTHORS.LASTNAME=\'" + lastName + "\') B " +
                "INNER JOIN TITLES T on B.ISBN = T.ISBN";
        Author rtn = null;
        try (Connection con = DriverManager.getConnection(
                DATABASE_URL, USERNAME, PASSWORD);
             PreparedStatement pstmt = con.prepareStatement(GET_AUTHOR_BY_NAME);
             ResultSet rs = pstmt.executeQuery()) {

            if(rs == null){
                return null;
            }

            int authorID = -1;
            int previousAuthorID = -1;
            String title;
            String ISBN;
            String copyright;
            int editionNumber;
            Set<TitleInfo> books = new HashSet<>();

            while(rs.next()){
                authorID = (int)rs.getObject("AUTHORID");
                if(previousAuthorID != -1 && previousAuthorID != authorID){
                    throw new IllegalArgumentException("The author name is not unique in this database");
                }
                title = (String)rs.getObject("TITLE");
                ISBN = (String)rs.getObject("ISBN");
                copyright = (String)rs.getObject("COPYRIGHT");
                editionNumber = (int)rs.getObject("EDITIONNUMBER");
                TitleInfo titleInfo = new TitleInfoImpl(title, ISBN, copyright, editionNumber);
                books.add(titleInfo);
            }

            rtn = new AuthorImpl(authorID, firstName, lastName, books);
        }
        catch (SQLException e) {
                e.printStackTrace();
        }

        return rtn;
    }

//    TitleInfo titleInfo;
//    ArrayList<AuthorInfo> authors
    /*  AuthorInfo
        int authorID;
        String firstName;
        String lastName;
     */
    public Set<Book> booksByTitle(final String title) {
        final String GET_BOOKS_BY_TITLE =
                "SELECT TITLE, ISBN, EDITIONNUMBER, COPYRIGHT " +
                "FROM TITLES " +
                "WHERE TITLE=\'" + title + "\'";

        Set<Book> rtn = new HashSet<>();
        try (Connection con = DriverManager.getConnection(
                DATABASE_URL, USERNAME, PASSWORD);
             PreparedStatement pstmt = con.prepareStatement(GET_BOOKS_BY_TITLE);
             ResultSet rs = pstmt.executeQuery()) {

            Book book = null;
            TitleInfo titleInfo = null;
            String ISBN;
            String copyright;
            int editionNumber;

            while (rs.next()){
                ISBN = (String) rs.getObject("ISBN");
                copyright = (String) rs.getObject("copyright");
                editionNumber = (int) rs.getObject("editionNumber");
                titleInfo = new TitleInfoImpl(title, ISBN, copyright, editionNumber);
                List<AuthorInfo> authorInfoList = new ArrayList<>();




                String GET_AUTHOR_INFO_BY_ISBN =
                        "SELECT A.AUTHORID, A.FIRSTNAME, A.LASTNAME " +
                        "FROM AUTHORISBN INNER JOIN AUTHORS A on AUTHORISBN.AUTHORID = A.AUTHORID " +
                        "WHERE AUTHORISBN.ISBN=\'" + ISBN + "\'";

                try (PreparedStatement pstmt2 = con.prepareStatement(GET_AUTHOR_INFO_BY_ISBN);
                                  ResultSet rs2 = pstmt2.executeQuery()) {
                    AuthorInfo authorInfo = null;
                    int authorID;
                    String firstName;
                    String lastName;

                    while (rs2.next()){
                        authorID = (int)rs2.getObject("AUTHORID");
                        firstName = (String) rs2.getObject("FIRSTNAME");
                        lastName = (String) rs2.getObject("LASTNAME");
                        authorInfo = new AuthorInfoImpl(authorID, firstName, lastName);
                        authorInfoList.add(authorInfo);
                    }
                }
                if(authorInfoList.size() < 1){
                    throw new IllegalArgumentException("There are no authors for this title");
                }
                //TODO create the comparator to sort by firstname then last name
                authorInfoList.sort(new Comparator<AuthorInfo>() {
                    @Override
                    public int compare(AuthorInfo o1, AuthorInfo o2) {
                        int rtn;
                        if(o1 == o2){
                            return 0;
                        }
                        rtn = o1.getLastName().compareToIgnoreCase(o2.getLastName());o1.getFirstName().compareToIgnoreCase(o2.getFirstName());
                        if(rtn == 0){
                            return o1.getFirstName().compareToIgnoreCase(o2.getFirstName());
                        }
                        return rtn;
                    }
                });
                book = new BookImpl(titleInfo, authorInfoList);
                rtn.add(book);

            }
            return rtn;

        } catch (SQLException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("There was an issue with this title");
        }

    }

    //\'" + authorID +"\',

//    final String INSERT_NEW_AUTHOR =
//            "INSERT INTO AUTHORS (AUTHORID, FIRSTNAME, LASTNAME)" +
//                    "VALUES ( \'" + authorID + "\',"  + "\'" + firstName + "\', \'" + lastName +"\')";

    public AuthorInfo createAuthorInfo(int authorID, String firstName, String lastName) {
        final String INSERT_NEW_AUTHOR =
                "INSERT INTO AUTHORS ( FIRSTNAME, LASTNAME)" +
                "VALUES (\'" + firstName + "\', \'" + lastName +"\')";

        AuthorInfo authorInfo = new AuthorInfoImpl(authorID, firstName, lastName);
        try (Connection con = DriverManager.getConnection(
                DATABASE_URL, USERNAME, PASSWORD);
             Statement stmt = con.createStatement()) {

            stmt.executeUpdate(INSERT_NEW_AUTHOR);

        }
        catch (SQLException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("something is wrong with the parameters");
        }
        return authorInfo;
    }

    public TitleInfo createTitleInfo(final String isbn, final String title,
                                     final String copyright, final int editionNumber) {
        final String INSERT_NEW_TITLE =
                "INSERT INTO TITLES (isbn, title, editionnumber, copyright)" +
                "VALUES (\'" + isbn + "\', \'" + title + "\', \'" +
                        copyright + "\', \'" + editionNumber + "\')";

        try (Connection con = DriverManager.getConnection(
                DATABASE_URL, USERNAME, PASSWORD);
             Statement stmt = con.createStatement()) {

            stmt.executeUpdate(INSERT_NEW_TITLE);

        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

        return new TitleInfoImpl(title, isbn, copyright, editionNumber);
    }
}
