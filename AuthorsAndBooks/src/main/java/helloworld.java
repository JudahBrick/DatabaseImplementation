import edu.yu.dbimpl.*;

import java.util.*;

public class helloworld {
    public static void main(String[] args) {
    	CreateCoffeeDB cc = new CreateCoffeeDB();
        System.out.println("GET ALL AUTHORS");
        QueryEngineImpl QE = new QueryEngineImpl();
        printAllAuthors(QE);
        System.out.println();

        System.out.println("TEST AUTHORS BY NAME");
        testGetAuthorByName(QE, "Paul", "Deitel");
        testGetAuthorByName(QE, "Harvey", "Deitel");
        testGetAuthorByName(QE, "Abbey", "Deitel");
        testGetAuthorByName(QE, "Eric", "Kern");

        System.out.println();
        System.out.println();

        //testAddNewAuthor(QE, 6, "Judah", "Brick");
        //i

        System.out.println("TEST GET BOOKS BY TITLE");
        testGetBookByTitle(QE ,"iPhone for Programmers: An App-Driven Approach");
        System.out.println();


    }

    public static void testGetAuthorByName(QueryEngine QE, String firstName, String lastName){
        System.out.println();
        Author author = QE.authorByName(lastName, firstName);
        System.out.println("Author:  " + author.getFirstName() + " " + author.getLastName()
                + " ID: " + author.getAuthorID());
        System.out.println("BOOKS:");
        Set<TitleInfo> titleInfoSet = author.getTitleInfos();
        Iterator it = titleInfoSet.iterator();
        while(it.hasNext()){
            System.out.println(it.next());
        }
        System.out.println();
    }

    public static void testGetBookByTitle(QueryEngine QE, String title){
        System.out.println();
        Set<Book> books = QE.booksByTitle(title);
        Iterator it = books.iterator();
        while (it.hasNext()){
            Book current = (Book)it.next();
            System.out.println("Title: " + current.getTitleInfo().getTitle() + "  Copyright: " + current.getTitleInfo().getCopyright() +
                    " ISBN: " + current.getTitleInfo().getISBN() + " Edition: " + current.getTitleInfo().getEditionNumber());
            List<AuthorInfo> authorInfoList = current.getAuthorInfos();
            for(AuthorInfo author: authorInfoList){
                System.out.println(author);
            }
        }
    }

    public static void testAddNewAuthor(QueryEngine QE, int authorID, String firstName, String lastName){
        System.out.println("Add new author: " +
                "Name: " + firstName + " " + lastName + " ID: " + authorID);
        QE.createAuthorInfo(authorID, firstName,lastName);
        printAllAuthors(QE);
        System.out.println();
    }

    public static void printAllAuthors(QueryEngine QE){
        Set<AuthorInfo> authorsInfo =  QE.getAllAuthorInfos();
        Iterator it = authorsInfo.iterator();
        while(it.hasNext()){
            System.out.println(it.next());
        }
        System.out.println();
    }
}
