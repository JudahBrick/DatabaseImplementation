import edu.yu.dbimpl.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;


public class authorsAndBooksTesterBae {

	QueryEngineImpl q = new QueryEngineImpl();



	/**
	 * Make sure ya override equals properly homeboy
	 */
	@Test
	public void testAuthorFirst(){
		Author author = q.authorByName("Deitel", "Paul");
		//assert author.equals(new AuthorImpl(1, "Paul", "Deitel"));
	}
	@Test
	public void testAuthorSecond(){
		Author batman = q.authorByName("Batman", "NANANANA");
		assert batman == null;
	}
	@Test
	public void testAuthorThird(){
		try{
			q.authorByName("", "HALLO");
		}
		catch (IllegalArgumentException e){
			//Good job
			return;
		}
		assert false;
	}

	@Test
	public void testAuthorFourth(){
		try{
			q.authorByName("Hallo", "");
		}
		catch (IllegalArgumentException e){
			//Good job
			return;
		}
		assert false;
	}

	@Test
	public void testAuthorFifth(){
		try{
			q.authorByName(null, "Hallo");
		}
		catch (IllegalArgumentException e){
			//Good job
			return;
		}
		assert false;
	}
	@Test
	public void testAuthorSixth(){
		try{
			q.authorByName("Hallo", null);
		}
		catch (IllegalArgumentException e){
			//Good job
			return;
		}
		assert false;
	}

	/**
	 * OK, Now we test more
	 */

	@Test
	public void testBooks1(){
		HashSet<Book> b = (HashSet<Book>) q.booksByTitle("Internet & World Wide Web How to Program");
		for(Book book : b){
			BookImpl otherB = (BookImpl) book;
			assert otherB.getAuthorInfos().size() == 3;
		}
	}

	@Test
	public void testBooks2(){
		HashSet<Book> b = (HashSet<Book>) q.booksByTitle("Java How to Program");
		for(Book book : b){
			BookImpl otherB = (BookImpl) book;
			assert otherB.getAuthorInfos().size() == 2;
		}

	}

	@Test
	public void testBooks3(){
		HashSet<Book> b = (HashSet<Book>) q.booksByTitle("Java How to Program, Late Objects Version");
		for(Book book : b){
			BookImpl otherB = (BookImpl) book;
			assert otherB.getAuthorInfos().size() == 2;
		}

	}

	@Test
	public void testBooks4(){
		HashSet<Book> b = (HashSet<Book>) q.booksByTitle("C How to Program");
		for(Book book : b){
			BookImpl otherB = (BookImpl) book;
			assert otherB.getAuthorInfos().size() == 2;
		}

	}

	@Test
	public void testBooks5(){
		HashSet<Book> b = (HashSet<Book>) q.booksByTitle("Simply Visual Basic 2010");
		for(Book book : b){
			BookImpl otherB = (BookImpl) book;
			assert otherB.getAuthorInfos().size() == 3;
		}

	}

	@Test
	public void testBooks6(){
		HashSet<Book> b = (HashSet<Book>) q.booksByTitle("Visual Basic 2012 How to Program");
		for(Book book : b){
			BookImpl otherB = (BookImpl) book;
			assert otherB.getAuthorInfos().size() == 3;
		}

	}

	@Test
	public void testBooks7(){
		HashSet<Book> b = (HashSet<Book>) q.booksByTitle("iPhone for Programmers: An App-Driven Approach");
		for(Book book : b){
			BookImpl otherB = (BookImpl) book;
			assert otherB.getAuthorInfos().size() == 5;
		}

	}

	@Test
	public void testBooks8(){
		HashSet<Book> b = (HashSet<Book>) q.booksByTitle("Potatos");
		assert b.isEmpty();

	}

	@Test
	public void getAllAuthorInfoTestThingyMajigy(){
		Set<AuthorInfo> allAuthorInfos = q.getAllAuthorInfos();
		assert allAuthorInfos.size() == 5;
	}


















































	/**
	 * Easter Egg for the passing BOIIIIIZZZZZZZZZ
	 */
	@After
	public void printGeshmack() {
		System.out.println();
		System.out.println("######   ########  ######  ##     ## ##     ##    ###     ######  ##    ## \n" +
				"##    ##  ##       ##    ## ##     ## ###   ###   ## ##   ##    ## ##   ##  \n" +
				"##        ##       ##       ##     ## #### ####  ##   ##  ##       ##  ##   \n" +
				"##   #### ######    ######  ######### ## ### ## ##     ## ##       #####    \n" +
				"##    ##  ##             ## ##     ## ##     ## ######### ##       ##  ##   \n" +
				"##    ##  ##       ##    ## ##     ## ##     ## ##     ## ##    ## ##   ##  \n" +
				" ######   ########  ######  ##     ## ##     ## ##     ##  ######  ##    ## ");
		System.out.println();

	}

}
