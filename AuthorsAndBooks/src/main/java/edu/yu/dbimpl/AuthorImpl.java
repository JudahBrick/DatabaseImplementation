package edu.yu.dbimpl;


import java.util.Objects;
import java.util.Set;

public class AuthorImpl implements Author{

    private int authorID;
    private String firstName;
    private String lastName;
    private Set<TitleInfo> books;

    public AuthorImpl(int authorID, String firstName,  String lastName, Set<TitleInfo> books){
        this.authorID = authorID;
        this.firstName = firstName;
        this.lastName = lastName;
        this.books = books;
    }


    public Set<TitleInfo> getTitleInfos() {
        return books;
    }

    public int getAuthorID() {
        return authorID;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AuthorImpl)) return false;
        AuthorImpl author = (AuthorImpl) o;
        return authorID == author.authorID &&
                firstName.equals(author.firstName) &&
                lastName.equals(author.lastName) &&
                books.equals(author.books);
    }

    @Override
    public String toString() {
        return "AuthorImpl{" +
                "authorID=" + authorID +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", books=" + books +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(authorID, firstName, lastName, books);
    }
}
