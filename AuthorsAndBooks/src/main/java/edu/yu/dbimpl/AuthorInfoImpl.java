package edu.yu.dbimpl;


import java.util.Objects;

public class AuthorInfoImpl implements AuthorInfo {

    private int authorID;
    private String firstName;
    private String lastName;


    public AuthorInfoImpl(int authorID, String firstName,  String lastName){
        this.authorID = authorID;
        this.firstName = firstName;
        this.lastName = lastName;
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
        if (!(o instanceof AuthorInfoImpl)) return false;
        AuthorInfoImpl that = (AuthorInfoImpl) o;
        return authorID == that.authorID &&
                firstName.equals(that.firstName) &&
                lastName.equals(that.lastName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(authorID, firstName, lastName);
    }

    @Override
    public String toString() {
        return "AuthorInfoImpl{" +
                "authorID=" + authorID +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                '}';
    }
}
