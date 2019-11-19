package edu.yu.dbimpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class BookImpl implements Book {

    private TitleInfo titleInfo;
    private List<AuthorInfo> authors;

    public BookImpl(TitleInfo titleInfo, List<AuthorInfo> authors){
        this.titleInfo = titleInfo;
        this.authors = authors;
    }

    public TitleInfo getTitleInfo() {
        return titleInfo;
    }

    public List<AuthorInfo> getAuthorInfos() {
        return authors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BookImpl)) return false;
        BookImpl book = (BookImpl) o;
        return titleInfo.equals(book.titleInfo) &&
                authors.equals(book.authors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(titleInfo, authors);
    }

    @Override
    public String toString() {
        return "BookImpl{" +
                "titleInfo=" + titleInfo +
                ", authors=" + authors +
                '}';
    }
}
