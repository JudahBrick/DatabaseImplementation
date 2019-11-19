package edu.yu.dbimpl;

import java.util.Objects;

public class TitleInfoImpl implements TitleInfo {

    private String title;
    private String ISBN;
    private String copyright;
    private int editionNumber;

    public TitleInfoImpl(String title, String ISBN, String copyright, int editionNumber){
        this.title = title;
        this.ISBN = ISBN;
        this.copyright = copyright;
        this.editionNumber = editionNumber;
    }

    public String getTitle() {
        return title;
    }

    public String getISBN() {
        return ISBN;
    }

    public int getEditionNumber() {
        return editionNumber;
    }

    public String getCopyright() {
        return copyright;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TitleInfoImpl)) return false;
        TitleInfoImpl titleInfo = (TitleInfoImpl) o;
        return editionNumber == titleInfo.editionNumber &&
                title.equals(titleInfo.title) &&
                ISBN.equals(titleInfo.ISBN) &&
                copyright.equals(titleInfo.copyright);
    }

    @Override
    public int hashCode() {
        return Objects.hash(title, ISBN, copyright, editionNumber);
    }

    @Override
    public String toString() {
        return "TitleInfoImpl{" +
                "title='" + title + '\'' +
                ", ISBN='" + ISBN + '\'' +
                ", copyright='" + copyright + '\'' +
                ", editionNumber=" + editionNumber +
                '}';
    }
}
