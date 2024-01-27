package classes;

public class Record implements Comparable<Record>{
    private String hashtag;
    private int frequency;

    public Record(String hashtag, int frequency) {
        this.hashtag = hashtag;
        this.frequency = frequency;
    }

    public String getHashtag() {
        return hashtag;
    }

    public void setHashtag(String hashtag) {
        this.hashtag = hashtag;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    @Override
    public int compareTo(Record o) {
        return Integer.compare(this.getFrequency(), o.getFrequency());
    }

    @Override
    public String toString() {
        return "Record{" +
                "hashtag='" + hashtag + '\'' +
                ", frequency=" + frequency +
                '}';
    }
}
