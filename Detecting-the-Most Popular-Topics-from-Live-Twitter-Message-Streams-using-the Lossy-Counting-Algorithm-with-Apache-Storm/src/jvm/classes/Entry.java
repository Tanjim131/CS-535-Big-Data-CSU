package classes;

public class Entry implements Comparable<Entry> {
    int frequency;
    int delta;

    public Entry(int frequency, int delta) {
        this.frequency = frequency;
        this.delta = delta;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

	public void incrementFrequency(int incrementAmount){
        this.frequency += incrementAmount;
    }

    public int getDelta() {
        return delta;
    }

    public void setDelta(int delta) {
        this.delta = delta;
    }

    @Override
    public int compareTo(Entry o) {
        return Integer.compare(this.getFrequency(), o.getFrequency());
    }
}
