package cn.doitedu.flink.day03;



public class WCBean {
    public String word;
    public Integer count;


    public WCBean(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public WCBean() {
        super();
    }

    @Override
    public String toString() {
        return "WCBean{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
